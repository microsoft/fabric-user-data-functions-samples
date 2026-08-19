"""
Standalone streaming tests for the `rayfin_kusto_v1` connector function.

These verify the byte-pump contract without the Fabric runtime: a minimal
`fabric.functions` stub is injected so `function_app` imports, and the shared
aiohttp session is replaced with a fake whose response yields several body
chunks. The tests assert that the 200 path:

  * relays MORE THAN ONE chunk for a multi-chunk result (true streaming),
  * never buffers or parses the body (`resp.text()` is never called), and
  * forwards the SDK-supplied clientRequestId as the `x-ms-client-request-id`
    request header.

Run directly (`python3 test_function_app.py`) or under pytest. Only requires
aiohttp to be importable (function_app imports it at module load).
"""

import asyncio
import importlib
import os
import sys
import types
import io
import json
import logging


def _install_fabric_stub():
    """Inject a minimal `fabric.functions` so `function_app` imports."""
    if "fabric.functions" in sys.modules:
        return

    fabric = types.ModuleType("fabric")
    functions = types.ModuleType("fabric.functions")

    class UserDataFunctions:
        def function(self, *_args, **_kwargs):
            return lambda fn: fn

        # The real decorators wire connections/streaming; for a direct unit
        # test they just return the function unchanged so we can call it.
        def generic_connection(self, *_args, **_kwargs):
            return lambda fn: fn

        def streaming_function(self, *_args, **_kwargs):
            return lambda fn: fn

    class StreamResponse:
        def __init__(self, body, media_type=None, status_code=None):
            self.body = body
            self.media_type = media_type
            self.status_code = status_code

    class FabricItem:  # placeholder type hint target
        pass

    functions.UserDataFunctions = UserDataFunctions
    functions.StreamResponse = StreamResponse
    functions.FabricItem = FabricItem
    fabric.functions = functions
    sys.modules["fabric"] = fabric
    sys.modules["fabric.functions"] = functions


def _load_function_app():
    _install_fabric_stub()
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    return importlib.import_module("function_app")


class _FakeContent:
    def __init__(self, chunks):
        self._chunks = chunks

    async def iter_any(self):
        for chunk in self._chunks:
            yield chunk


class _FakeResponse:
    def __init__(self, chunks, status=200):
        self.status = status
        self.headers = {}
        self.content = _FakeContent(chunks)
        self.text_called = False

    async def text(self):
        # The 200 path must never buffer/parse the body.
        self.text_called = True
        return ""

    async def release(self):
        self.released = True


class _FakeSession:
    def __init__(self, response):
        self._response = response
        self.captured_headers = None
        self.captured_url = None

    async def post(self, url, json=None, headers=None):
        self.captured_url = url
        self.captured_headers = headers
        return self._response


class _FakeCredential:
    class _Token:
        token = "fake-kusto-token"

    def get_token(self):
        return self._Token()


class _FakeKustoClient:
    def get_access_token(self):
        return _FakeCredential()


def _payload(client_request_id="KPC.rayfin_kusto_v1;test-id"):
    return {
        "input": {
            "queryServiceUri": "https://cluster.kusto.fabric.microsoft.com",
            "databaseName": "db",
            "query": "T | take 1",
            "clientRequestId": client_request_id,
        }
    }


def _command_payload(client_request_id="KPC.rayfin_kusto_v1;cmd-id"):
    return {
        "operation": "executeCommand",
        "input": {
            "queryServiceUri": "https://cluster.kusto.fabric.microsoft.com",
            "databaseName": "db",
            "command": ".show tables",
            "clientRequestId": client_request_id,
        },
    }


async def _invoke(chunks, payload):
    mod = _load_function_app()
    response = _FakeResponse(chunks)
    session = _FakeSession(response)

    async def _fake_get_session():
        return session

    mod._get_session = _fake_get_session  # type: ignore[attr-defined]

    result = await mod.rayfin_kusto_v1(payload, _FakeKustoClient())

    body = []
    async for chunk in result.body:
        body.append(chunk)
    return mod, result, body, response, session


async def _check_streams_multiple_chunks_without_buffering():
    chunks = [b'{"Tables":[', b'{"TableName":"T","Rows":[[1]]}', b"]}"]
    mod, result, body, response, _session = await _invoke(chunks, _payload())

    # More than one chunk reaches the caller -> genuine streaming.
    assert len(body) == 3, f"expected 3 relayed chunks, got {len(body)}"
    # Bytes are relayed verbatim (no transform, no re-serialize).
    assert b"".join(body) == b"".join(chunks), "relayed bytes must match Kusto's"
    # The 200 path never buffers/parses the body.
    assert response.text_called is False, "200 path must not call resp.text()"
    # It is streamed as JSON, not the Arrow media type.
    assert result.media_type == mod._JSON_MEDIA_TYPE, "media_type must be JSON"


async def _check_forwards_client_request_id_header():
    chunks = [b'{"Tables":[]}']
    _mod, _result, _body, _response, session = await _invoke(
        chunks, _payload("KPC.rayfin_kusto_v1;abc-123")
    )
    assert session.captured_headers is not None
    assert (
        session.captured_headers.get("x-ms-client-request-id")
        == "KPC.rayfin_kusto_v1;abc-123"
    ), "SDK-supplied clientRequestId must be forwarded as the header"
    # And the query endpoint (not mgmt) is used for executeQuery.
    assert session.captured_url.endswith("/v1/rest/query")


async def _check_execute_command_routes_to_mgmt_and_streams():
    # executeCommand must route to /v1/rest/mgmt (not /query) and still stream
    # the v1 {Tables} body as a pure byte pump, exactly like executeQuery.
    chunks = [b'{"Tables":[', b'{"TableName":"T","Rows":[["x"]]}', b"]}"]
    _mod, _result, body, response, session = await _invoke(
        chunks, _command_payload("KPC.rayfin_kusto_v1;cmd-1")
    )
    assert session.captured_url.endswith(
        "/v1/rest/mgmt"
    ), f"executeCommand must route to /v1/rest/mgmt, got {session.captured_url}"
    assert len(body) == 3, "executeCommand 200 path must stream, not buffer"
    assert response.text_called is False, "executeCommand must not call resp.text()"
    assert (
        session.captured_headers.get("x-ms-client-request-id")
        == "KPC.rayfin_kusto_v1;cmd-1"
    ), "clientRequestId is forwarded for executeCommand too"


def test_streams_multiple_chunks_without_buffering():
    asyncio.run(_check_streams_multiple_chunks_without_buffering())


def test_forwards_client_request_id_header():
    asyncio.run(_check_forwards_client_request_id_header())


def test_execute_command_routes_to_mgmt_and_streams():
    asyncio.run(_check_execute_command_routes_to_mgmt_and_streams())


# ---------------------------------------------------------------------------
# rayfin_fabric_aihub_v1 tests. The fabric stub above also exposes a plain
# function() decorator so this dict-returning UDF imports. These fakes carry a
# status, headers, a content-type and a text body, and the session can return a
# SEQUENCE of responses (needed for getInfo's initialize + tools/list).
# ---------------------------------------------------------------------------

_AIHUB_TOKEN = "fake-fabric-token"


class _AihubContent:
    def __init__(self, chunks):
        self._chunks = chunks or []

    async def iter_any(self):
        for chunk in self._chunks:
            yield chunk

    async def iter_chunked(self, _size):
        for chunk in self._chunks:
            yield chunk


class _AihubResponse:
    def __init__(self, status=200, headers=None, body="", content_type=None):
        self.status = status
        self.headers = dict(headers) if headers else {}
        if content_type is not None:
            self.headers["Content-Type"] = content_type
        self._body = body
        self.content = _AihubContent([body.encode("utf-8")])
        self.text_called = False
        self.released = False

    async def text(self):
        self.text_called = True
        return self._body

    def release(self):
        self.released = True


class _AihubSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self._i = 0
        self.posts = []

    async def post(self, url, json=None, headers=None, timeout=None):
        # `timeout` is accepted (and recorded) because the AI Hub calls pass an
        # explicit finite per-request timeout; the fake must tolerate the kwarg.
        self.posts.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        if self._i < len(self._responses):
            resp = self._responses[self._i]
        else:
            resp = self._responses[-1]
        self._i += 1
        return resp


class _AihubCred:
    class _T:
        token = "fake-fabric-token"

    def get_token(self):
        return self._T()


class _AihubFabricClient:
    def get_access_token(self):
        return _AihubCred()


async def _invoke_aihub(payload, responses, fabric_client=None):
    mod = _load_function_app()
    session = _AihubSession(responses)

    async def _fake_get_session():
        return session

    mod._get_session = _fake_get_session  # type: ignore[attr-defined]
    request_ids = iter(["invocation-id", "1", "2", "3", "4"])
    original_uuid4 = mod.uuid.uuid4
    mod.uuid.uuid4 = lambda: next(request_ids)
    try:
        result = await mod.rayfin_fabric_aihub_v1(
            payload, fabric_client or _AihubFabricClient()
        )
    finally:
        mod.uuid.uuid4 = original_uuid4
    return mod, result, session


def _resp(status=200, body="", content_type="application/json", headers=None):
    return _AihubResponse(status=status, body=body, content_type=content_type, headers=headers)


async def _check_get_info_initialize_and_tools_list():
    init_body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {
        "protocolVersion": "2025-06-18",
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "fabricaihub", "version": "9"},
        "instructions": "hello",
        "_meta": {"foo": "bar"},
    }})
    tools_body = json.dumps({"jsonrpc": "2.0", "id": "2", "result": {
        "tools": [{"name": "ask", "description": "d"}],
        "nextCursor": "abc",
    }})
    mod, result, session = await _invoke_aihub(
        {"operation": "getInfo"},
        [_resp(body=init_body), _resp(status=202, body=""), _resp(body=tools_body)],
    )
    # MCP 2025-06-18 lifecycle: initialize -> notifications/initialized -> tools/list.
    assert len(session.posts) == 3, f"getInfo must do 3 posts, got {len(session.posts)}"
    assert session.posts[0]["json"]["method"] == "initialize", "first call must be initialize"
    assert session.posts[1]["json"]["method"] == "notifications/initialized", "second call must be the initialized notification"
    assert "id" not in session.posts[1]["json"], "a JSON-RPC notification must NOT carry an id"
    assert session.posts[2]["json"]["method"] == "tools/list", "third call must be tools/list"
    # Each AI Hub call must carry an explicit finite per-request timeout.
    assert session.posts[0]["timeout"] is mod._AIHUB_REQUEST_TIMEOUT, "AI Hub calls must pass the finite timeout"
    assert result["tools"][0]["name"] == "ask", "tools list must be returned"
    assert result["serverInfo"]["name"] == "fabricaihub"
    assert result["capabilities"] == {"tools": {}}
    assert result["protocolVersion"] == "2025-06-18"
    assert result["instructions"] == "hello", "additive initialize field must survive"
    assert result["_meta"] == {"foo": "bar"}, "_meta must survive"
    assert result["nextCursor"] == "abc", "additive tools/list field must survive"


async def _check_start_task_creates_task():
    task_body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {"task": {
        "taskId": "T-1", "status": "working", "pollInterval": 5, "ttl": 300, "createdAt": "2026-01-01"}}})
    payload = {"operation": "startTask", "input": {
        "toolName": "ask", "arguments": {"q": "secret", "artifactId": "A-9"}, "ttl": 300}}
    mod, result, session = await _invoke_aihub(payload, [_resp(body=task_body)])
    assert result["taskId"] == "T-1"
    assert result["status"] == "working"
    assert result["pollInterval"] == 5
    assert result["ttl"] == 300
    sent = session.posts[0]["json"]
    assert sent["method"] == "tools/call", "startTask must send tools/call"
    assert sent["params"]["name"] == "ask"
    assert sent["params"]["arguments"]["artifactId"] == "A-9", "artifactId must be forwarded inside arguments"
    assert sent["params"]["task"]["ttl"] == 300, "ttl must be forwarded in the task request"


async def _check_start_task_immediate_result_fallback():
    imm_body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {
        "content": [{"type": "text", "text": "answer"}],
        "isError": False,
        "structuredContent": {"score": 1},
        "_meta": {"k": "v"},
        "extraField": "keep",
    }})
    payload = {"operation": "startTask", "input": {"toolName": "ask", "arguments": {}}}
    mod, result, session = await _invoke_aihub(payload, [_resp(body=imm_body)])
    assert result["content"][0]["text"] == "answer", "immediate content must be returned"
    assert result["isError"] is False
    assert result["structuredContent"] == {"score": 1}
    assert result["_meta"] == {"k": "v"}
    assert result["extraField"] == "keep", "unknown additive field must survive"
    assert "task" in session.posts[0]["json"]["params"], "tools/call must be task-augmented"


async def _check_task_ops_route_to_methods():
    async def _one(op, method, body):
        payload = {"operation": op, "input": {"taskId": "T-7"}}
        mod, result, session = await _invoke_aihub(payload, [_resp(body=body)])
        assert session.posts[0]["json"]["method"] == method, f"{op} must call {method}"
        assert session.posts[0]["json"]["params"]["taskId"] == "T-7"
        return result

    gt = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {"taskId": "T-7", "status": "working"}})
    res = await _one("getTask", "tasks/get", gt)
    assert res["status"] == "working"
    grr = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {"content": [{"type": "text", "text": "done"}], "isError": False}})
    res = await _one("getTaskResult", "tasks/result", grr)
    assert res["content"][0]["text"] == "done"
    gc = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {"taskId": "T-7", "status": "cancelled"}})
    res = await _one("cancelTask", "tasks/cancel", gc)
    assert res["status"] == "cancelled"


async def _check_sse_response_parsed():
    sse = (
        ": ping\n"
        "event: message\n"
        "data: {\"jsonrpc\": \"2.0\", \"id\": \"other\", \"result\": {\"status\": \"working\"}}\n"
        "\n"
        "event: ping\n"
        ": keep-alive\n"
        "\n"
        "event: message\n"
        "data: {\"jsonrpc\": \"2.0\", \"id\": \"1\", \"result\":\n"
        "data: {\"taskId\": \"T-9\", \"status\": \"completed\"}}\n"
        "\n"
    )
    payload = {"operation": "getTask", "input": {"taskId": "T-9"}}
    mod, result, session = await _invoke_aihub(payload, [_resp(body=sse, content_type="text/event-stream")])
    assert result["taskId"] == "T-9", "the correlated SSE result must win"
    assert result["status"] == "completed"

async def _check_202_ack_and_json_task():
    # 202 empty body -> ack merged with the KNOWN taskId (never fabricated).
    payload = {"operation": "getTask", "input": {"taskId": "T-3"}}
    mod, result, session = await _invoke_aihub(payload, [_resp(status=202, body="", content_type="text/plain")])
    assert result.get("status") == "accepted", "empty 202 must be a normalized ack"
    assert result.get("taskId") == "T-3", "known taskId must be merged into the ack"

    # 202 plain-text body on startTask -> ack, and NO fabricated taskId.
    payload2 = {"operation": "startTask", "input": {"toolName": "ask", "arguments": {}}}
    mod, result2, session2 = await _invoke_aihub(payload2, [_resp(status=202, body="Accepted", content_type="text/plain")])
    assert result2.get("status") == "accepted"
    assert "taskId" not in result2, "startTask ack must not invent a taskId"

    # 202 WITH a JSON body -> parsed as a real task response.
    task_json = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {"taskId": "T-42", "status": "working"}})
    payload3 = {"operation": "startTask", "input": {"toolName": "ask", "arguments": {}}}
    mod, result3, session3 = await _invoke_aihub(payload3, [_resp(status=202, body=task_json)])
    assert result3["taskId"] == "T-42", "202 JSON body must be parsed as a task response"
    assert result3["status"] == "working"


async def _check_http_error_envelopes():
    cases = [
        (401, "Unauthorized", False, True),
        (403, "Forbidden", False, True),
        (429, "Throttled", True, False),
        (500, "UpstreamError", True, False),
    ]
    for status, code, retryable, user_err in cases:
        headers = {"x-ms-request-id": "req-1", "x-ms-root-activity-id": "root-1"}
        payload = {"operation": "getTask", "input": {"taskId": "T-1"}}
        mod, result, session = await _invoke_aihub(
            payload, [_resp(status=status, body="upstream-detail", content_type="text/plain", headers=headers)])
        assert "error" in result, f"status {status} must return a structured error"
        err = result["error"]
        assert err["code"] == code, f"status {status} code should be {code}, got {err['code']}"
        assert err["retryable"] is retryable, f"status {status} retryable mismatch"
        assert err["userError"] is user_err, f"status {status} userError mismatch"
        assert err["httpStatus"] == status
        assert err["connectorName"] == "fabric-aihub"
        assert err["source"] == "upstream"
        assert err["requestId"] == "req-1", "requestId must be preserved from upstream headers"
        assert err["rootActivityId"] == "root-1", "rootActivityId must be preserved"
        assert err.get("invocationId"), "invocationId must be present"


async def _check_retry_after_surfaced():
    payload = {"operation": "getTask", "input": {"taskId": "T-1"}}
    mod, result, session = await _invoke_aihub(
        payload, [_resp(status=429, body="", content_type="text/plain", headers={"Retry-After": "30"})])
    err = result["error"]
    assert err["retryable"] is True, "429 must be retryable"
    assert err["diagnostics"]["retryAfterSeconds"] == 30, "Retry-After seconds must be surfaced"


async def _check_fixed_target_security():
    bad_keys = ["endpoint", "target", "baseUrl", "workspaceId", "itemId", "url", "mcpUrl", "host", "origin", "ENDPOINT"]
    for bad in bad_keys:
        payload = {"operation": "getInfo", "input": {bad: "https://evil.example/mcp"}}
        mod, result, session = await _invoke_aihub(payload, [_resp(body="{}")])
        assert "error" in result, f"key {bad} must be rejected"
        err = result["error"]
        assert err["code"] == "InvalidTargetOverride", f"key {bad} must be InvalidTargetOverride"
        assert err["userError"] is True
        assert err["retryable"] is False
        assert err["httpStatus"] == 400
        assert err["source"] == "connector"
        assert len(session.posts) == 0, f"key {bad} must never reach the network"

    # The posted URL is ALWAYS the fixed endpoint, and artifactId is NOT a target.
    task_body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {"task": {"taskId": "T-1", "status": "working"}}})
    payload = {"operation": "startTask", "input": {"toolName": "ask", "arguments": {"artifactId": "A-1"}}}
    mod, result, session = await _invoke_aihub(payload, [_resp(body=task_body)])
    assert session.posts[0]["url"] == mod._FABRIC_AIHUB_MCP_URL, "URL must always be the fixed endpoint"
    assert session.posts[0]["json"]["params"]["arguments"]["artifactId"] == "A-1", "artifactId must be forwarded"
    assert "error" not in result, "artifactId must not be treated as a target override"


async def _check_lossless_preservation():
    body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {
        "content": [{"type": "text", "text": "a"}],
        "isError": True,
        "structuredContent": {"nested": {"x": [1, 2, 3]}},
        "_meta": {"m": 1},
        "meta": {"n": 2},
        "brandNewField": "survive",
        "taskId": "T-5",
        "status": "failed",
    }})
    payload = {"operation": "getTaskResult", "input": {"taskId": "T-5"}}
    mod, result, session = await _invoke_aihub(payload, [_resp(body=body)])
    for key in ["content", "isError", "structuredContent", "_meta", "meta", "brandNewField", "taskId", "status"]:
        assert key in result, f"{key} must be preserved losslessly"
    assert result["structuredContent"]["nested"]["x"] == [1, 2, 3]
    assert result["isError"] is True
    assert result["status"] == "failed"


async def _check_safe_logging_and_no_token_leak():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.DEBUG)
    root = logging.getLogger()
    old_level = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {
            "content": [{"type": "text", "text": "BUSINESS-SECRET-ANSWER"}], "isError": False}})
        payload = {"operation": "startTask", "input": {
            "toolName": "ask", "arguments": {"question": "SECRET-QUESTION-PII"}}}
        mod, result, session = await _invoke_aihub(payload, [_resp(body=body)])
    finally:
        root.removeHandler(handler)
        root.setLevel(old_level)
    logs = stream.getvalue()
    assert "fake-fabric-token" not in logs, "token must never be logged"
    assert "SECRET-QUESTION-PII" not in logs, "tool arguments must never be logged"
    assert "BUSINESS-SECRET-ANSWER" not in logs, "business content must never be logged"
    assert "fake-fabric-token" not in json.dumps(result), "token must never appear in the returned dict"


async def _check_auth_header_and_variants():
    task_body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {"task": {"taskId": "T-1", "status": "working"}}})
    payload = {"operation": "startTask", "input": {"toolName": "ask", "arguments": {}}}
    mod, result, session = await _invoke_aihub(payload, [_resp(body=task_body)])
    sent_headers = session.posts[0]["headers"]
    assert sent_headers["Authorization"] == "Bearer fake-fabric-token", "Bearer token from the generic-connection seam"
    assert sent_headers.get("X-Variants") == "Fabric.DisableMsitRedirect", "daily host must get the MSIT-redirect-disable variant"
    assert sent_headers["Accept"] == "application/json, text/event-stream"


async def _check_unsupported_and_missing_input():
    mod, result, session = await _invoke_aihub({"operation": "bogusOp"}, [_resp(body="{}")])
    assert result["error"]["code"] == "UnsupportedOperation"
    assert len(session.posts) == 0
    mod, result2, session2 = await _invoke_aihub({"operation": "startTask", "input": {}}, [_resp(body="{}")])
    assert result2["error"]["code"] == "MissingInput"
    mod, result3, session3 = await _invoke_aihub({"operation": "getTask", "input": {}}, [_resp(body="{}")])
    assert result3["error"]["code"] == "MissingInput"


# ---------------------------------------------------------------------------
# Additional rayfin_fabric_aihub_v1 tests covering the MCP handshake, input
# validation, decode failures, Retry-After HTTP-dates, collision losslessness
# and log-injection sanitization.
# ---------------------------------------------------------------------------


class _RaisingTextResponse:
    # A response whose body cannot be decoded: text() raises UnicodeDecodeError.
    def __init__(self, status=200, headers=None, content_type="application/json"):
        self.status = status
        self.headers = dict(headers) if headers else {}
        if content_type is not None:
            self.headers["Content-Type"] = content_type
        self.content = _AihubContent([b"\xff"])
        self.released = False
        self.text_called = False

    async def text(self):
        self.text_called = True
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

    def release(self):
        self.released = True


async def _check_get_info_session_id_echoed():
    # initialize returns an Mcp-Session-Id; it must be echoed on the initialized
    # notification and tools/list, and the notification must have NO id.
    init_body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {
        "protocolVersion": "2025-06-18", "capabilities": {}, "serverInfo": {"name": "x"}}})
    tools_body = json.dumps({"jsonrpc": "2.0", "id": "2", "result": {"tools": [{"name": "ask"}]}})
    init_resp = _resp(body=init_body, headers={"mcp-session-id": "sess-XYZ"})  # lower-case on purpose
    mod, result, session = await _invoke_aihub(
        {"operation": "getInfo"},
        [init_resp, _resp(status=202, body=""), _resp(body=tools_body)],
    )
    assert len(session.posts) == 3
    assert session.posts[1]["json"]["method"] == "notifications/initialized"
    assert "id" not in session.posts[1]["json"], "notification must carry no id"
    assert session.posts[1]["headers"].get("Mcp-Session-Id") == "sess-XYZ", "session id must be echoed on the notification"
    assert session.posts[2]["headers"].get("Mcp-Session-Id") == "sess-XYZ", "session id must be echoed on tools/list"
    # The initialize request itself must NOT carry a session id (none yet).
    assert "Mcp-Session-Id" not in session.posts[0]["headers"]
    assert result["tools"][0]["name"] == "ask"


async def _check_get_info_no_session_id_still_succeeds():
    # Server sends no Mcp-Session-Id: getInfo must still succeed and no session
    # header is added to later requests.
    init_body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {"capabilities": {}}})
    tools_body = json.dumps({"jsonrpc": "2.0", "id": "2", "result": {"tools": []}})
    mod, result, session = await _invoke_aihub(
        {"operation": "getInfo"},
        [_resp(body=init_body), _resp(status=202, body=""), _resp(body=tools_body)],
    )
    assert "tools" in result, "getInfo must succeed without a session id"
    assert "Mcp-Session-Id" not in session.posts[2]["headers"], "no session header when server sent none"


async def _check_bad_request_payload_and_operation():
    # A non-dict payload must return the structured BadRequest envelope (no raise).
    mod, result, session = await _invoke_aihub(["not", "a", "dict"], [_resp(body="{}")])
    err = result["error"]
    assert err["code"] == "BadRequest"
    assert err["source"] == "connector"
    assert err["retryable"] is False
    assert err["userError"] is True
    assert err["httpStatus"] == 400
    assert len(session.posts) == 0, "malformed payload must never reach the network"

    # A non-string operation (int) must also return BadRequest, not AttributeError.
    mod, result2, session2 = await _invoke_aihub({"operation": 1}, [_resp(body="{}")])
    err2 = result2["error"]
    assert err2["code"] == "BadRequest"
    assert err2["httpStatus"] == 400
    assert len(session2.posts) == 0


async def _check_decode_failure_returns_envelope_and_releases():
    raising = _RaisingTextResponse(status=200)
    payload = {"operation": "getTask", "input": {"taskId": "T-1"}}
    mod, result, session = await _invoke_aihub(payload, [raising])
    err = result["error"]
    assert err["code"] == "UpstreamError", "decode failure must map to UpstreamError"
    assert err["message"] == "Unparseable upstream response"
    assert err["retryable"] is True
    assert err["source"] == "upstream"
    assert raising.text_called is False, "bounded byte streaming must avoid unbounded text()"
    assert raising.released is True, "response must still be released exactly once on the decode-failure path"


async def _check_retry_after_http_date_and_integer():
    # HTTP-date Retry-After -> a non-negative integer under retryAfterSeconds.
    payload = {"operation": "getTask", "input": {"taskId": "T-1"}}
    mod, result, session = await _invoke_aihub(
        payload, [_resp(status=503, body="", content_type="text/plain",
                        headers={"Retry-After": "Wed, 21 Oct 2099 07:28:00 GMT"})])
    err = result["error"]
    assert err["retryable"] is True, "503 must be retryable"
    ra = err["diagnostics"]["retryAfterSeconds"]
    assert isinstance(ra, int) and ra > 0, f"HTTP-date must parse to a positive int, got {ra!r}"

    # A bare integer still works (already asserted elsewhere, re-checked here).
    mod, result2, session2 = await _invoke_aihub(
        payload, [_resp(status=429, body="", content_type="text/plain",
                        headers={"Retry-After": "45"})])
    assert result2["error"]["diagnostics"]["retryAfterSeconds"] == 45

    # A value that is neither seconds nor a valid HTTP-date is kept under the
    # distinct `retryAfter` key, never mislabeled as seconds.
    mod, result3, session3 = await _invoke_aihub(
        payload, [_resp(status=429, body="", content_type="text/plain",
                        headers={"Retry-After": "soon-ish"})])
    diag = result3["error"]["diagnostics"]
    assert "retryAfterSeconds" not in diag, "an unparseable Retry-After must not become a seconds value"
    assert diag["retryAfter"] == "soon-ish", "unparseable Retry-After must be preserved under a distinct key"


async def _check_collision_lossless():
    # A task wrapper whose inner _meta collides with an OUTER _meta must lose
    # neither: inner wins at the top level, outer is recoverable under
    # _collisions.outer.
    body = json.dumps({"jsonrpc": "2.0", "id": "1", "result": {
        "task": {"taskId": "T-1", "status": "working", "_meta": {"inner": 1}},
        "_meta": {"outer": 2},
        "outerOnly": "keep",
    }})
    payload = {"operation": "getTask", "input": {"taskId": "T-1"}}
    mod, result, session = await _invoke_aihub(payload, [_resp(body=body)])
    assert result["taskId"] == "T-1"
    assert result["_meta"] == {"inner": 1}, "inner task _meta wins at the top level"
    assert result["outerOnly"] == "keep", "non-colliding outer field survives"
    assert result["_collisions"]["outer"]["_meta"] == {"outer": 2}, "colliding outer _meta must be recoverable, not dropped"


async def _check_operation_log_injection_sanitized():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.DEBUG)
    root = logging.getLogger()
    old_level = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        # Unsupported caller-controlled operation text may contain PII or
        # secrets, so logs must use only the fixed allowlist fallback.
        mod, result, session = await _invoke_aihub(
            {"operation": "SECRET-PII\r\ninjected-forged-line"}, [_resp(body="{}")])
    finally:
        root.removeHandler(handler)
        root.setLevel(old_level)
    logs = stream.getvalue()
    assert result["error"]["code"] == "UnsupportedOperation"
    assert "SECRET-PII" not in logs, "unsupported operation text must never be logged"
    assert "\ninjected-forged-line" not in logs, "no forged standalone log line may appear"
    assert "op=unsupported" in logs, "logs must use the fixed unsupported-operation label"


async def _check_response_id_correlation():
    mismatched = json.dumps({
        "jsonrpc": "2.0", "id": "another-request",
        "result": {"taskId": "WRONG", "status": "completed"},
    })
    payload = {"operation": "getTask", "input": {"taskId": "T-1"}}
    mod, result, session = await _invoke_aihub(payload, [_resp(body=mismatched)])
    err = result["error"]
    assert err["code"] == "UpstreamError"
    assert err["causeCode"] == "ResponseIdMismatch"
    assert err["taskId"] == "T-1"
    assert result.get("taskId") != "WRONG", "an unrelated JSON-RPC response must never be returned"


async def _check_success_correlation_headers_preserved():
    body = json.dumps({
        "jsonrpc": "2.0", "id": "1",
        "result": {"taskId": "T-1", "status": "working"},
    })
    headers = {
        "x-ms-request-id": "request-success",
        "x-ms-root-activity-id": "root-success",
    }
    payload = {"operation": "getTask", "input": {"taskId": "T-1"}}
    mod, result, session = await _invoke_aihub(
        payload, [_resp(body=body, headers=headers)]
    )
    assert result["requestId"] == "request-success"
    assert result["rootActivityId"] == "root-success"


async def _check_authentication_and_session_setup_errors():
    class _FailingFabricClient:
        def __init__(self, error):
            self._error = error

        def get_access_token(self):
            raise self._error

    mod = _load_function_app()
    auth_errors = (
        RuntimeError("SECRET-CREDENTIAL-DIAGNOSTIC"),
        KeyError("AccessToken"),
        StopIteration(),
        mod.InvalidTokenError("SECRET-JWT-DIAGNOSTIC"),
    )
    for auth_error in auth_errors:
        mod, auth_result, session = await _invoke_aihub(
            {"operation": "getInfo"}, [_resp(body="{}")],
            fabric_client=_FailingFabricClient(auth_error),
        )
        envelope = json.dumps(auth_result)
        auth_err = auth_result["error"]
        assert auth_err["code"] == "AuthenticationFailed"
        assert auth_err["source"] == "connector"
        assert auth_err["httpStatus"] == 500
        assert len(session.posts) == 0
        assert "SECRET-CREDENTIAL-DIAGNOSTIC" not in envelope
        assert "SECRET-JWT-DIAGNOSTIC" not in envelope

    mod = _load_function_app()

    async def _failing_get_session():
        raise RuntimeError("SECRET-SESSION-DIAGNOSTIC")

    mod._get_session = _failing_get_session  # type: ignore[attr-defined]
    setup_result = await mod.rayfin_fabric_aihub_v1(
        {"operation": "getInfo"}, _AihubFabricClient()
    )
    setup_err = setup_result["error"]
    assert setup_err["code"] == "ConnectorUnavailable"
    assert setup_err["source"] == "connector"
    assert setup_err["retryable"] is True
    assert setup_err["httpStatus"] == 503
    assert "SECRET-SESSION-DIAGNOSTIC" not in json.dumps(setup_result)


async def _check_initialized_notification_error():
    init_body = json.dumps({
        "jsonrpc": "2.0", "id": "1", "result": {"capabilities": {}}
    })
    mod, result, session = await _invoke_aihub(
        {"operation": "getInfo"},
        [_resp(body=init_body), _resp(status=500, body="failed", content_type="text/plain")],
    )
    assert result["error"]["code"] == "UpstreamError"
    assert result["error"]["httpStatus"] == 500
    assert len(session.posts) == 2, "tools/list must not run after a failed notification"


async def _check_get_info_success_correlations_preserved():
    init_body = json.dumps({
        "jsonrpc": "2.0", "id": "1", "result": {"capabilities": {}}
    })
    tools_body = json.dumps({
        "jsonrpc": "2.0", "id": "2", "result": {"tools": []}
    })
    tools_headers = {
        "x-ms-request-id": "get-info-request",
        "x-ms-root-activity-id": "get-info-root",
    }
    mod, result, session = await _invoke_aihub(
        {"operation": "getInfo"},
        [
            _resp(body=init_body),
            _resp(status=202, body=""),
            _resp(body=tools_body, headers=tools_headers),
        ],
    )
    assert result["requestId"] == "get-info-request"
    assert result["rootActivityId"] == "get-info-root"


async def _check_oversized_response_rejected():
    mod = _load_function_app()
    original_limit = mod._AIHUB_MAX_RESPONSE_BYTES
    mod._AIHUB_MAX_RESPONSE_BYTES = 5
    try:
        payload = {"operation": "getTask", "input": {"taskId": "T-1"}}
        mod, result, session = await _invoke_aihub(
            payload, [_resp(body="123456", content_type="text/plain")]
        )
    finally:
        mod._AIHUB_MAX_RESPONSE_BYTES = original_limit
    err = result["error"]
    assert err["code"] == "UpstreamError"
    assert err["causeCode"] == "ResponseTooLarge"
    assert session._responses[0].released is True


def test_aihub_get_info_initialize_and_tools_list():
    asyncio.run(_check_get_info_initialize_and_tools_list())


def test_aihub_start_task_creates_task():
    asyncio.run(_check_start_task_creates_task())


def test_aihub_start_task_immediate_result_fallback():
    asyncio.run(_check_start_task_immediate_result_fallback())


def test_aihub_task_ops_route_to_methods():
    asyncio.run(_check_task_ops_route_to_methods())


def test_aihub_sse_response_parsed():
    asyncio.run(_check_sse_response_parsed())


def test_aihub_202_ack_and_json_task():
    asyncio.run(_check_202_ack_and_json_task())


def test_aihub_http_error_envelopes():
    asyncio.run(_check_http_error_envelopes())


def test_aihub_retry_after_surfaced():
    asyncio.run(_check_retry_after_surfaced())


def test_aihub_fixed_target_security():
    asyncio.run(_check_fixed_target_security())


def test_aihub_lossless_preservation():
    asyncio.run(_check_lossless_preservation())


def test_aihub_safe_logging_and_no_token_leak():
    asyncio.run(_check_safe_logging_and_no_token_leak())


def test_aihub_auth_header_and_variants():
    asyncio.run(_check_auth_header_and_variants())


def test_aihub_unsupported_and_missing_input():
    asyncio.run(_check_unsupported_and_missing_input())


def test_aihub_get_info_session_id_echoed():
    asyncio.run(_check_get_info_session_id_echoed())


def test_aihub_get_info_no_session_id_still_succeeds():
    asyncio.run(_check_get_info_no_session_id_still_succeeds())


def test_aihub_bad_request_payload_and_operation():
    asyncio.run(_check_bad_request_payload_and_operation())


def test_aihub_decode_failure_returns_envelope_and_releases():
    asyncio.run(_check_decode_failure_returns_envelope_and_releases())


def test_aihub_retry_after_http_date_and_integer():
    asyncio.run(_check_retry_after_http_date_and_integer())


def test_aihub_collision_lossless():
    asyncio.run(_check_collision_lossless())


def test_aihub_operation_log_injection_sanitized():
    asyncio.run(_check_operation_log_injection_sanitized())


def test_aihub_response_id_correlation():
    asyncio.run(_check_response_id_correlation())


def test_aihub_success_correlation_headers_preserved():
    asyncio.run(_check_success_correlation_headers_preserved())


def test_aihub_authentication_and_session_setup_errors():
    asyncio.run(_check_authentication_and_session_setup_errors())


def test_aihub_initialized_notification_error():
    asyncio.run(_check_initialized_notification_error())


def test_aihub_get_info_success_correlations_preserved():
    asyncio.run(_check_get_info_success_correlations_preserved())


def test_aihub_oversized_response_rejected():
    asyncio.run(_check_oversized_response_rejected())


if __name__ == "__main__":
    test_streams_multiple_chunks_without_buffering()
    print("  ok: streams multiple chunks without buffering")
    test_forwards_client_request_id_header()
    print("  ok: forwards clientRequestId as x-ms-client-request-id header")
    test_execute_command_routes_to_mgmt_and_streams()
    print("  ok: executeCommand routes to /v1/rest/mgmt and streams")
    test_aihub_get_info_initialize_and_tools_list()
    print("  ok: getInfo issues initialize + tools/list and preserves fields")
    test_aihub_start_task_creates_task()
    print("  ok: startTask creates a task and forwards ttl/artifactId")
    test_aihub_start_task_immediate_result_fallback()
    print("  ok: startTask immediate-result fallback is lossless")
    test_aihub_task_ops_route_to_methods()
    print("  ok: getTask/getTaskResult/cancelTask route to the right methods")
    test_aihub_sse_response_parsed()
    print("  ok: SSE stream parsed, last result wins, pings ignored")
    test_aihub_202_ack_and_json_task()
    print("  ok: 202 ack vs 202 JSON task handled")
    test_aihub_http_error_envelopes()
    print("  ok: 401/403/429/500 structured error envelopes")
    test_aihub_retry_after_surfaced()
    print("  ok: Retry-After surfaced and retryable")
    test_aihub_fixed_target_security()
    print("  ok: fixed-target security enforced, artifactId allowed")
    test_aihub_lossless_preservation()
    print("  ok: _meta/meta/structuredContent/additive keys preserved")
    test_aihub_safe_logging_and_no_token_leak()
    print("  ok: token/arguments/business content never logged or returned")
    test_aihub_auth_header_and_variants()
    print("  ok: Bearer token + X-Variants for daily host")
    test_aihub_unsupported_and_missing_input()
    print("  ok: unsupported operation and missing input rejected")
    test_aihub_get_info_session_id_echoed()
    print("  ok: Mcp-Session-Id captured on initialize and echoed; notification has no id")
    test_aihub_get_info_no_session_id_still_succeeds()
    print("  ok: getInfo succeeds when the server sends no session id")
    test_aihub_bad_request_payload_and_operation()
    print("  ok: non-dict payload and non-string operation return BadRequest")
    test_aihub_decode_failure_returns_envelope_and_releases()
    print("  ok: undecodable body -> UpstreamError envelope, response released")
    test_aihub_retry_after_http_date_and_integer()
    print("  ok: Retry-After HTTP-date -> int seconds; integer + unparseable handled")
    test_aihub_collision_lossless()
    print("  ok: colliding outer/inner fields are lossless (recoverable)")
    test_aihub_operation_log_injection_sanitized()
    print("  ok: unsupported operation text is excluded from logs")
    test_aihub_response_id_correlation()
    print("  ok: unrelated JSON-RPC response ids are rejected")
    test_aihub_success_correlation_headers_preserved()
    print("  ok: successful responses preserve request/root activity ids")
    test_aihub_authentication_and_session_setup_errors()
    print("  ok: authentication and HTTP client setup failures are structured")
    test_aihub_initialized_notification_error()
    print("  ok: failed initialized notification returns a structured error")
    test_aihub_get_info_success_correlations_preserved()
    print("  ok: getInfo success preserves request/root activity ids")
    test_aihub_oversized_response_rejected()
    print("  ok: oversized responses are rejected before unbounded buffering")
    print("ALL UDF STREAMING TESTS PASSED")
