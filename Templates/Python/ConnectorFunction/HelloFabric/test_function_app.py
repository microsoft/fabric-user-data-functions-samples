"""
Standalone tests for the Rayfin Kusto and generic Fabric MCP connector functions.

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
import json
import os
import sys
import types
import zipfile


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
# rayfin_fabric_mcp_v1 tests.
# ---------------------------------------------------------------------------

_MCP_TOKEN = "delegated-token"
_PROD_ENDPOINT = "https://api.fabric.microsoft.com/v1/mcp/example"
_DAILY_ENDPOINT = "https://dailyapi.fabric.microsoft.com/v1/mcp/example"


class _McpContent:
    def __init__(self, body=b""):
        self.body = body

    async def iter_chunked(self, _size):
        if self.body:
            midpoint = max(1, len(self.body) // 2)
            yield self.body[:midpoint]
            yield self.body[midpoint:]


class _McpResponse:
    def __init__(self, body, status=200, headers=None):
        self._body = body
        self.status = status
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.content = _McpContent()
        self.content_length = None
        self.released = False

    def prepare(self, request):
        body = self._body(request) if callable(self._body) else self._body
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.content = _McpContent(body)
        self.content_length = len(body)

    def release(self):
        self.released = True


class _McpSession:
    def __init__(self, response):
        self.response = response
        self.posts = []

    async def post(self, url, json=None, headers=None, timeout=None):
        self.posts.append(
            {"url": url, "json": json, "headers": headers, "timeout": timeout}
        )
        self.response.prepare(json)
        return self.response


async def _invoke_mcp(input_data, response, operation="executeQuery", token=_MCP_TOKEN):
    mod = _load_function_app()
    session = _McpSession(response)

    async def _fake_get_session():
        return session

    mod._get_session = _fake_get_session  # type: ignore[attr-defined]
    stream = await mod.rayfin_fabric_mcp_v1(
        {"operation": operation, "input": input_data}, token
    )
    body = b"".join(stream.body)
    return mod, json.loads(body), session, response


def _mcp_input(endpoint=_DAILY_ENDPOINT, **overrides):
    result = {
        "endpoint": endpoint,
        "method": "tools/list",
        "params": {},
        "protocolVersion": "2025-06-18",
    }
    result.update(overrides)
    return result


async def _check_url_allowlist_table():
    mod = _load_function_app()
    accepted = [
        _PROD_ENDPOINT,
        _DAILY_ENDPOINT,
        "https://api.fabric.microsoft.com:443/v1/mcp/example",
    ]
    rejected = {
        "substring bypass": "https://evil.com/v1/mcp/?x=api.fabric.microsoft.com",
        "userinfo": "https://user:pass@api.fabric.microsoft.com/v1/mcp/example",
        "http scheme": "http://api.fabric.microsoft.com/v1/mcp/example",
        "non-default port": "https://api.fabric.microsoft.com:444/v1/mcp/example",
        "wrong path": "https://api.fabric.microsoft.com/v1/not-mcp/example",
        "dot segment": "https://api.fabric.microsoft.com/v1/mcp/../admin",
        "encoded dot segment": "https://api.fabric.microsoft.com/v1/mcp/%2e%2e/admin",
        "encoded slash": "https://api.fabric.microsoft.com/v1/mcp/example%2fadmin",
        "path parameters": "https://api.fabric.microsoft.com/v1/mcp/..;x/admin",
        "final path parameters": "https://api.fabric.microsoft.com/v1/mcp/example;param",
        "punycode lookalike": "https://api.xn--fbric-9za.microsoft.com/v1/mcp/example",
        "suffix confusion": "https://api.fabric.microsoft.com.evil.com/v1/mcp/example",
        "empty DNS label": "https://api..fabric.microsoft.com/v1/mcp/example",
        "trailing DNS dot": "https://api.fabric.microsoft.com./v1/mcp/example",
    }
    for endpoint in accepted:
        assert mod._validate_fabric_mcp_endpoint(endpoint) == endpoint
    for name, endpoint in rejected.items():
        try:
            mod._validate_fabric_mcp_endpoint(endpoint)
        except ValueError:
            pass
        else:
            raise AssertionError(f"{name} endpoint was accepted: {endpoint}")


async def _check_ring_override_validation():
    mod = _load_function_app()
    env_name = mod._FABRIC_MCP_ALLOWED_HOSTS_ENV
    original = os.environ.get(env_name)
    try:
        os.environ[env_name] = "*.fabric.microsoft.com,*.powerbi.com"
        mod._validate_fabric_mcp_endpoint(
            "https://dxtapi.fabric.microsoft.com/v1/mcp/example"
        )
        mod._validate_fabric_mcp_endpoint(
            "https://edogapi.powerbi.com/v1/mcp/example"
        )
        os.environ[env_name] = "evil.com"
        try:
            mod._validate_fabric_mcp_endpoint(_PROD_ENDPOINT)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid operational allowlist bypassed validation")
    finally:
        if original is None:
            os.environ.pop(env_name, None)
        else:
            os.environ[env_name] = original


async def _check_json_request_headers_and_envelope():
    def body(request):
        return json.dumps(
            {"jsonrpc": "2.0", "id": request["id"], "result": {"tools": []}}
        )

    response = _McpResponse(body, headers={"Mcp-Session-Id": "next-session"})
    mod, envelope, session, response = await _invoke_mcp(
        _mcp_input(sessionId="prior-session"), response
    )
    post = session.posts[0]
    assert post["url"] == _DAILY_ENDPOINT
    assert post["headers"]["Authorization"] == f"Bearer {_MCP_TOKEN}"
    assert post["headers"]["Accept"] == "application/json, text/event-stream"
    assert post["headers"]["Content-Type"] == "application/json"
    assert post["headers"]["MCP-Protocol-Version"] == "2025-06-18"
    assert post["headers"]["Mcp-Session-Id"] == "prior-session"
    assert post["headers"]["X-Variants"] == "Fabric.DisableMsitRedirect"
    assert post["timeout"] is mod._FABRIC_MCP_REQUEST_TIMEOUT
    assert post["json"]["method"] == "tools/list"
    assert envelope == {
        "status": "Succeeded",
        "output": {
            "jsonrpc": "2.0",
            "id": post["json"]["id"],
            "result": {"tools": []},
            "sessionId": "next-session",
        },
        "errors": [],
    }
    assert response.released


async def _check_prod_has_no_msit_header():
    response = _McpResponse(
        lambda request: json.dumps(
            {"jsonrpc": "2.0", "id": request["id"], "result": {}}
        )
    )
    _mod, envelope, session, _response = await _invoke_mcp(
        _mcp_input(endpoint=_PROD_ENDPOINT), response
    )
    assert "X-Variants" not in session.posts[0]["headers"]
    assert envelope["status"] == "Succeeded"


async def _check_sse_frames_and_session_propagation():
    def body(request):
        return (
            'data: {"jsonrpc":"2.0","id":"other","result":{"wrong":true}}\n\n'
            f'data: {{"jsonrpc":"2.0","id":"{request["id"]}",'
            '"result":{"content":[{"type":"text","text":"ok"}]}}\n\n'
        )

    response = _McpResponse(
        body,
        headers={
            "Content-Type": "text/event-stream; charset=utf-8",
            "mcp-session-id": "sse-session",
        },
    )
    _mod, envelope, _session, _response = await _invoke_mcp(
        _mcp_input(method="tools/call", params={"name": "sample", "arguments": {}}),
        response,
    )
    assert envelope["status"] == "Succeeded"
    assert envelope["output"]["result"]["content"][0]["text"] == "ok"
    assert envelope["output"]["sessionId"] == "sse-session"


async def _check_failures_use_sdk_envelope():
    invalid = _mcp_input(endpoint="https://evil.com/v1/mcp/example")
    _mod, invalid_envelope, invalid_session, _response = await _invoke_mcp(
        invalid, _McpResponse("{}")
    )
    assert invalid_envelope["status"] == "Failed"
    assert invalid_envelope["output"] == {}
    assert invalid_envelope["errors"][0]["code"] == "InvalidInput"
    assert invalid_session.posts == []

    override = _mcp_input(headers={"Authorization": "replacement"})
    _mod, override_envelope, override_session, _response = await _invoke_mcp(
        override, _McpResponse("{}")
    )
    assert override_envelope["status"] == "Failed"
    assert override_session.posts == []

    response = _McpResponse("service unavailable", status=503)
    _mod, http_envelope, _session, response = await _invoke_mcp(
        _mcp_input(), response
    )
    assert http_envelope["status"] == "Failed"
    assert http_envelope["errors"][0]["httpStatus"] == 503
    assert response.released

    response = _McpResponse(b"\xff", status=400)
    _mod, bad_request_envelope, _session, response = await _invoke_mcp(
        _mcp_input(), response
    )
    assert bad_request_envelope["errors"][0]["code"] == "UpstreamHttpError"
    assert bad_request_envelope["errors"][0]["httpStatus"] == 400
    assert bad_request_envelope["errors"][0]["retryable"] is False
    assert response.released


async def _check_jsonrpc_error_uses_failed_envelope():
    response = _McpResponse(
        lambda request: json.dumps(
            {
                "jsonrpc": "2.0",
                "id": request["id"],
                "error": {"code": -32601, "message": "Method not found"},
            }
        )
    )
    _mod, envelope, session, _response = await _invoke_mcp(
        _mcp_input(method="unknown/method"), response
    )
    assert session.posts[0]["json"]["method"] == "unknown/method"
    assert envelope["status"] == "Failed"
    assert envelope["output"] == {}
    assert envelope["errors"][0]["code"] == -32601


async def _check_notification_without_body_succeeds():
    response = _McpResponse("", status=202, headers={"Mcp-Session-Id": "session"})
    _mod, envelope, session, _response = await _invoke_mcp(
        _mcp_input(method="notifications/initialized"), response
    )
    assert "id" not in session.posts[0]["json"]
    assert envelope == {
        "status": "Succeeded",
        "output": {"sessionId": "session"},
        "errors": [],
    }


async def _check_unsupported_operation_raises():
    mod = _load_function_app()
    try:
        await mod.rayfin_fabric_mcp_v1(
            {"operation": "read", "input": _mcp_input()}, _MCP_TOKEN
        )
    except ValueError as exc:
        assert str(exc) == "Unsupported operation: read"
    else:
        raise AssertionError("unsupported operation did not raise ValueError")


async def _check_non_object_payload_uses_failed_envelope():
    mod = _load_function_app()
    stream = await mod.rayfin_fabric_mcp_v1(None, _MCP_TOKEN)
    envelope = json.loads(b"".join(stream.body))
    assert envelope["status"] == "Failed"
    assert envelope["output"] == {}
    assert envelope["errors"][0]["code"] == "InvalidInput"


def _check_packaged_artifacts_match_sources():
    base = os.path.dirname(os.path.abspath(__file__))
    expected_members = {
        ".vscode/extensions.json",
        ".vscode/launch.json",
        ".vscode/settings.json",
        ".vscode/tasks.json",
        ".funcignore",
        ".gitignore",
        "host.json",
        "local.settings.json",
        "readme.md",
        "requirements.txt",
        "fabric_lib/functions.metadata",
        "function_app.py",
    }
    with open(os.path.join(base, "function_app.py"), "rb") as source:
        function_source = source.read()
    with open(os.path.join(base, "functions.metadata"), "rb") as source:
        metadata_source = source.read()

    for archive_name in ("SourceCode.zip", "Deploy.zip"):
        with zipfile.ZipFile(os.path.join(base, archive_name)) as archive:
            member_names = archive.namelist()
            assert len(member_names) == len(expected_members)
            assert set(member_names) == expected_members
            assert archive.read("function_app.py") == function_source
            assert archive.read("fabric_lib/functions.metadata") == metadata_source
            assert all(
                ((info.external_attr >> 16) & 0o777) == 0o644
                for info in archive.infolist()
            )


def test_fabric_mcp_url_allowlist_table():
    asyncio.run(_check_url_allowlist_table())


def test_fabric_mcp_ring_override_validation():
    asyncio.run(_check_ring_override_validation())


def test_fabric_mcp_json_request_headers_and_envelope():
    asyncio.run(_check_json_request_headers_and_envelope())


def test_fabric_mcp_prod_has_no_msit_header():
    asyncio.run(_check_prod_has_no_msit_header())


def test_fabric_mcp_sse_frames_and_session_propagation():
    asyncio.run(_check_sse_frames_and_session_propagation())


def test_fabric_mcp_failures_use_sdk_envelope():
    asyncio.run(_check_failures_use_sdk_envelope())


def test_fabric_mcp_jsonrpc_error_uses_failed_envelope():
    asyncio.run(_check_jsonrpc_error_uses_failed_envelope())


def test_fabric_mcp_notification_without_body_succeeds():
    asyncio.run(_check_notification_without_body_succeeds())


def test_fabric_mcp_unsupported_operation_raises():
    asyncio.run(_check_unsupported_operation_raises())


def test_fabric_mcp_non_object_payload_uses_failed_envelope():
    asyncio.run(_check_non_object_payload_uses_failed_envelope())


def test_packaged_artifacts_match_sources():
    _check_packaged_artifacts_match_sources()


if __name__ == "__main__":
    test_streams_multiple_chunks_without_buffering()
    print("  ok: streams multiple chunks without buffering")
    test_forwards_client_request_id_header()
    print("  ok: forwards clientRequestId as x-ms-client-request-id header")
    test_execute_command_routes_to_mgmt_and_streams()
    print("  ok: executeCommand routes to /v1/rest/mgmt and streams")
    test_fabric_mcp_url_allowlist_table()
    print("  ok: Fabric MCP URL allowlist table")
    test_fabric_mcp_ring_override_validation()
    print("  ok: Fabric MCP ring override validation")
    test_fabric_mcp_json_request_headers_and_envelope()
    print("  ok: Fabric MCP JSON request, headers, and success envelope")
    test_fabric_mcp_prod_has_no_msit_header()
    print("  ok: production omits MSIT redirect override")
    test_fabric_mcp_sse_frames_and_session_propagation()
    print("  ok: SSE parsing and MCP session propagation")
    test_fabric_mcp_failures_use_sdk_envelope()
    print("  ok: failures use SDK envelope")
    test_fabric_mcp_jsonrpc_error_uses_failed_envelope()
    print("  ok: JSON-RPC errors use failed envelope")
    test_fabric_mcp_notification_without_body_succeeds()
    print("  ok: MCP notification acknowledgement")
    test_fabric_mcp_unsupported_operation_raises()
    print("  ok: unsupported operation raises")
    test_fabric_mcp_non_object_payload_uses_failed_envelope()
    print("  ok: malformed payload uses failed envelope")
    test_packaged_artifacts_match_sources()
    print("  ok: packaged artifacts match loose sources")
    print("ALL CONNECTOR FUNCTION TESTS PASSED")
