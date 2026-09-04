import asyncio
import json
import logging
from pathlib import Path

import pytest

from test_function_app import _load_function_app


class _Content:
    def __init__(self, chunks=(), block=False, close_error=None):
        self.chunks = iter(chunks)
        self.block = block
        self.close_error = close_error
        self.started = asyncio.Event()
        self.closed = 0

    def iter_any(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.started.set()
        if self.block:
            await asyncio.Future()
        try:
            return next(self.chunks)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self):
        self.closed += 1
        if self.close_error is not None:
            raise self.close_error


class _Response:
    def __init__(
        self,
        message=None,
        *,
        raw=None,
        status=200,
        content_type="application/json",
        release_error=None,
    ):
        if raw is None:
            raw = json.dumps(message, separators=(",", ":")).encode()
        self.status = status
        self.headers = {"Content-Type": content_type}
        self.content = _Content((raw,))
        self.release_error = release_error
        self.released = 0
        self.closed = 0

    def release(self):
        self.released += 1
        if self.release_error is not None:
            raise self.release_error

    def close(self):
        self.closed += 1


class _Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    async def post(self, url, **kwargs):
        self.requests.append({"url": url, **kwargs})
        return next(self.responses)


def _request(
    method="tools/list",
    request_id="sdk-id",
    params=None,
    protocol_version="2026-07-28",
    version=1,
    headers=None,
    message=None,
):
    return {
        "version": version,
        "protocolVersion": protocol_version,
        "headers": (
            {
                "X-Variants": (
                    "Fabric.Routing.M365.V1,Fabric.DisableMsitRedirect"
                )
            }
            if headers is None
            else headers
        ),
        "message": (
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": {} if params is None else params,
            }
            if message is None
            else message
        ),
    }


def _invoke(app, payload, responses, token="ephemeral-token"):
    session = _Session(responses)
    output = asyncio.run(
        app._invoke_fabric_mcp(
            payload, lambda: token, session_provider=lambda: session
        )
    )
    return output, session


def test_raw_contract_constants_and_removed_semantics():
    app = _load_function_app()
    assert not hasattr(app, "_FABRIC_MCP_PROTOCOL_VERSION")
    assert app._FABRIC_MCP_MAX_PROTOCOL_VERSION_LENGTH == 128
    assert not hasattr(app, "_FABRIC_MCP_METHODS")
    assert not hasattr(app, "_FABRIC_MCP_INPUT_FIELDS")
    assert not hasattr(app, "_FABRIC_MCP_REQUEST_FIELDS")
    assert not hasattr(app, "_valid_mcp_id")
    assert not hasattr(app, "_safe_mcp_task_id")
    assert app._FABRIC_MCP_ENDPOINT == (
        "https://api.fabric.microsoft.com/v1/mcp/fabriciq"
    )
    assert not hasattr(app, "_FABRIC_MCP_VARIANTS")
    assert not hasattr(app, "_FABRIC_MCP_ALLOWED_VARIANTS")
    assert app._FABRIC_MCP_MAX_BYTES == 5_242_880
    assert app._FABRIC_MCP_MAX_DEPTH == 64
    assert app._FABRIC_MCP_TIMEOUT_SECONDS == 300
    for removed in (
        "_merge_fabric_mcp_request_meta",
        "_validate_task_result",
        "_validate_call_tool_result",
        "_validate_discovery",
        "_sanitize_mcp_error",
        "_materialize_poll_interval",
    ):
        assert not hasattr(app, removed)


def test_arbitrary_objects_notifications_and_extensions_pass_through():
    app = _load_function_app()
    messages = (
        {"opaque": {"notJsonRpc": True}},
        {"method": "notifications/future", "extension": [1, 2, 3]},
        {"params": {"taskId": "task-1"}, "futureField": "preserved"},
    )
    responses = [
        _Response({"opaqueResponse": index}) for index, _ in enumerate(messages)
    ]
    session = _Session(responses)
    for index, message in enumerate(messages):
        payload = _request(message=message)
        output = asyncio.run(
            app._invoke_fabric_mcp(
                payload, lambda: "token", session_provider=lambda: session
            )
        )
        assert output["message"] == {"opaqueResponse": index}
        assert json.loads(session.requests[index]["data"]) == payload["message"]
    assert len(session.requests) == len(messages)


def test_outer_fields_are_validated_by_name_not_order_and_allow_extensions():
    app = _load_function_app()
    payload = _request(message={"notification": True})
    reordered = {
        "message": payload["message"],
        "headers": payload["headers"],
        "version": payload["version"],
        "protocolVersion": payload["protocolVersion"],
        "futureOuterField": {"ignored": True},
    }
    output, _ = _invoke(app, reordered, [_Response({"ack": True})])
    assert output == {"version": 1, "message": {"ack": True}}


@pytest.mark.parametrize(
    "field", ("version", "protocolVersion", "headers", "message")
)
def test_missing_required_outer_field_is_rejected(field):
    app = _load_function_app()
    payload = _request(message={"opaque": True})
    del payload[field]
    session = _Session(())
    with pytest.raises(app.FabricMcpRequestError):
        asyncio.run(
            app._invoke_fabric_mcp(
                payload, lambda: "token", session_provider=lambda: session
            )
        )
    assert session.requests == []


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("version", "1"),
        ("version", 2),
        ("protocolVersion", 1),
        ("headers", []),
        ("message", []),
    ),
)
def test_invalid_required_outer_field_is_rejected(field, value):
    app = _load_function_app()
    payload = _request(message={"opaque": True})
    payload[field] = value
    session = _Session(())
    with pytest.raises(app.FabricMcpRequestError):
        asyncio.run(
            app._invoke_fabric_mcp(
                payload, lambda: "token", session_provider=lambda: session
            )
        )
    assert session.requests == []


@pytest.mark.parametrize("protocol_version", ("2026-07-28", "2027-01-15"))
def test_protocol_version_and_server_owned_headers_pass_through(protocol_version):
    app = _load_function_app()
    output, session = _invoke(
        app,
        _request(
            "vendor.example/extension",
            "get",
            {"taskId": "server-task"},
            protocol_version=protocol_version,
        ),
        [_Response({"jsonrpc": "2.0", "id": "get", "result": {}})],
    )
    assert output["version"] == 1
    request = session.requests[0]
    assert request["url"] == app._FABRIC_MCP_ENDPOINT
    assert request["allow_redirects"] is False
    assert request["timeout"].total == 300
    assert request["headers"] == {
        "Authorization": "Bearer ephemeral-token",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Mcp-Protocol-Version": protocol_version,
        "X-Variants": "Fabric.Routing.M365.V1,Fabric.DisableMsitRedirect",
    }


def test_x_variants_is_required_allowlisted_and_forwarded_unchanged():
    app = _load_function_app()
    variants = "Fabric.Routing.M365.V1,Fabric.DisableMsitRedirect"
    output, session = _invoke(
        app,
        _request(headers={"X-Variants": variants}),
        [_Response({"jsonrpc": "2.0", "id": "sdk-id", "result": {}})],
    )
    assert output["version"] == 1
    assert session.requests[0]["headers"]["X-Variants"] == variants


@pytest.mark.parametrize(
    "headers",
    (
        {
            "X-Variants": "Fabric.Routing.M365.V1,Fabric.DisableMsitRedirect",
            "aUtHoRiZaTiOn": "secret",
        },
        {"Host": "attacker.example"},
        {":authority": "attacker.example"},
        {"Content-Length": "1"},
        {"Connection": "close"},
        {"Transfer-Encoding": "chunked"},
        {"Cookie": "secret"},
        {"X-Api-Key": "secret"},
        {"X-Api_Key": "secret"},
        {"X-ApiKey-V2": "secret"},
        {"X-Access-Token": "secret"},
        {"X-Access_Token": "secret"},
        {"X-AccessToken-Id": "secret"},
        {"X.Credentials": "secret"},
        {"X-CredentialId": "secret"},
        {"mCp-Method": "tools/call"},
        {"Proxy-Custom": "value"},
        {"X-Test": "one", "x-test": "two"},
        {"Bad Name": "value"},
        {"X-Test": ""},
        {"X-Test": "bad\rvalue"},
        {"X-Test": "bad\nvalue"},
        {"X-Test": "bad\tvalue"},
        {"X-Test": "é"},
        {"X-Test": "x" * 129},
        {f"X-{index}": "value" for index in range(33)},
    ),
)
def test_application_headers_reject_reserved_unsafe_or_excessive_values(headers):
    app = _load_function_app()
    session = _Session(())
    with pytest.raises(app.FabricMcpRequestError):
        asyncio.run(
            app._invoke_fabric_mcp(
                _request(headers=headers),
                lambda: "token",
                session_provider=lambda: session,
            )
        )
    assert session.requests == []


def test_generic_application_headers_are_forwarded_unchanged():
    app = _load_function_app()
    application_headers = {
        "X-Variants": "Fabric.Routing.M365.V1,Fabric.DisableMsitRedirect",
        "X-Client-Feature": "alpha,beta",
    }
    _, session = _invoke(
        app,
        _request(headers=application_headers),
        [_Response({"jsonrpc": "2.0", "id": "sdk-id", "result": {}})],
    )
    for name, value in application_headers.items():
        assert session.requests[0]["headers"][name] == value


@pytest.mark.parametrize(
    "payload",
    (
        None,
        {},
        {"version": 1, "protocolVersion": "2026-07-28", "message": {}},
        _request(message=[]),
        _request(version=2),
    ),
)
def test_invalid_boundary_fails_before_network(payload):
    app = _load_function_app()
    session = _Session(())
    with pytest.raises(app.FabricMcpRequestError):
        asyncio.run(
            app._invoke_fabric_mcp(
                payload, lambda: "token", session_provider=lambda: session
            )
        )
    assert session.requests == []


def test_task_fields_are_opaque_and_never_become_headers():
    app = _load_function_app()
    payload = _request(
        message={"params": {"taskId": "task\nis opaque"}, "Mcp-Name": "opaque"}
    )
    _, session = _invoke(
        app,
        payload,
        [_Response({"ack": True})],
    )
    assert "Mcp-Name" not in session.requests[0]["headers"]
    assert "Mcp-Method" not in session.requests[0]["headers"]
    assert json.loads(session.requests[0]["data"]) == payload["message"]


@pytest.mark.parametrize(
    "protocol_version",
    ("", " bad", "bad ", "bad\rvalue", "bad\nvalue", "bad\tvalue", "bad\x7fvalue", "é", "x" * 129),
)
def test_unsafe_protocol_version_fails_before_network(protocol_version):
    app = _load_function_app()
    session = _Session(())
    with pytest.raises(app.FabricMcpRequestError):
        asyncio.run(
            app._invoke_fabric_mcp(
                _request(protocol_version=protocol_version),
                lambda: "token",
                session_provider=lambda: session,
            )
        )
    assert session.requests == []


def test_task_and_jsonrpc_error_are_not_transformed():
    app = _load_function_app()
    task = {
        "jsonrpc": "2.0",
        "id": "task",
        "result": {
            "resultType": "task",
            "taskId": "t",
            "status": "working",
            "_meta": {"opaque": [1, {"sdk": True}]},
        },
    }
    error = {
        "jsonrpc": "2.0",
        "id": "error",
        "error": {"code": -32042, "message": "SDK owned", "data": {"retry": False}},
    }
    session = _Session((_Response(task), _Response(error)))
    task_output = asyncio.run(
        app._invoke_fabric_mcp(
            _request("tools/call", "task", {"name": "tool"}),
            lambda: "token",
            session_provider=lambda: session,
        )
    )
    error_output = asyncio.run(
        app._invoke_fabric_mcp(
            _request("tools/list", "error"),
            lambda: "token",
            session_provider=lambda: session,
        )
    )
    assert task_output == {"version": 1, "message": task}
    assert "pollIntervalMs" not in task_output["message"]["result"]
    assert error_output == {"version": 1, "message": error}


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("result", ["SDK", {"owns": "shape"}]),
        ("error", {"custom": ["opaque"], "_meta": {"sdk": True}}),
    ),
)
def test_upstream_result_and_error_contents_are_opaque(field, value):
    app = _load_function_app()
    message = {"jsonrpc": "2.0", "id": "sdk-id", field: value}
    output, _ = _invoke(app, _request(), [_Response(message)])
    assert output == {"version": 1, "message": message}


def test_sse_returns_last_bounded_generic_object():
    app = _load_function_app()
    raw = (
        b'event: custom-intermediate\n'
        b'data: {"arbitrary":"intermediate"}\n\n'
        b'event: another-custom-name\n'
        b'data: {"opaque":{"final":true}}\n\n'
    )
    output, _ = _invoke(
        app,
        _request(),
        [_Response(raw=raw, content_type="text/event-stream")],
    )
    assert output["message"] == {"opaque": {"final": True}}


@pytest.mark.parametrize("payload", ("[]", "null", '"scalar"', "1", "true"))
def test_sse_rejects_non_object_data_events(payload):
    app = _load_function_app()
    raw = (
        f"event: custom\n"
        f"data: {payload}\n\n"
        f'data: {{"final":true}}\n\n'
    ).encode()
    with pytest.raises(app.FabricMcpResponseError):
        _invoke(
            app,
            _request(message={"opaque": True}),
            [_Response(raw=raw, content_type="text/event-stream")],
        )


def test_sse_events_cannot_bypass_generic_depth(monkeypatch):
    app = _load_function_app()
    monkeypatch.setattr(app, "_FABRIC_MCP_MAX_DEPTH", 4)
    deep = (
        b'data: {"jsonrpc":"2.0","method":"notifications/progress",'
        b'"params":{"a":{"b":{"c":1}}}}\n\n'
        b'data: {"jsonrpc":"2.0","id":"sdk-id","result":{}}\n\n'
    )
    with pytest.raises(app.FabricMcpBoundsError):
        _invoke(
            app,
            _request(),
            [_Response(raw=deep, content_type="text/event-stream")],
        )

    monkeypatch.setattr(app, "_FABRIC_MCP_MAX_DEPTH", 64)
    extended = (
        b'data: {"extension":{"arbitrary":true}}\n\n'
        b'data: {"final":{"opaque":true}}\n\n'
    )
    output, _ = _invoke(
        app,
        _request(message={"notification": True}),
        [_Response(raw=extended, content_type="text/event-stream")],
    )
    assert output["message"] == {"final": {"opaque": True}}


@pytest.mark.parametrize(
    "message",
    ({"id": 1}, {"id": "other"}, {"notification": True}, {"extension": []}),
)
def test_response_objects_are_not_correlated_or_interpreted(message):
    app = _load_function_app()
    output, session = _invoke(app, _request(request_id="1"), [_Response(message)])
    assert output == {"version": 1, "message": message}
    assert len(session.requests) == 1


def test_bounds_depth_http_and_endpoint_fail_closed(monkeypatch):
    app = _load_function_app()
    monkeypatch.setattr(app, "_FABRIC_MCP_MAX_BYTES", 64)
    response = _Response(raw=b"x" * 65)
    with pytest.raises(app.FabricMcpBoundsError):
        asyncio.run(app._read_mcp_response(response))
    assert response.released == 1

    monkeypatch.setattr(app, "_FABRIC_MCP_MAX_BYTES", 5_242_880)
    monkeypatch.setattr(app, "_FABRIC_MCP_MAX_DEPTH", 3)
    with pytest.raises(app.FabricMcpRequestError):
        _invoke(app, _request(params={"deep": {"x": 1}}), [])

    monkeypatch.setattr(app, "_FABRIC_MCP_MAX_DEPTH", 64)
    with pytest.raises(app.FabricMcpResponseError):
        _invoke(app, _request(), [_Response({}, status=302)])
    monkeypatch.setenv("FABRIC_MCP_ENDPOINT", "https://attacker.example")
    with pytest.raises(app.FabricMcpConfigurationError):
        app._load_mcp_endpoint()


def test_cancellation_and_release_failure_cleanup():
    app = _load_function_app()
    blocked = _Response({})
    blocked.content = _Content(block=True)
    session = _Session((blocked,))

    async def cancel():
        invocation = asyncio.create_task(
            app._invoke_fabric_mcp(
                _request(),
                lambda: "token",
                session_provider=lambda: session,
            )
        )
        await blocked.content.started.wait()
        invocation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await invocation

    asyncio.run(cancel())
    assert blocked.released == 1
    assert blocked.content.closed == 1

    failed = _Response(
        {"jsonrpc": "2.0", "id": "sdk-id", "result": {}},
        release_error=RuntimeError("private detail"),
    )
    with pytest.raises(app.FabricMcpResponseError) as error:
        _invoke(app, _request(), [failed])
    assert str(error.value) == app._FABRIC_MCP_UPSTREAM_ERROR
    assert failed.closed == 1
    assert failed.content.closed == 1


def test_logs_and_errors_exclude_token_and_payload():
    app = _load_function_app()
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = Capture()
    app._fabric_mcp_logger.addHandler(handler)
    try:
        with pytest.raises(app.FabricMcpResponseError) as error:
            _invoke(
                app,
                _request("tools/call", params={"name": "customer-secret"}),
                [_Response(raw=b"customer-secret", status=500)],
                "token-secret",
            )
    finally:
        app._fabric_mcp_logger.removeHandler(handler)
    combined = repr(error.value) + repr(records)
    assert "token-secret" not in combined
    assert "customer-secret" not in combined


def test_managed_wrapper_uses_fabric_item_obo(monkeypatch):
    app = _load_function_app()
    captured = {}

    class Token:
        token = "obo-token"

    class Credential:
        def get_token(self):
            return Token()

    class FabricItem:
        def get_access_token(self):
            return Credential()

    async def fake_invoke(payload, token_provider, session_provider=app._get_session):
        captured["payload"] = payload
        captured["token"] = token_provider()
        return {"version": 1, "message": {}}

    monkeypatch.setattr(app, "_invoke_fabric_mcp", fake_invoke)
    payload = _request()
    output = asyncio.run(app.rayfin_fabric_mcp_v1(payload, FabricItem()))
    assert output["version"] == 1
    assert captured == {"payload": payload, "token": "obo-token"}


def test_metadata_declares_only_payload_and_fabric_item():
    metadata = json.loads(Path(__file__).with_name("functions.metadata").read_text())
    function = next(
        item for item in metadata if item["name"] == "rayfin_fabric_mcp_v1"
    )
    assert function["fabricProperties"]["fabricFunctionParameters"] == [
        {"name": "payload", "dataType": "dict"}
    ]
    assert function["bindings"][1] == {
        "name": "fabricIqClient",
        "direction": "In",
        "type": "FabricItem",
        "audienceType": "Fabric",
    }
