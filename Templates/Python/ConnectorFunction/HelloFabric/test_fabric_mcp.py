import asyncio
import importlib
import json
from pathlib import Path

import pytest
import aiohttp
from aiohttp import web
from multidict import CIMultiDict

from test_function_app import _load_function_app


@pytest.fixture(autouse=True)
def _clear_endpoint_settings(monkeypatch):
    monkeypatch.delenv("FABRIC_API_BASE", raising=False)
    importlib.reload(_load_function_app())


class _Response:
    def __init__(self, text, status=200, headers=None):
        self.status = status
        self.headers = CIMultiDict(headers or {})
        self.text_value = text
        self.text_count = 0
        self.released = False

    async def text(self, encoding=None):
        assert encoding == "utf-8"
        self.text_count += 1
        if isinstance(self.text_value, BaseException):
            raise self.text_value
        return self.text_value

    def release(self):
        self.released = True


class _Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    async def post(self, url, **kwargs):
        self.requests.append({"url": url, **kwargs})
        response = next(self.responses)
        if isinstance(response, BaseException):
            raise response
        return response


def _request(message=None, headers=None, protocol_version="2026-07-28"):
    return {
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
        "message": {"opaque": True} if message is None else message,
    }


def _invoke(app, payload, responses, token="obo-token"):
    session = _Session(responses)
    output = asyncio.run(
        app._invoke_fabric_mcp(
            payload, lambda: token, session_provider=lambda: session
        )
    )
    return output, session


def test_direct_relay_has_only_fixed_endpoint_and_obo_policy():
    app = _load_function_app()
    _, session = _invoke(app, _request(), [_Response("response")])
    assert session.requests[0]["url"] == (
        "https://api.fabric.microsoft.com/v1/mcp/fabriciq"
    )
    for removed in (
        "_FABRIC_MCP_MAX_BYTES",
        "_FABRIC_MCP_RESERVED_HEADERS",
        "_safe_mcp_application_headers",
        "_parse_mcp_request",
        "_read_mcp_response",
        "_decode_mcp_sse",
        "_decode_mcp_response",
        "FabricMcpBoundsError",
    ):
        assert not hasattr(app, removed)


@pytest.mark.parametrize(
    "origin",
    (
        "https://api.fabric.microsoft.com",
        "https://msitapi.fabric.microsoft.com",
        "https://dxtapi.fabric.microsoft.com",
        "https://dailyapi.fabric.microsoft.com",
        "https://managed.example",
        "https://api.fabric.microsoft.com/",
    ),
)
def test_managed_fabric_origin_appends_fixed_route(monkeypatch, origin):
    monkeypatch.setenv("FABRIC_API_BASE", origin)
    app = importlib.reload(_load_function_app())
    payload = _request(message={"opaque": True})
    payload["FABRIC_API_BASE"] = "https://attacker.example"
    payload["endpoint"] = "https://attacker.example/alternate"
    output, session = _invoke(app, payload, [_Response("opaque response")])
    assert output == {"status": 200, "headers": {}, "message": "opaque response"}
    assert len(session.requests) == 1
    assert session.requests[0]["url"] == origin + "/v1/mcp/fabriciq"
    assert session.requests[0]["allow_redirects"] is False
    assert session.requests[0]["headers"]["Authorization"] == "Bearer obo-token"
    assert json.loads(session.requests[0]["data"]) == payload["message"]


def test_empty_managed_base_is_not_replaced_with_prod(monkeypatch):
    monkeypatch.setenv("FABRIC_API_BASE", "")
    app = importlib.reload(_load_function_app())
    assert app._FABRIC_API_BASE == ""
    _, session = _invoke(app, _request(), [_Response("response")])
    assert session.requests[0]["url"] == "/v1/mcp/fabriciq"


def test_empty_managed_base_fails_explicitly_in_http_client(monkeypatch):
    app = _load_function_app()
    monkeypatch.setattr(app, "_FABRIC_API_BASE", "")

    async def run():
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RuntimeError, match="^Fabric MCP upstream transport failed\\.$"):
                await app._invoke_fabric_mcp(
                    _request(), lambda: "obo-token", lambda: session
                )

    asyncio.run(run())


def test_managed_base_is_captured_at_module_load(monkeypatch):
    monkeypatch.setenv("FABRIC_API_BASE", "https://msitapi.fabric.microsoft.com")
    app = importlib.reload(_load_function_app())
    monkeypatch.setenv("FABRIC_API_BASE", "https://dailyapi.fabric.microsoft.com")
    _, session = _invoke(app, _request(), [_Response("response")])
    assert session.requests[0]["url"] == (
        "https://msitapi.fabric.microsoft.com/v1/mcp/fabriciq"
    )
    importlib.reload(app)
    _, reloaded_session = _invoke(app, _request(), [_Response("response")])
    assert reloaded_session.requests[0]["url"] == (
        "https://dailyapi.fabric.microsoft.com/v1/mcp/fabriciq"
    )


def test_fixed_url_opaque_body_headers_and_final_authorization_overwrite():
    app = _load_function_app()
    message = {
        "arbitrary": [1, {"nested": True}],
        "endpoint": "https://attacker.example",
    }
    caller_headers = {
        "authorization": "Bearer caller-token",
        "AUTHORIZATION": "another-attacker-token",
        "Host": "alternate.example",
        "Content-Length": "999",
        "Transfer-Encoding": "chunked",
        "Connection": "X-Hop, keep-alive",
        "X-Hop": "remove-me",
        "Keep-Alive": "timeout=1",
        "Proxy-Authorization": "proxy-secret",
        "Proxy-Authenticate": "Basic",
        "Proxy-Connection": "keep-alive",
        "TE": "trailers",
        "Trailer": "X-Trailer",
        "Upgrade": "websocket",
        "X-Rewrite-URL": "/alternate",
        "X-Real-IP": "192.0.2.1",
        "X-HTTP-Method-Override": "DELETE",
        "X-MS-CLIENT-PRINCIPAL": "opaque-identity",
        "X-Variants": "Fabric.Routing.M365.V1,Fabric.DisableMsitRedirect",
    }
    original_headers = caller_headers.copy()
    output, session = _invoke(
        app,
        _request(message=message, headers=caller_headers),
        [_Response("opaque upstream response")],
    )

    assert output == {
        "status": 200, "headers": {}, "message": "opaque upstream response"
    }
    assert len(session.requests) == 1
    request = session.requests[0]
    assert request["url"] == "https://api.fabric.microsoft.com/v1/mcp/fabriciq"
    assert request["allow_redirects"] is False
    assert "timeout" not in request
    assert json.loads(request["data"]) == message
    assert request["headers"]["Authorization"] == "Bearer obo-token"
    assert [
        name for name in request["headers"] if name.lower() == "authorization"
    ] == ["Authorization"]
    forwarded_headers = {
        name: value for name, value in caller_headers.items()
        if name.lower() != "authorization"
    }
    assert request["headers"] == {
        **forwarded_headers,
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2026-07-28",
        "Authorization": "Bearer obo-token",
    }
    assert caller_headers == original_headers


def test_default_transport_headers_are_added_without_overwriting_caller_values():
    app = _load_function_app()
    supplied = {
        "content-type": "application/custom+json",
        "accept": "application/custom-response",
        "mcp-protocol-version": "caller-value",
        "X-App": "value",
    }
    _, supplied_session = _invoke(
        app, _request(headers=supplied), [_Response("response")]
    )
    outbound = supplied_session.requests[0]["headers"]
    assert {name: outbound[name] for name in supplied} == supplied
    for name in ("content-type", "accept", "mcp-protocol-version"):
        assert sum(key.lower() == name for key in outbound) == 1

    _, default_session = _invoke(app, _request(headers={}), [_Response("response")])
    defaults = default_session.requests[0]["headers"]
    assert defaults == {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2026-07-28",
        "Authorization": "Bearer obo-token",
    }
    assert "skip_auto_headers" not in default_session.requests[0]


@pytest.mark.parametrize(
    "message",
    (
        {"notJsonRpc": True},
        {"id": None, "method": ["not", "validated"]},
        {"params": {"taskId": "opaque\nvalue"}},
    ),
)
def test_inner_message_is_serialized_without_mcp_validation(message):
    app = _load_function_app()
    _, session = _invoke(app, _request(message=message), [_Response("response")])
    assert json.loads(session.requests[0]["data"]) == message


@pytest.mark.parametrize(
    "response",
    (
        '{"object":true}',
        '["array",1]',
        "scalar",
        "",
        "data: opaque\n\n",
        ': keepalive\nid: 1\ndata: {"result":"caf\u00e9"}\n\ndata: [DONE]\n\n',
    ),
)
def test_upstream_text_is_returned_without_json_or_sse_parsing(response):
    app = _load_function_app()
    upstream = _Response(response)
    output, _ = _invoke(app, _request(), [upstream])
    assert output == {"status": 200, "headers": {}, "message": response}
    assert upstream.text_count == 1
    assert upstream.released


def test_large_request_has_no_relay_owned_size_limit():
    app = _load_function_app()
    message = {"large": "x" * (5 * 1024 * 1024 + 1)}
    output, session = _invoke(app, _request(message=message), [_Response("accepted")])
    assert output == {"status": 200, "headers": {}, "message": "accepted"}
    assert len(session.requests[0]["data"]) > 5 * 1024 * 1024


def test_only_managed_fabric_base_selects_the_destination(monkeypatch):
    monkeypatch.setenv("FABRIC_API_BASE", "https://dailyapi.fabric.microsoft.com")
    monkeypatch.setenv("FABRIC_MCP_ENDPOINT", "https://unrelated.example")
    monkeypatch.setenv("FABRIC_MCP_RING", "unrelated")
    app = importlib.reload(_load_function_app())
    output, session = _invoke(app, _request(), [_Response("opaque response")])
    assert output == {"status": 200, "headers": {}, "message": "opaque response"}
    assert session.requests[0]["url"] == (
        "https://dailyapi.fabric.microsoft.com/v1/mcp/fabriciq"
    )


@pytest.mark.parametrize("status", (202, 204, 301, 302, 303, 307, 308, 400, 403, 429, 500))
def test_every_http_response_preserves_status_headers_and_complete_body(status):
    app = _load_function_app()
    body = "" if status == 204 else "opaque upstream detail"
    response = _Response(body, status=status, headers={
        "content-type": "text/event-stream; charset=utf-8",
        "mcp-session-id": "session-123",
        "mcp-protocol-version": "2026-07-28",
        "retry-after": "7",
        "Set-Cookie": "private-cookie",
        "Authorization": "private-credential",
        "Content-Length": "111",
        "Transfer-Encoding": "chunked",
        "Location": "https://attacker.example",
        "X-Private": "private",
    })
    output, session = _invoke(app, _request(), [response])
    assert output == {
        "status": status,
        "headers": {
            "content-type": "text/event-stream; charset=utf-8",
            "mcp-session-id": "session-123",
            "mcp-protocol-version": "2026-07-28",
            "retry-after": "7",
            "Set-Cookie": "private-cookie",
            "Authorization": "private-credential",
            "Content-Length": "111",
            "Transfer-Encoding": "chunked",
            "Location": "https://attacker.example",
            "X-Private": "private",
        },
        "message": body,
    }
    assert response.text_count == 1
    assert response.released
    assert len(session.requests) == 1
    assert session.requests[0]["allow_redirects"] is False


@pytest.mark.parametrize("headers", ({}, {"Retry-After": ""}, {"MCP-Session-Id": "s"}))
def test_response_metadata_never_fabricates_missing_values(headers):
    output, _ = _invoke(_load_function_app(), _request(), [_Response("", headers=headers)])
    assert output == {"status": 200, "headers": headers, "message": ""}


@pytest.mark.parametrize("name", ("Set-Cookie", "MCP-Session-Id", "X-Custom"))
@pytest.mark.parametrize("first", ("first-value", ""))
def test_repeated_response_headers_keep_first_value_and_casing(name, first):
    response = _Response("", headers=[
        (name, first),
        (name, "second-value"),
        (name.lower(), "third-value"),
        ("X-Unrelated", "unchanged"),
    ])
    output, _ = _invoke(_load_function_app(), _request(), [response])
    assert output["headers"] == {name: first, "X-Unrelated": "unchanged"}


@pytest.mark.parametrize(
    "failure",
    (
        asyncio.TimeoutError("private detail"),
        aiohttp.ClientConnectionError("private detail"),
        aiohttp.ClientPayloadError("private detail"),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "private detail"),
    ),
)
@pytest.mark.parametrize("during_body", (False, True))
def test_transport_failure_is_explicit_sanitized_and_not_retried(failure, during_body):
    response = _Response(failure) if during_body else failure
    session = _Session([response])
    with pytest.raises(RuntimeError, match="^Fabric MCP upstream transport failed\\.$"):
        asyncio.run(_load_function_app()._invoke_fabric_mcp(
            _request(), lambda: "obo-token", lambda: session
        ))
    assert len(session.requests) == 1
    if during_body:
        assert response.released


def test_credential_failure_is_not_an_http_success():
    def fail_token():
        raise RuntimeError("Credential unavailable")

    def unexpected_session():
        pytest.fail("No network without credentials")

    with pytest.raises(RuntimeError, match="Credential unavailable"):
        asyncio.run(_load_function_app()._invoke_fabric_mcp(
            _request(), fail_token, unexpected_session
        ))


def test_async_credential_and_session_providers_are_preserved():
    session = _Session([_Response("")])

    async def token_provider():
        return "obo-token"

    async def session_provider():
        return session

    output = asyncio.run(_load_function_app()._invoke_fabric_mcp(
        _request(), token_provider, session_provider
    ))
    assert output == {"status": 200, "headers": {}, "message": ""}


@pytest.mark.parametrize("token", ("", "bad\rtoken", "bad\ntoken", None))
def test_invalid_obo_token_fails_before_network(token):
    app = _load_function_app()
    session = _Session(())
    with pytest.raises(app.FabricMcpRequestError) as error:
        asyncio.run(
            app._invoke_fabric_mcp(
                _request(), lambda: token, session_provider=lambda: session
            )
        )
    assert str(error.value) == "Invalid Fabric MCP access token."
    assert session.requests == []


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
        return {"status": 429, "headers": {"Retry-After": "7"}, "message": "response"}

    monkeypatch.setattr(app, "_invoke_fabric_mcp", fake_invoke)
    payload = _request()
    response = asyncio.run(app.rayfin_fabric_mcp_v1(payload, FabricItem()))
    assert response.media_type == "application/json"
    assert json.loads(b"".join(response.body)) == {
        "status": 429, "headers": {"Retry-After": "7"}, "message": "response"
    }
    assert captured == {"payload": payload, "token": "obo-token"}


def test_metadata_declares_only_payload_and_fabric_item():
    metadata = json.loads(Path(__file__).with_name("functions.metadata").read_text())
    function = next(
        item for item in metadata if item["name"] == "rayfin_fabric_mcp_v1"
    )
    assert function["fabricProperties"]["fabricFunctionParameters"] == [
        {"name": "payload", "dataType": "dict"}
    ]
    assert function["fabricProperties"]["fabricFunctionReturnType"] == "StreamResponse"
    assert function["bindings"][1] == {
        "name": "fabricIqClient",
        "direction": "In",
        "type": "FabricItem",
        "audienceType": "Fabric",
    }


@pytest.mark.parametrize("status", (200, 202, 204, 307, 400, 403, 429, 500))
@pytest.mark.parametrize("client_headers", (False, True))
@pytest.mark.parametrize("media_type", ("application/json", "text/event-stream"))
def test_real_http_forwarding_and_buffering(status, client_headers, media_type):
    """Exercise aiohttp on loopback; only the test adapter substitutes the URL."""
    app = _load_function_app()
    captured = []
    sent_headers = []
    chunks = [b'{"result":"caf\xc3', b'\xa9"}']
    if media_type == "text/event-stream":
        chunks = [b"data: " + chunks[0], chunks[1] + b"\n\ndata: opaque\n\n"]
    payload = _request(headers={
        "aUtHoRiZaTiOn": "caller-token",
        "Host": "attacker.example",
        "Connection": "X-Hop",
        "X-Hop": "original-forwarding",
        "X-Variants": "test-variant",
        **({
            "content-type": "application/json",
            "accept": "application/json, text/event-stream",
            "mcp-protocol-version": "client-version",
            "mcp-session-id": "client-session",
        } if client_headers else {}),
    })

    async def run():
        async def upstream(request):
            captured.append((request.method, request.path, request.headers, await request.read()))
            response = web.StreamResponse(status=status, headers={
                "Content-Type": media_type,
                "MCP-Session-Id": "upstream-session",
                "MCP-Protocol-Version": "upstream-version",
                "Retry-After": "5",
                "Set-Cookie": "secret-cookie",
                "Authorization": "secret-upstream-token",
                "Location": "/redirected",
                "X-Custom": "custom-metadata",
            })
            response.headers.add("Set-Cookie", "second-cookie")
            await response.prepare(request)
            sent_headers.append(dict(response.headers))
            if status != 204:
                for chunk in chunks:
                    await response.write(chunk)
                    await asyncio.sleep(0)
            await response.write_eof()
            return response

        server = web.Application()
        server.router.add_route("*", "/{path:.*}", upstream)
        runner = web.AppRunner(server, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        try:
            await site.start()
            port = runner.addresses[0][1]
            async with aiohttp.ClientSession() as client:
                class LoopbackSession:
                    def post(self, url, **kwargs):
                        assert url == "https://api.fabric.microsoft.com/v1/mcp/fabriciq"
                        return client.post(
                            f"http://127.0.0.1:{port}/v1/mcp/fabriciq", **kwargs
                        )

                output = await app._invoke_fabric_mcp(
                    payload, lambda: "obo-token", lambda: LoopbackSession()
                )
                assert not client.closed
                return output
        finally:
            await runner.cleanup()

    output = asyncio.run(run())
    assert output == {
        "status": status,
        "headers": sent_headers[0],
        "message": "" if status == 204 else b"".join(chunks).decode("utf-8"),
    }
    assert len(captured) == 1  # In particular, the 307 was not followed.
    assert output["headers"]["Set-Cookie"] == "secret-cookie"
    assert output["headers"]["Authorization"] == "secret-upstream-token"
    assert output["headers"]["Location"] == "/redirected"
    assert output["headers"]["X-Custom"] == "custom-metadata"
    if status != 204:
        assert output["headers"]["Transfer-Encoding"] == "chunked"
    method, path, headers, body = captured[0]
    assert method == "POST" and path == "/v1/mcp/fabriciq"
    assert json.loads(body) == payload["message"]
    assert headers.getall("Authorization") == ["Bearer obo-token"]
    assert headers["Host"] == "attacker.example"
    assert int(headers["Content-Length"]) == len(body)
    assert "Transfer-Encoding" not in headers
    assert headers["Connection"] == "X-Hop"
    assert headers["X-Hop"] == "original-forwarding"
    assert headers["X-Variants"] == "test-variant"
    if client_headers:
        for name in ("Content-Type", "Accept", "MCP-Protocol-Version", "MCP-Session-Id"):
            assert headers[name] == payload["headers"][name.lower()]
    else:
        assert headers["Content-Type"] == "application/json"
        assert headers["Accept"] == "application/json, text/event-stream"
        assert headers["MCP-Protocol-Version"] == payload["protocolVersion"]
        assert "MCP-Session-Id" not in headers


def test_cancellation_releases_response_without_success_or_retry():
    response = _Response(asyncio.CancelledError())
    session = _Session([response])
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(_load_function_app()._invoke_fabric_mcp(
            _request(), lambda: "obo-token", lambda: session
        ))
    assert response.released
    assert len(session.requests) == 1


def test_forwarder_waits_for_this_body_not_task_completion():
    app = _load_function_app()

    async def run():
        body_started = asyncio.Event()
        finish_body = asyncio.Event()
        response = _Response('{"resultType":"task","task":{"status":"working"}}', status=202)

        async def delayed_text(encoding=None):
            assert encoding == "utf-8"
            body_started.set()
            await finish_body.wait()
            return response.text_value

        response.text = delayed_text
        session = _Session([response])
        invocation = asyncio.create_task(app._invoke_fabric_mcp(
            _request(), lambda: "obo-token", lambda: session
        ))
        await asyncio.wait_for(body_started.wait(), 1)
        assert not invocation.done()
        finish_body.set()
        output = await asyncio.wait_for(invocation, 1)
        assert output == {
            "status": 202, "headers": {}, "message": response.text_value
        }
        assert len(session.requests) == 1
        assert response.released

    asyncio.run(run())


@pytest.mark.parametrize("failure", ("timeout", "disconnect", "invalid-utf8"))
def test_real_http_failure_never_returns_a_success_envelope(failure):
    app = _load_function_app()

    async def run():
        requests = []
        release_server = asyncio.Event()

        async def upstream(request):
            requests.append(request)
            if failure == "timeout":
                await release_server.wait()
                return web.Response()
            if failure == "disconnect":
                request.transport.close()
                return web.Response()
            return web.Response(body=b"\xff", content_type="application/json")

        server = web.Application()
        server.router.add_post("/v1/mcp/fabriciq", upstream)
        runner = web.AppRunner(server, access_log=None)
        await runner.setup()
        try:
            await web.TCPSite(runner, "127.0.0.1", 0).start()
            port = runner.addresses[0][1]
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=2, sock_read=0.1)
            ) as client:
                class LoopbackSession:
                    def post(self, url, **kwargs):
                        assert url == "https://api.fabric.microsoft.com/v1/mcp/fabriciq"
                        return client.post(
                            f"http://127.0.0.1:{port}/v1/mcp/fabriciq", **kwargs
                        )

                with pytest.raises(RuntimeError, match="^Fabric MCP upstream transport failed\\.$"):
                    await app._invoke_fabric_mcp(
                        _request(), lambda: "obo-token", lambda: LoopbackSession()
                    )
            assert len(requests) == 1
        finally:
            release_server.set()
            await runner.cleanup()

    asyncio.run(run())
