import asyncio
import json
from pathlib import Path

import pytest

from test_function_app import _load_function_app


class _Response:
    def __init__(self, text, status=200):
        self.status = status
        self.text_value = text
        self.text_count = 0

    async def text(self, encoding=None):
        assert encoding == "utf-8"
        self.text_count += 1
        return self.text_value


class _Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    async def post(self, url, **kwargs):
        self.requests.append({"url": url, **kwargs})
        return next(self.responses)


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
    assert app._FABRIC_MCP_ENDPOINT == (
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


def test_fixed_url_opaque_body_headers_and_final_authorization_overwrite():
    app = _load_function_app()
    message = {
        "arbitrary": [1, {"nested": True}],
        "endpoint": "https://attacker.example",
    }
    caller_headers = {
        "authorization": "Bearer caller-token",
        "Host": "alternate.example",
        "X-Rewrite-URL": "/alternate",
        "X-Real-IP": "192.0.2.1",
        "X-HTTP-Method-Override": "DELETE",
        "X-MS-CLIENT-PRINCIPAL": "opaque-identity",
        "X-Variants": "Fabric.Routing.M365.V1,Fabric.DisableMsitRedirect",
    }
    output, session = _invoke(
        app,
        _request(message=message, headers=caller_headers),
        [_Response("opaque upstream response")],
    )

    assert output == {"message": "opaque upstream response"}
    assert len(session.requests) == 1
    request = session.requests[0]
    assert request["url"] == app._FABRIC_MCP_ENDPOINT
    assert request["allow_redirects"] is False
    assert "timeout" not in request
    assert json.loads(request["data"]) == message
    assert request["headers"]["Authorization"] == "Bearer obo-token"
    assert [
        name for name in request["headers"] if name.lower() == "authorization"
    ] == ["Authorization"]
    for name, value in caller_headers.items():
        if name.lower() != "authorization":
            assert request["headers"][name] == value


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
    assert defaults["Content-Type"] == "application/json"
    assert defaults["Accept"] == "application/json, text/event-stream"
    assert defaults["MCP-Protocol-Version"] == "2026-07-28"


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
        "data: opaque\n\n",
    ),
)
def test_upstream_text_is_returned_without_json_or_sse_parsing(response):
    app = _load_function_app()
    output, _ = _invoke(app, _request(), [_Response(response)])
    assert output == {"message": response}


def test_large_request_has_no_relay_owned_size_limit():
    app = _load_function_app()
    message = {"large": "x" * (5 * 1024 * 1024 + 1)}
    output, session = _invoke(app, _request(message=message), [_Response("accepted")])
    assert output == {"message": "accepted"}
    assert len(session.requests[0]["data"]) > 5 * 1024 * 1024


def test_endpoint_override_is_rejected_before_network(monkeypatch):
    app = _load_function_app()
    monkeypatch.setenv("FABRIC_MCP_ENDPOINT", "https://attacker.example")
    session = _Session(())
    with pytest.raises(app.FabricMcpRequestError) as error:
        asyncio.run(
            app._invoke_fabric_mcp(
                _request(), lambda: "obo-token", session_provider=lambda: session
            )
        )
    assert str(error.value) == "Invalid Fabric MCP endpoint configuration."
    assert session.requests == []


def test_non_2xx_reports_only_status():
    app = _load_function_app()
    response = _Response("private upstream detail", status=500)
    with pytest.raises(RuntimeError) as error:
        _invoke(app, _request(), [response])
    assert str(error.value) == "Fabric MCP upstream returned HTTP 500."
    assert response.text_count == 1


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
        return {"message": "response"}

    monkeypatch.setattr(app, "_invoke_fabric_mcp", fake_invoke)
    payload = _request()
    assert asyncio.run(app.rayfin_fabric_mcp_v1(payload, FabricItem())) == {
        "message": "response"
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
    assert function["bindings"][1] == {
        "name": "fabricIqClient",
        "direction": "In",
        "type": "FabricItem",
        "audienceType": "Fabric",
    }
