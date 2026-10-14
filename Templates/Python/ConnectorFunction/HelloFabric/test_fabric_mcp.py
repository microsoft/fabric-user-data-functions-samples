"""Focused FabricIQ relay tests using the shared connector's Fabric stub."""

import asyncio
import json
from pathlib import Path

import pytest
from multidict import CIMultiDict

from test_function_app import _load_function_app


def payload():
    return {
        "operation": "executeQuery",
        "input": {
            "protocolVersion": "2026-07-28",
            "headers": {"authorization": "caller", "AUTHORIZATION": "other",
                        "Mcp-Name": "opaque-task", "X-Variants": "opaque-variant"},
            "message": {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                        "params": {"opaque": True}},
        },
    }


class FabricItem:
    token = "synthetic-obo"

    def get_access_token(self):
        return self

    def get_token(self):
        return self


class Response:
    status = 202
    headers = CIMultiDict([("Retry-After", "7"), ("retry-after", "8")])

    def __init__(self, content):
        self.content = content
        self.released = False

    async def text(self, encoding):
        assert encoding == "utf-8"
        if isinstance(self.content, Exception):
            raise self.content
        return self.content

    def release(self):
        self.released = True


@pytest.mark.parametrize("content", ['{"result":{"opaque":true}}', "data: opaque\n\n"])
def test_public_relay_preserves_exchange_and_overwrites_auth(monkeypatch, content):
    app = _load_function_app()
    request = payload()
    before = json.dumps(request)
    response = Response(content)
    captured = {}

    class Session:
        async def post(self, url, **kwargs):
            captured.update(url=url, **kwargs)
            return response

    invoke = app._invoke_fabric_mcp

    async def with_session(value, token_provider):
        assert value is request["input"]
        return await invoke(value, token_provider, lambda: Session())

    monkeypatch.setattr(app, "_POWERBI_BASE", "https://dailyapi.powerbi.com/v1.0/myorg")
    monkeypatch.setattr(app, "_invoke_fabric_mcp", with_session)
    result = asyncio.run(app.rayfin_fabriciq_mcp_v1(request, FabricItem()))
    assert result.media_type == "application/json"
    assert json.loads(b"".join(result.body)) == {
        "status": 202, "headers": {"Retry-After": "7"}, "message": content,
    }
    assert captured["url"] == "https://dailyapi.fabric.microsoft.com/v1/mcp/fabriciq"
    assert captured["allow_redirects"] is False
    assert captured["data"] == json.dumps(request["input"]["message"], separators=(",", ":")).encode()
    assert captured["headers"] == {
        "Authorization": "Bearer synthetic-obo", "Mcp-Name": "opaque-task",
        "X-Variants": "opaque-variant", "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2026-07-28",
    }
    assert response.released
    assert json.dumps(request) == before


@pytest.mark.parametrize("supplied", [
    {}, payload()["input"], {"operation": "executeQuery"}, {"input": {}},
    {"operation": "wrong", "input": {}}, {"operation": "executeQuery", "input": []},
    {**payload(), "message": {}},
])
def test_invalid_wrapper_fails_before_credentials(supplied):
    app = _load_function_app()
    with pytest.raises(app.FabricMcpRequestError, match="Invalid connector invocation wrapper"):
        asyncio.run(app.rayfin_fabriciq_mcp_v1(supplied, object()))


def test_response_is_released_on_decode_failure():
    app = _load_function_app()
    response = Response(UnicodeError("synthetic"))

    class Session:
        async def post(self, *_args, **_kwargs):
            return response

    with pytest.raises(RuntimeError, match="Fabric MCP upstream transport failed"):
        asyncio.run(app._invoke_fabric_mcp(
            payload()["input"], lambda: "synthetic-obo", lambda: Session()
        ))
    assert response.released


def test_metadata_matches_public_function():
    entries = json.loads(Path(__file__).with_name("functions.metadata").read_text())
    entry = next(e for e in entries if e["name"] == "rayfin_fabriciq_mcp_v1")
    assert entry["bindings"][0]["route"] == entry["name"]
    assert entry["bindings"][1] == {
        "name": "fabricIqClient", "direction": "In", "type": "FabricItem", "audienceType": "Fabric",
    }
    assert entry["fabricProperties"] == {
        "fabricFunctionReturnType": "StreamResponse",
        "fabricFunctionParameters": [{"name": "payload", "dataType": "dict"}],
    }
