"""Focused FabricIQ relay tests adapted from merged PR #118."""

import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import fabric.functions as fn
import pytest
from multidict import CIMultiDict


def load_app():
    spec = importlib.util.spec_from_file_location(
        "fabriciq_test_app", Path(__file__).with_name("function_app.py")
    )
    app = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(app)
    return app


@pytest.fixture
def app(monkeypatch):
    # Bypass host invocation only; retain the real StreamResponse implementation.
    monkeypatch.setattr(fn, "UserDataFunctions", lambda: SimpleNamespace(
        generic_connection=lambda **kwargs: lambda function: function,
        streaming_function=lambda: lambda function: function,
    ))
    monkeypatch.delenv("POWERBI_API_BASE", raising=False)
    return load_app()


def payload():
    return {
        "operation": "executeQuery",
        "input": {
            "protocolVersion": "2026-07-28",
            "headers": {"authorization": "caller", "AUTHORIZATION": "other",
                        "hOsT": "wrong.example", "CONTENT-LENGTH": "999", "Connection": "close",
                        "Mcp-Name": "opaque-task", "X-Variants": "opaque-variant",
                        "X-Sdk-Extension": "unchanged"},
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
    headers = CIMultiDict([("Retry-After", "7"), ("retry-after", "8")])

    def __init__(self, content, status=202):
        self.content = content
        self.status = status
        self.released = False

    async def text(self, encoding):
        assert encoding == "utf-8"
        if isinstance(self.content, Exception):
            raise self.content
        return self.content

    def release(self):
        self.released = True


@pytest.mark.parametrize("content", ['{"result":{"opaque":true}}', "data: opaque\n\n"])
@pytest.mark.parametrize("status", [202, 302, 401, 500])
def test_public_relay_preserves_exchange_and_overwrites_auth(app, monkeypatch, content, status):
    request = payload()
    before = json.dumps(request)
    response = Response(content, status)
    captured = {}

    class Session:
        async def post(self, url, **kwargs):
            captured.update(url=url, **kwargs)
            return response

    invoke = app._invoke_fabric_mcp

    async def with_session(value, token_provider):
        assert value is request["input"]
        return await invoke(value, token_provider, lambda: Session())

    monkeypatch.setattr(app, "_invoke_fabric_mcp", with_session)
    result = asyncio.run(app.rayfin_fabriciq_mcp_v1(request, FabricItem()))
    assert isinstance(result, fn.StreamResponse)
    assert result.media_type == "application/json"
    assert result.status_code == 200
    assert json.loads(b"".join(result.content)) == {
        "status": status, "headers": {"Retry-After": "7"}, "message": content,
    }
    assert captured["url"] == "https://api.fabric.microsoft.com/v1/mcp/fabriciq"
    assert captured["allow_redirects"] is False
    assert captured["data"] == json.dumps(request["input"]["message"], separators=(",", ":")).encode()
    assert captured["headers"] == {
        "Authorization": "Bearer synthetic-obo", "Mcp-Name": "opaque-task",
        "X-Variants": "opaque-variant", "X-Sdk-Extension": "unchanged",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2026-07-28",
    }
    assert response.released
    assert json.dumps(request) == before


@pytest.mark.parametrize("supplied", [
    {}, payload()["input"], {"operation": "executeQuery"}, {"input": {}},
    {"operation": "wrong", "input": {}}, {"operation": "executeQuery", "input": []},
    {**payload(), "message": {}},
])
def test_invalid_wrapper_fails_before_credentials(app, supplied):
    with pytest.raises(app.FabricMcpRequestError, match="Invalid connector invocation wrapper"):
        asyncio.run(app.rayfin_fabriciq_mcp_v1(supplied, object()))


@pytest.mark.parametrize("host, expected", [
    ("dailyapi.powerbi.com", "dailyapi.fabric.microsoft.com"),
    ("api.powerbi.com", "api.fabric.microsoft.com"),
    ("dxtapi.powerbi.com", "dxtapi.fabric.microsoft.com"),
    ("powerbiapi.analysis-df.windows.net", "powerbiapi.analysis-df.windows.net"),
])
def test_supported_origins_map_to_fabric(app, monkeypatch, host, expected):
    monkeypatch.setattr(app, "_POWERBI_BASE", f"https://{host}/v1.0/myorg")
    captured = []

    class Session:
        async def post(self, url, **_kwargs):
            captured.append(url)
            return Response("opaque")

    asyncio.run(app._invoke_fabric_mcp(
        payload()["input"], lambda: "synthetic-obo", lambda: Session()
    ))
    assert captured == [f"https://{expected}/v1/mcp/fabriciq"]


@pytest.mark.parametrize("base", [
    "", "https://[invalid", "https://managed.example/v1.0/myorg",
    "https://api.attacker.example/v1.0/myorg", "https://unknown.powerbi.com",
    "https://api.powerbi.com.attacker.example", "http://api.powerbi.com",
    "https://user@api.powerbi.com", "https://api.powerbi.com:443",
    "https://api.powerbi.com\\@attacker.example",
    "https://api.powerbi.com/v1.0/myorg?host=attacker.example",
    "https://api.powerbi.com/v1.0/myorg#fragment",
    "https://api.powerbi.com/\npath", "https://msitapi.fabric.microsoft.com",
    "https://dailyapi.fabric.microsoft.com", "https://dxtapi.fabric.microsoft.com",
    "https://api.fabric.microsoft.com",
])
def test_unsupported_origins_fail_before_token_or_network(app, monkeypatch, base):
    monkeypatch.setattr(app, "_POWERBI_BASE", base)

    def unexpected():
        pytest.fail("Unsupported origin must fail before token/session acquisition")

    with pytest.raises(app.FabricMcpRequestError, match="supported Power BI HTTPS origin"):
        asyncio.run(app._invoke_fabric_mcp(payload()["input"], unexpected, unexpected))


@pytest.mark.parametrize("error", [
    UnicodeError("synthetic"), aiohttp.ClientError("synthetic"), asyncio.TimeoutError(),
])
def test_response_is_released_on_read_failure(app, error):
    response = Response(error)

    class Session:
        async def post(self, *_args, **_kwargs):
            return response

    with pytest.raises(RuntimeError, match="Fabric MCP upstream transport failed"):
        asyncio.run(app._invoke_fabric_mcp(
            payload()["input"], lambda: "synthetic-obo", lambda: Session()
        ))
    assert response.released


@pytest.mark.parametrize("token", [None, "", 42, "invalid\rvalue", "invalid\nvalue"])
def test_invalid_token_fails_before_network(app, token):
    def unexpected():
        pytest.fail("Invalid token must fail before session acquisition")

    with pytest.raises(app.FabricMcpRequestError, match="Invalid Fabric MCP access token"):
        asyncio.run(app._invoke_fabric_mcp(payload()["input"], lambda: token, unexpected))


def test_sdk_headers_and_async_providers_are_preserved(app):
    request = payload()["input"]
    request["headers"] = {"accept": "text/event-stream", "content-type": "application/json",
                          "mcp-protocol-version": "opaque-version"}

    class Session:
        async def post(self, _url, **kwargs):
            assert kwargs["headers"] == {
                **request["headers"], "Authorization": "Bearer synthetic-obo",
            }
            return Response("")

    async def token():
        return "synthetic-obo"

    async def session():
        return Session()

    assert asyncio.run(app._invoke_fabric_mcp(request, token, session))["message"] == ""


def test_metadata_matches_real_runtime_binding():
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
    functions = {f.get_function_name(): f for f in load_app().udf.get_functions()}
    assert set(functions) == {e["name"] for e in entries}
    bindings = functions[entry["name"]].get_dict_repr()["bindings"]
    assert next(b for b in bindings if b["type"] == "httpTrigger")["route"] == entry["name"]
    binding = next(b for b in bindings if b["type"] == "FabricItem")
    assert binding["name"] == "fabricIqClient"
    assert binding["audienceType"] == "Fabric"
