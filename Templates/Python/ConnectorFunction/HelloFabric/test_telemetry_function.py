"""Focused tests for the telemetry connector and its deployment packages."""

import asyncio
import importlib.util
import json
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import fabric.functions as fn
import pytest


ROOT = Path(__file__).resolve().parent
RESOURCE_ID = (
    "/subscriptions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    "/resourceGroups/telemetry-rg/providers/Microsoft.Insights/components/app-insights"
)


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setattr(fn, "UserDataFunctions", lambda: SimpleNamespace(
        generic_connection=lambda **kwargs: lambda function: function,
        streaming_function=lambda: lambda function: function,
    ))
    spec = importlib.util.spec_from_file_location("telemetry_test_app", ROOT / "function_app.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def payload(**overrides):
    return {"operation": "query", "input": {
        "kql": "AppTraces | project TimeGenerated, Message",
        "resourceId": RESOURCE_ID,
        "timespan": "P1D",
        "maxRows": 1000,
        **overrides,
    }}


class Response:
    def __init__(self, chunks=(), status=200, document=None):
        self.chunks = chunks
        self.status = status
        self.headers = {"Content-Type": "application/json; charset=utf-8"}
        self.json = AsyncMock(return_value=document)
        self.read = AsyncMock(return_value=b"private identity error")
        self.text = AsyncMock(return_value='{"error":"upstream failure"}')
        self.released = False
        self.content = self

    async def iter_any(self):
        for chunk in self.chunks:
            yield chunk

    def release(self):
        self.released = True


@pytest.fixture
def network(app, monkeypatch):
    monkeypatch.setenv("IDENTITY_ENDPOINT", "http://127.0.0.1:41741/MSI/token")
    monkeypatch.setenv("IDENTITY_HEADER", "synthetic-identity-header")
    identity = Response(document={"access_token": "synthetic-token", "expires_on": "4102444800"})
    query = Response([b'{"tables":[', b'{"name":"PrimaryResult"}', b"]}"])
    session = SimpleNamespace(
        get=AsyncMock(return_value=identity),
        post=AsyncMock(return_value=query),
    )
    monkeypatch.setattr(app, "_get_session", AsyncMock(return_value=session))
    return SimpleNamespace(session=session, identity=identity, query=query)


def test_resource_scoped_managed_identity_stream(app, network):
    async def invoke():
        result = await app.rayfin_telemetry_v1(payload(workspaceId="ignored-legacy-workspace"))
        assert isinstance(result, fn.StreamResponse)
        assert result.status_code == 200
        assert result.media_type == "application/json"
        assert [chunk async for chunk in result.content] == network.query.chunks

    asyncio.run(invoke())
    network.session.get.assert_awaited_once_with(
        "http://127.0.0.1:41741/MSI/token",
        params={"resource": "https://api.loganalytics.io", "api-version": "2019-08-01"},
        headers={"X-IDENTITY-HEADER": "synthetic-identity-header"},
    )
    network.session.post.assert_awaited_once_with(
        f"https://api.loganalytics.azure.com/v1{RESOURCE_ID}/query",
        json={"query": "AppTraces | project TimeGenerated, Message\n| take 1000", "timespan": "P1D"},
        headers={"Authorization": "Bearer synthetic-token", "Content-Type": "application/json",
                 "Accept": "application/json"},
    )
    assert network.identity.released and network.query.released
    network.query.text.assert_not_awaited()


@pytest.mark.parametrize("timespan", [None, "PT1S", "PT24H", "P30D"])
def test_query_bounds_and_optional_timespan(app, network, timespan):
    request = payload(kql="AppRequests | take 1; \r\n", timespan=timespan, maxRows=1)
    if timespan is None:
        del request["input"]["timespan"]
        del request["input"]["maxRows"]
    asyncio.run(app.rayfin_telemetry_v1(request))
    expected = {"query": f"AppRequests | take 1\n| take {10000 if timespan is None else 1}"}
    if timespan:
        expected["timespan"] = timespan
    assert network.session.post.call_args.kwargs["json"] == expected


@pytest.mark.parametrize("overrides", [
    {"kql": ""}, {"kql": "  "}, {"kql": None}, {"kql": "x" * 8193},
    *({"timespan": value} for value in ("P31D", "P30DT1S", "P0D", "PT", "P1M", 1)),
    *({"maxRows": value} for value in (True, 0, -1, 1.5, "100", 10001)),
    *({"resourceId": value} for value in (
        None, 123, {}, "", "not-an-arm-id",
        RESOURCE_ID.replace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "not-a-guid"),
        RESOURCE_ID.replace("Microsoft.Insights/components", "Microsoft.OperationalInsights/workspaces"),
        RESOURCE_ID.replace("Microsoft.Insights/components", "Microsoft.Insights/webtests"),
        RESOURCE_ID.replace("/telemetry-rg/", "/../"),
        RESOURCE_ID.replace("/telemetry-rg/", "/./"),
        RESOURCE_ID.replace("/telemetry-rg/", "/bad rg/"),
        RESOURCE_ID.replace("/telemetry-rg/", "/bad./"),
        RESOURCE_ID.rsplit("/", 1)[0] + "/..",
        RESOURCE_ID.rsplit("/", 1)[0] + "/.",
        *(RESOURCE_ID + suffix for suffix in (
            "/", "/child", "/../other", "?query=other", "#fragment",
            "%2f..%2fother", "\\other", " space", "\n", "\r", "\t", "\x00",
        )),
        " " + RESOURCE_ID, RESOURCE_ID.lstrip("/"), "/" + RESOURCE_ID,
        "https://api.loganalytics.azure.com/v1" + RESOURCE_ID,
        RESOURCE_ID.replace("Insights", "Inſights"),
    )),
])
def test_invalid_input_fails_before_network(app, network, overrides):
    with pytest.raises(ValueError):
        asyncio.run(app.rayfin_telemetry_v1(payload(**overrides)))
    app._get_session.assert_not_awaited()


@pytest.mark.parametrize("supplied", [
    None, [], {}, {"operation": "emit", "input": {}}, {"operation": "query"},
    {"operation": "query", "input": []},
    {"operation": "query", "input": {"kql": "AppTraces", "workspaceId": "legacy"}},
])
def test_invalid_wrapper_fails_before_network(app, network, supplied):
    with pytest.raises(ValueError):
        asyncio.run(app.rayfin_telemetry_v1(supplied))
    app._get_session.assert_not_awaited()


def test_query_length_boundary_and_resource_id_casing(app, network):
    resource_id = RESOURCE_ID.upper().replace("APP-INSIGHTS", "App_Insights.v1(blue)")
    asyncio.run(app.rayfin_telemetry_v1(payload(kql="x" * 8192, resourceId=resource_id)))
    assert network.session.post.call_args.args[0] == (
        f"https://api.loganalytics.azure.com/v1{resource_id}/query"
    )


@pytest.mark.parametrize("status", [400, 401, 403, 429, 500])
def test_upstream_errors_are_forwarded(app, network, status):
    network.query.status = status
    network.query.text.return_value = json.dumps({"error": "échec"})
    result = asyncio.run(app.rayfin_telemetry_v1(payload()))
    assert result.status_code == status
    assert result.media_type == network.query.headers["Content-Type"]
    assert b"".join(result.content) == network.query.text.return_value.encode("utf-8")
    assert network.query.released


def test_response_released_on_stream_close(app, network):
    async def invoke():
        result = await app.rayfin_telemetry_v1(payload())
        assert not network.query.released
        assert await anext(result.content) == network.query.chunks[0]
        assert not network.query.released
        await result.content.aclose()
        assert network.query.released

    asyncio.run(invoke())


def test_token_cache_concurrency_and_refresh(app, network, monkeypatch):
    clock = SimpleNamespace(now=100)
    monkeypatch.setattr(app.time, "time", lambda: clock.now)
    network.identity.json.return_value["expires_on"] = "1000"

    async def get_identity(*_args, **_kwargs):
        await asyncio.sleep(0)
        return network.identity

    network.session.get.side_effect = get_identity

    async def invoke():
        tokens = await asyncio.gather(*(app._get_telemetry_access_token() for _ in range(5)))
        assert tokens == ["synthetic-token"] * 5
        assert network.session.get.await_count == 1
        clock.now = 939
        assert await app._get_telemetry_access_token() == "synthetic-token"
        assert network.session.get.await_count == 1
        clock.now = 940
        network.identity.json.return_value = {"access_token": "refreshed", "expires_on": "2000"}
        assert await app._get_telemetry_access_token() == "refreshed"
        assert network.session.get.await_count == 2

    asyncio.run(invoke())


@pytest.mark.parametrize("missing", [("IDENTITY_ENDPOINT",), ("IDENTITY_HEADER",),
                                     ("IDENTITY_ENDPOINT", "IDENTITY_HEADER")])
def test_missing_identity_configuration(app, network, monkeypatch, missing):
    for name in missing:
        monkeypatch.delenv(name)
    with pytest.raises(RuntimeError, match="Managed identity is unavailable"):
        asyncio.run(app.rayfin_telemetry_v1(payload()))
    app._get_session.assert_not_awaited()


@pytest.mark.parametrize("status, document, error", [
    (403, None, "token acquisition failed"),
    (500, None, "token acquisition failed"),
    (200, [], "invalid token"),
    (200, {}, "invalid token"),
    (200, {"access_token": "", "expires_on": "1000"}, "invalid token"),
    (200, {"access_token": "token", "expires_on": "invalid"}, "invalid token"),
    (200, {"access_token": "token", "expires_on": None}, "invalid token"),
    (200, {"access_token": "token", "expires_on": "100"}, "invalid token"),
])
def test_identity_failures_release_response(app, network, monkeypatch, status, document, error):
    monkeypatch.setattr(app.time, "time", lambda: 100)
    network.identity.status = status
    network.identity.json.return_value = document
    with pytest.raises(RuntimeError, match=error):
        asyncio.run(app.rayfin_telemetry_v1(payload()))
    assert network.identity.released
    network.session.post.assert_not_awaited()
    assert app._telemetry_token is None
    assert app._telemetry_token_expires_at == 0


def test_metadata_and_packages():
    source = (ROOT / "function_app.py").read_bytes().replace(b"\r\n", b"\n")
    metadata = (ROOT / "functions.metadata").read_bytes().replace(b"\r\n", b"\n")
    entries = json.loads(metadata)
    assert len({entry["name"] for entry in entries}) == len(entries)
    entry = next(entry for entry in entries if entry["name"] == "rayfin_telemetry_v1")
    assert entry == {
        "name": "rayfin_telemetry_v1", "scriptFile": "function_app.py",
        "bindings": [{"name": "req", "direction": "In", "type": "httpTrigger",
                      "methods": ["post"], "route": "rayfin_telemetry_v1", "authLevel": "Anonymous"}],
        "fabricProperties": {
            "fabricFunctionReturnType": "StreamResponse",
            "fabricFunctionParameters": [{"name": "payload", "dataType": "dict"}],
        },
    }
    for name in ("SourceCode.zip", "Deploy.zip"):
        with zipfile.ZipFile(ROOT / name) as archive:
            assert archive.testzip() is None
            assert archive.read("function_app.py") == source
            assert archive.read("fabric_lib/functions.metadata") == metadata
            assert not any("test_" in member or "__pycache__" in member
                           for member in archive.namelist())
            for entry in archive.infolist():
                mode = (entry.external_attr >> 16) & 0xFFFF
                assert mode == 0 or mode & 0o044 == 0o044, (name, entry.filename, oct(mode))
                assert not {".github", ".git"}.intersection(entry.filename.split("/"))
