"""Standalone tests for the rayfin_telemetry_v1 streaming function."""

import asyncio
import importlib
import json
import os
import sys
import types
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, patch


_RESOURCE_ID = (
    "/subscriptions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    "/resourceGroups/telemetry-rg/providers/Microsoft.Insights/components/app-insights"
)
_IDENTITY_ENVIRONMENT = {
    "IDENTITY_ENDPOINT": "http://127.0.0.1:41741/MSI/token",
    "IDENTITY_HEADER": "rotating-identity-header",
}


def _install_fabric_stub():
    if "fabric.functions" in sys.modules:
        return

    fabric = types.ModuleType("fabric")
    functions = types.ModuleType("fabric.functions")

    class UserDataFunctions:
        def generic_connection(self, *_args, **_kwargs):
            return lambda function: function

        def streaming_function(self, *_args, **_kwargs):
            return lambda function: function

    class StreamResponse:
        def __init__(self, body, media_type=None, status_code=None):
            self.body = body
            self.media_type = media_type
            self.status_code = status_code

    class FabricItem:
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


class _CompletedAwaitable:
    def __await__(self):
        if False:
            yield None
        return None


class _FakeResponse:
    def __init__(self, chunks, status=200, headers=None, json_body=None):
        self.status = status
        self.headers = headers or {}
        self.content = _FakeContent(chunks)
        self._json_body = json_body
        self.text_called = False
        self.released = False

    async def text(self):
        self.text_called = True
        return b"".join(self.content._chunks).decode("utf-8")

    async def read(self):
        return b"".join(self.content._chunks)

    async def json(self):
        return self._json_body

    def release(self):
        self.released = True
        return _CompletedAwaitable()


class _FakeSession:
    def __init__(self, response, token_response):
        self._response = response
        self._token_response = token_response
        self.captured_headers = None
        self.captured_url = None
        self.captured_json = None
        self.captured_get_headers = None
        self.captured_get_params = None
        self.captured_get_url = None
        self.get_count = 0
        self.post_count = 0

    async def post(self, url, json=None, headers=None):
        self.post_count += 1
        self.captured_url = url
        self.captured_headers = headers
        self.captured_json = json
        return self._response

    async def get(self, url, params=None, headers=None):
        self.get_count += 1
        self.captured_get_url = url
        self.captured_get_headers = headers
        self.captured_get_params = params
        await asyncio.sleep(0)
        return self._token_response


def _payload(**overrides):
    input_data = {
        "kql": "AppTraces | project TimeGenerated, Message",
        "resourceId": _RESOURCE_ID,
        "timespan": "P1D",
        "maxRows": 1000,
    }
    input_data.update(overrides)
    return {"operation": "query", "input": input_data}


async def _invoke(chunks, payload=None, status=200, error_body="", consume=True):
    function_app = _load_function_app()
    function_app._telemetry_token = None
    function_app._telemetry_token_expires_at = 0
    query_response = _FakeResponse(
        chunks if status == 200 else [error_body.encode("utf-8")],
        status=status,
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    token_response = _FakeResponse(
        [],
        json_body={"access_token": "fake-monitor-token", "expires_on": "4102444800"},
    )
    session = _FakeSession(query_response, token_response)

    async def _fake_get_session():
        return session

    with (
        patch.object(function_app, "_get_session", _fake_get_session),
        patch.dict(os.environ, _IDENTITY_ENVIRONMENT),
    ):
        result = await function_app.rayfin_telemetry_v1(
            _payload() if payload is None else payload
        )

    body = []
    if not consume:
        return function_app, result, body, query_response, token_response, session
    if hasattr(result.body, "__aiter__"):
        async for chunk in result.body:
            body.append(chunk)
    else:
        body.extend(result.body)
    return function_app, result, body, query_response, token_response, session


async def _check_uses_managed_identity_and_resource_scope():
    chunks = [b'{"tables":[', b'{"name":"PrimaryResult"}', b"]}"]
    function_app, result, body, response, token_response, session = await _invoke(
        chunks,
        _payload(workspaceId="ignored-legacy-workspace"),
    )

    assert body == chunks
    assert response.text_called is False
    assert result.media_type == function_app._JSON_MEDIA_TYPE
    assert session.captured_url == (
        f"https://api.loganalytics.azure.com/v1{_RESOURCE_ID}/query"
    )
    assert session.captured_json == {
        "query": "AppTraces | project TimeGenerated, Message\n| take 1000",
        "timespan": "P1D",
    }
    assert session.captured_headers["Authorization"] == "Bearer fake-monitor-token"
    assert session.captured_get_params == {
        "resource": "https://api.loganalytics.io",
        "api-version": "2019-08-01",
    }
    assert session.captured_get_headers == {
        "X-IDENTITY-HEADER": "rotating-identity-header"
    }
    assert token_response.released is True
    assert response.released is True
    assert session.captured_get_url == _IDENTITY_ENVIRONMENT["IDENTITY_ENDPOINT"]
    assert session.get_count == session.post_count == 1


async def _check_enforces_timespan_and_row_limit():
    _function_app, _result, _body, _response, _token_response, session = (
        await _invoke([b'{"tables":[]}'])
    )
    assert session.captured_json == {
        "query": "AppTraces | project TimeGenerated, Message\n| take 1000",
        "timespan": "P1D",
    }
    payload = _payload(kql="AppRequests | take 1")
    del payload["input"]["timespan"]
    del payload["input"]["maxRows"]
    *_, session = await _invoke([b'{"tables":[]}'], payload)
    assert session.captured_json == {"query": "AppRequests | take 1\n| take 10000"}
    for timespan in ("PT1S", "PT24H", "P30D"):
        *_, session = await _invoke([], _payload(timespan=timespan, maxRows=1))
        assert session.captured_json["timespan"] == timespan
        assert session.captured_json["query"].endswith("| take 1")
    mixed_case = _RESOURCE_ID.upper().replace("APP-INSIGHTS", "App_Insights.v1(blue)")
    *_, session = await _invoke([], _payload(resourceId=mixed_case))
    assert session.captured_url == (
        f"https://api.loganalytics.azure.com/v1{mixed_case}/query"
    )


async def _check_removes_trailing_query_semicolon():
    _function_app, _result, _body, _response, _token_response, session = (
        await _invoke(
            [b'{"tables":[]}'],
            _payload(kql="AppTraces | project TimeGenerated, Message;  \r\n"),
        )
    )
    assert session.captured_json["query"] == (
        "AppTraces | project TimeGenerated, Message\n| take 1000"
    )


async def _check_forwards_upstream_error():
    for status in (400, 401, 403, 429, 500):
        error_body = json.dumps({"error": {"code": str(status), "message": "échec"}})
        _function_app, result, body, response, _token_response, _session = (
            await _invoke([], status=status, error_body=error_body)
        )
        assert result.status_code == status
        assert result.media_type == "application/json; charset=utf-8"
        assert b"".join(body) == error_body.encode("utf-8")
        assert response.text_called is True
        assert response.released is True


async def _check_stream_releases_on_close():
    chunks = [b'{"tables":[', b'{"name":"PrimaryResult"}', b"]}"]
    _, result, _, response, _, _ = await _invoke(chunks, consume=False)
    assert not response.released
    assert await anext(result.body) == chunks[0]
    assert not response.released
    await result.body.aclose()
    assert response.released
    assert not response.text_called


async def _check_rejects_invalid_input():
    function_app = _load_function_app()
    invalid_payloads = [
        None,
        [],
        {},
        {"operation": "emit", "input": {}},
        {"operation": "query"},
        {"operation": "query", "input": []},
        _payload(timespan="P31D"),
        _payload(timespan="P30DT1S"),
        _payload(timespan="P0D"),
        _payload(timespan="PT"),
        _payload(timespan="P1M"),
        _payload(timespan=1),
        _payload(maxRows=True),
        _payload(maxRows=0),
        _payload(maxRows=-1),
        _payload(maxRows=1.5),
        _payload(maxRows="100"),
        _payload(maxRows=10001),
        _payload(kql=""),
        _payload(kql="  "),
        _payload(kql=None),
        _payload(kql="x" * (8 * 1024 + 1)),
    ]
    invalid_ids = [
        None, 123, {}, "", "not-an-arm-id",
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        _RESOURCE_ID.replace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "not-a-guid"),
        _RESOURCE_ID.replace("Microsoft.Insights/components", "Microsoft.OperationalInsights/workspaces"),
        _RESOURCE_ID.replace("Microsoft.Insights/components", "Microsoft.Insights/webtests"),
        _RESOURCE_ID.replace("Microsoft.Insights", "Other.Insights"),
        _RESOURCE_ID.replace("/telemetry-rg/", "//"),
        _RESOURCE_ID.replace("/telemetry-rg/", "/../"),
        _RESOURCE_ID.replace("/telemetry-rg/", "/./"),
        _RESOURCE_ID.replace("/telemetry-rg/", "/bad rg/"),
        _RESOURCE_ID.replace("/telemetry-rg/", "/bad./"),
        _RESOURCE_ID.rsplit("/", 1)[0] + "/..",
        _RESOURCE_ID.rsplit("/", 1)[0] + "/.",
        _RESOURCE_ID.rsplit("/", 1)[0] + "/",
        _RESOURCE_ID + "/", _RESOURCE_ID + "/child",
        _RESOURCE_ID + "/../other", _RESOURCE_ID + "/./query",
        _RESOURCE_ID + "?query=other", _RESOURCE_ID + "#fragment",
        _RESOURCE_ID + "%2f..%2fother", _RESOURCE_ID + "%252fother",
        _RESOURCE_ID + "\\other", _RESOURCE_ID + " space",
        _RESOURCE_ID + "\n", _RESOURCE_ID + "\r", _RESOURCE_ID + "\t",
        _RESOURCE_ID + "\x00", " " + _RESOURCE_ID,
        _RESOURCE_ID.lstrip("/"), "/" + _RESOURCE_ID,
        "https://api.loganalytics.azure.com/v1" + _RESOURCE_ID,
        "//evil.example" + _RESOURCE_ID,
        _RESOURCE_ID.replace("Insights", "Inſights"),
    ]
    invalid_payloads.extend(_payload(resourceId=value) for value in invalid_ids)
    legacy = _payload(workspaceId="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    del legacy["input"]["resourceId"]
    invalid_payloads.append(legacy)
    token = AsyncMock(side_effect=AssertionError("invalid input requested a token"))
    session = AsyncMock(side_effect=AssertionError("invalid input requested HTTP"))
    with (
        patch.object(function_app, "_get_telemetry_access_token", token),
        patch.object(function_app, "_get_session", session),
    ):
        for payload in invalid_payloads:
            try:
                await function_app.rayfin_telemetry_v1(payload)
            except ValueError:
                continue
            raise AssertionError(f"invalid payload was accepted: {payload}")
    token.assert_not_awaited()
    session.assert_not_awaited()


async def _check_token_cache_and_errors():
    function_app = _load_function_app()

    def reset():
        function_app._telemetry_token = None
        function_app._telemetry_token_expires_at = 0
        function_app._telemetry_token_lock = asyncio.Lock()

    reset()
    response = _FakeResponse([], json_body={"access_token": "cached-token", "expires_on": "1000"})
    session = _FakeSession(_FakeResponse([]), response)
    with (
        patch.object(function_app, "_get_session", AsyncMock(return_value=session)),
        patch.dict(os.environ, _IDENTITY_ENVIRONMENT, clear=True),
        patch.object(function_app.time, "time", return_value=100) as clock,
    ):
        tokens = await asyncio.gather(*(
            function_app._get_telemetry_access_token() for _ in range(5)
        ))
        assert tokens == ["cached-token"] * 5
        assert session.get_count == 1
        clock.return_value = 939
        assert await function_app._get_telemetry_access_token() == "cached-token"
        assert session.get_count == 1
        clock.return_value = 940
        response._json_body = {"access_token": "refreshed-token", "expires_on": "2000"}
        assert await function_app._get_telemetry_access_token() == "refreshed-token"
        assert session.get_count == 2
        assert response.released

    for environment in ({}, {"IDENTITY_ENDPOINT": "http://identity"}, {"IDENTITY_HEADER": "header"}):
        reset()
        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(function_app, "_get_session", AsyncMock()) as get_session,
        ):
            try:
                await function_app._get_telemetry_access_token()
            except RuntimeError as exc:
                assert str(exc) == "Managed identity is unavailable"
            else:
                raise AssertionError("missing managed identity was accepted")
            get_session.assert_not_awaited()

    for status, document, error in [
        (403, None, "Managed identity token acquisition failed"),
        (500, None, "Managed identity token acquisition failed"),
        (200, [], "Managed identity returned an invalid token"),
        (200, {}, "Managed identity returned an invalid token"),
        (200, {"access_token": "", "expires_on": "1000"}, "Managed identity returned an invalid token"),
        (200, {"access_token": "token", "expires_on": "invalid"}, "Managed identity returned an invalid token"),
        (200, {"access_token": "token", "expires_on": None}, "Managed identity returned an invalid token"),
        (200, {"access_token": "token", "expires_on": "100"}, "Managed identity returned an invalid token"),
    ]:
        reset()
        response = _FakeResponse([b"private identity error"], status=status, json_body=document)
        session = _FakeSession(_FakeResponse([]), response)
        with (
            patch.object(function_app, "_get_session", AsyncMock(return_value=session)),
            patch.dict(os.environ, _IDENTITY_ENVIRONMENT, clear=True),
            patch.object(function_app.time, "time", return_value=100),
        ):
            try:
                await function_app.rayfin_telemetry_v1(_payload())
            except RuntimeError as exc:
                assert str(exc) == error
            else:
                raise AssertionError("invalid managed identity response was accepted")
        assert response.released
        assert session.post_count == 0
        assert function_app._telemetry_token is None
        assert function_app._telemetry_token_expires_at == 0


def test_uses_managed_identity_and_resource_scope():
    asyncio.run(_check_uses_managed_identity_and_resource_scope())


def test_enforces_timespan_and_row_limit():
    asyncio.run(_check_enforces_timespan_and_row_limit())


def test_removes_trailing_query_semicolon():
    asyncio.run(_check_removes_trailing_query_semicolon())


def test_forwards_upstream_error():
    asyncio.run(_check_forwards_upstream_error())


def test_rejects_invalid_input():
    asyncio.run(_check_rejects_invalid_input())


def test_stream_releases_on_close():
    asyncio.run(_check_stream_releases_on_close())


def test_token_cache_and_errors():
    asyncio.run(_check_token_cache_and_errors())


def test_packed_source_matches():
    root = Path(__file__).resolve().parent
    source = (root / "function_app.py").read_bytes().replace(b"\r\n", b"\n")
    metadata = (root / "functions.metadata").read_bytes().replace(b"\r\n", b"\n")
    for name in ("SourceCode.zip", "Deploy.zip"):
        with zipfile.ZipFile(root / name) as archive:
            assert archive.testzip() is None, name
            assert archive.read("function_app.py") == source, name
            assert archive.read("fabric_lib/functions.metadata") == metadata, name
            assert not any(
                "test_" in member or "__pycache__" in member
                for member in archive.namelist()
            ), name


if __name__ == "__main__":
    test_uses_managed_identity_and_resource_scope()
    test_enforces_timespan_and_row_limit()
    test_removes_trailing_query_semicolon()
    test_forwards_upstream_error()
    test_rejects_invalid_input()
    test_stream_releases_on_close()
    test_token_cache_and_errors()
    test_packed_source_matches()
    print("ALL TELEMETRY TESTS PASSED")
