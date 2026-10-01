"""Standalone tests for the rayfin_telemetry_v1 streaming function."""

import asyncio
import importlib
import json
import os
import sys
import types
from unittest.mock import patch


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

    async def post(self, url, json=None, headers=None):
        self.captured_url = url
        self.captured_headers = headers
        self.captured_json = json
        return self._response

    async def get(self, _url, params=None, headers=None):
        self.captured_get_headers = headers
        self.captured_get_params = params
        return self._token_response


def _payload(**overrides):
    input_data = {
        "kql": "AppTraces | project TimeGenerated, Message",
        "workspaceId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "timespan": "P1D",
        "maxRows": 1000,
    }
    input_data.update(overrides)
    return {"operation": "query", "input": input_data}


async def _invoke(chunks, payload=None, status=200, error_body=""):
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

    function_app._get_session = _fake_get_session
    environment = {
        "IDENTITY_ENDPOINT": "http://127.0.0.1:41741/MSI/token",
        "IDENTITY_HEADER": "rotating-identity-header",
    }
    with patch.dict(os.environ, environment, clear=False):
        result = await function_app.rayfin_telemetry_v1(payload or _payload())

    body = []
    if hasattr(result.body, "__aiter__"):
        async for chunk in result.body:
            body.append(chunk)
    else:
        body.extend(result.body)
    return function_app, result, body, query_response, token_response, session


async def _check_uses_managed_identity_and_server_workspace():
    chunks = [b'{"tables":[', b'{"name":"PrimaryResult"}', b"]}"]
    function_app, result, body, response, token_response, session = await _invoke(
        chunks,
        _payload(resourceId="/subscriptions/caller-controlled"),
    )

    assert body == chunks
    assert response.text_called is False
    assert result.media_type == function_app._JSON_MEDIA_TYPE
    assert session.captured_url == (
        "https://api.loganalytics.azure.com/v1/workspaces/"
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/query"
    )
    assert "caller-controlled" not in session.captured_url
    assert session.captured_headers["Authorization"] == "Bearer fake-monitor-token"
    assert session.captured_get_params == {
        "resource": "https://api.loganalytics.io",
        "api-version": "2019-08-01",
    }
    assert session.captured_get_headers == {
        "X-IDENTITY-HEADER": "rotating-identity-header"
    }
    assert token_response.released is True


async def _check_enforces_timespan_and_row_limit():
    _function_app, _result, _body, _response, _token_response, session = (
        await _invoke([b'{"tables":[]}'])
    )
    assert session.captured_json == {
        "query": "AppTraces | project TimeGenerated, Message\n| take 1000",
        "timespan": "P1D",
    }


async def _check_forwards_upstream_error():
    error_body = json.dumps({"error": {"code": "Forbidden"}})
    _function_app, result, body, response, _token_response, _session = (
        await _invoke([], status=403, error_body=error_body)
    )
    assert result.status_code == 403
    assert b"".join(body) == error_body.encode("utf-8")
    assert response.text_called is True
    assert response.released is True


async def _check_rejects_invalid_input():
    function_app = _load_function_app()
    invalid_payloads = [
        {"operation": "emit", "input": {}},
        _payload(workspaceId="not-a-guid"),
        _payload(timespan="P31D"),
        _payload(maxRows=10001),
        _payload(kql=""),
    ]
    for payload in invalid_payloads:
        try:
            await function_app.rayfin_telemetry_v1(payload)
        except ValueError:
            continue
        raise AssertionError(f"invalid payload was accepted: {payload}")


def test_uses_managed_identity_and_server_workspace():
    asyncio.run(_check_uses_managed_identity_and_server_workspace())


def test_enforces_timespan_and_row_limit():
    asyncio.run(_check_enforces_timespan_and_row_limit())


def test_forwards_upstream_error():
    asyncio.run(_check_forwards_upstream_error())


def test_rejects_invalid_input():
    asyncio.run(_check_rejects_invalid_input())


if __name__ == "__main__":
    test_uses_managed_identity_and_server_workspace()
    test_enforces_timespan_and_row_limit()
    test_forwards_upstream_error()
    test_rejects_invalid_input()
    print("ALL TELEMETRY TESTS PASSED")
