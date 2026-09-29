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
import json
import os
import sys
import types
from unittest.mock import patch


def _install_fabric_stub():
    """Inject a minimal `fabric.functions` so `function_app` imports."""
    if "fabric.functions" in sys.modules:
        return

    fabric = types.ModuleType("fabric")
    functions = types.ModuleType("fabric.functions")

    class UserDataFunctions:
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
        self.read_called = False
        self.released = False

    async def text(self):
        # The 200 path must never buffer/parse the body.
        self.text_called = True
        return b"".join(self.content._chunks).decode("utf-8")

    async def read(self):
        self.read_called = True
        return b"".join(self.content._chunks)

    async def json(self):
        return self._json_body

    def release(self):
        self.released = True
        return _CompletedAwaitable()


class _FakeSession:
    def __init__(self, response, get_response=None):
        self._response = response
        self._get_response = get_response
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

    async def get(self, url, params=None, headers=None):
        self.captured_get_headers = headers
        self.captured_get_params = params
        return self._get_response


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


def _telemetry_payload(**overrides):
    input_data = {
        "kql": "AppTraces | project TimeGenerated, Message",
        "workspaceId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "timespan": "P1D",
        "maxRows": 1000,
    }
    input_data.update(overrides)
    return {"operation": "query", "input": input_data}


async def _invoke_telemetry(chunks, payload=None, status=200, error_body=""):
    mod = _load_function_app()
    mod._telemetry_token = None
    mod._telemetry_token_expires_at = 0
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

    mod._get_session = _fake_get_session  # type: ignore[attr-defined]
    environment = {
        "IDENTITY_ENDPOINT": "http://127.0.0.1:41741/MSI/token",
        "IDENTITY_HEADER": "rotating-identity-header",
    }
    with patch.dict(os.environ, environment, clear=False):
        result = await mod.rayfin_telemetry_v1(payload or _telemetry_payload())

    body = []
    if hasattr(result.body, "__aiter__"):
        async for chunk in result.body:
            body.append(chunk)
    else:
        body.extend(result.body)
    return mod, result, body, query_response, token_response, session


async def _check_telemetry_uses_managed_identity_and_server_workspace():
    chunks = [b'{"tables":[', b'{"name":"PrimaryResult"}', b"]}"]
    mod, result, body, response, token_response, session = await _invoke_telemetry(
        chunks,
        _telemetry_payload(resourceId="/subscriptions/caller-controlled"),
    )

    assert body == chunks, "telemetry response must be relayed chunk-by-chunk"
    assert response.text_called is False, "successful telemetry query must not buffer"
    assert result.media_type == mod._JSON_MEDIA_TYPE
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


async def _check_telemetry_enforces_timespan_and_row_limit():
    _mod, _result, _body, _response, _token_response, session = (
        await _invoke_telemetry([b'{"tables":[]}'])
    )
    assert session.captured_json == {
        "query": "AppTraces | project TimeGenerated, Message\n| take 1000",
        "timespan": "P1D",
    }


async def _check_telemetry_forwards_upstream_error():
    error_body = json.dumps({"error": {"code": "Forbidden"}})
    _mod, result, body, response, _token_response, _session = (
        await _invoke_telemetry([], status=403, error_body=error_body)
    )
    assert result.status_code == 403
    assert b"".join(body) == error_body.encode("utf-8")
    assert response.text_called is True
    assert response.released is True


async def _check_telemetry_rejects_invalid_input():
    mod = _load_function_app()
    invalid_payloads = [
        {"operation": "emit", "input": {}},
        _telemetry_payload(workspaceId="not-a-guid"),
        _telemetry_payload(timespan="P31D"),
        _telemetry_payload(maxRows=10001),
        _telemetry_payload(kql=""),
    ]
    for payload in invalid_payloads:
        try:
            await mod.rayfin_telemetry_v1(payload)
        except ValueError:
            continue
        raise AssertionError(f"invalid payload was accepted: {payload}")


def test_streams_multiple_chunks_without_buffering():
    asyncio.run(_check_streams_multiple_chunks_without_buffering())


def test_forwards_client_request_id_header():
    asyncio.run(_check_forwards_client_request_id_header())


def test_execute_command_routes_to_mgmt_and_streams():
    asyncio.run(_check_execute_command_routes_to_mgmt_and_streams())


def test_telemetry_uses_managed_identity_and_server_workspace():
    asyncio.run(_check_telemetry_uses_managed_identity_and_server_workspace())


def test_telemetry_enforces_timespan_and_row_limit():
    asyncio.run(_check_telemetry_enforces_timespan_and_row_limit())


def test_telemetry_forwards_upstream_error():
    asyncio.run(_check_telemetry_forwards_upstream_error())


def test_telemetry_rejects_invalid_input():
    asyncio.run(_check_telemetry_rejects_invalid_input())


if __name__ == "__main__":
    test_streams_multiple_chunks_without_buffering()
    print("  ok: streams multiple chunks without buffering")
    test_forwards_client_request_id_header()
    print("  ok: forwards clientRequestId as x-ms-client-request-id header")
    test_execute_command_routes_to_mgmt_and_streams()
    print("  ok: executeCommand routes to /v1/rest/mgmt and streams")
    test_telemetry_uses_managed_identity_and_server_workspace()
    print("  ok: telemetry uses managed identity and server-provided workspace")
    test_telemetry_enforces_timespan_and_row_limit()
    print("  ok: telemetry enforces timespan and maximum rows")
    test_telemetry_forwards_upstream_error()
    print("  ok: telemetry forwards upstream errors")
    test_telemetry_rejects_invalid_input()
    print("  ok: telemetry rejects invalid input")
    print("ALL UDF STREAMING TESTS PASSED")
