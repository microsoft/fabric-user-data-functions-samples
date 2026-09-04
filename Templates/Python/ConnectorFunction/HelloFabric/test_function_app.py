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
import os
import sys
import types


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

        def function(self, *_args, **_kwargs):
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


if __name__ == "__main__":
    test_streams_multiple_chunks_without_buffering()
    print("  ok: streams multiple chunks without buffering")
    test_forwards_client_request_id_header()
    print("  ok: forwards clientRequestId as x-ms-client-request-id header")
    test_execute_command_routes_to_mgmt_and_streams()
    print("  ok: executeCommand routes to /v1/rest/mgmt and streams")
    print("ALL UDF STREAMING TESTS PASSED")
