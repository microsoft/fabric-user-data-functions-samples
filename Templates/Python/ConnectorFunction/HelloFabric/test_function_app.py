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
        def __init__(self, body, media_type=None, status_code=None, headers=None):
            self.body = body
            self.media_type = media_type
            self.status_code = status_code
            self.headers = headers or {}

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
        self.captured_json = None
        self.captured_allow_redirects = None

    async def post(self, url, json=None, headers=None, *, allow_redirects=True):
        self.captured_url = url
        self.captured_headers = headers
        self.captured_json = json
        self.captured_allow_redirects = allow_redirects
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


# --- rayfin_office365users_v1: optional accessToken vs managed identity -------


class _FakeConnectorException(Exception):
    def __init__(self, status_code, response_body):
        super().__init__("connector error")
        self.status_code = status_code
        self.response_body = response_body


class _FakeTokenProvider:
    def __init__(self, api_key):
        if not api_key:
            raise ValueError("empty")
        self.api_key = api_key


class _FakeO365Client:
    instances: list = []

    def __init__(self, url, token_provider=None, options=None):
        self.url = url
        self.token_provider = token_provider
        self.closed = False
        self.fail_with = None
        _FakeO365Client.instances.append(self)

    async def manager_async(self, **kwargs):
        if self.fail_with is not None:
            raise self.fail_with
        return {"displayName": "Boss", "kwargs": kwargs}

    async def close(self):
        self.closed = True


def _install_connectors_stub():
    azure = types.ModuleType("azure")
    connectors = types.ModuleType("azure.connectors")
    connectors.ConnectorException = _FakeConnectorException
    o365 = types.ModuleType("azure.connectors.office365users")
    o365.Office365usersClient = _FakeO365Client
    sdk = types.ModuleType("azure.connectors.sdk")
    auth = types.ModuleType("azure.connectors.sdk.authentication")
    auth.ConnectionStringTokenProvider = _FakeTokenProvider
    azure.connectors = connectors
    connectors.office365users = o365
    connectors.sdk = sdk
    sdk.authentication = auth
    sys.modules.update(
        {
            "azure": azure,
            "azure.connectors": connectors,
            "azure.connectors.office365users": o365,
            "azure.connectors.sdk": sdk,
            "azure.connectors.sdk.authentication": auth,
        }
    )


def _o365_payload(**extra):
    data = {"connectionRuntimeUrl": "https://rt.example/conn", "userId": "u1"}
    data.update(extra)
    return {"operation": "managerAsync", "input": data}


def _o365_setup():
    _install_connectors_stub()
    mod = _load_function_app()
    mod._acn_clients.clear()
    _FakeO365Client.instances.clear()
    return mod


def _body(response):
    return b"".join(response.body)


async def _check_o365_without_token_uses_cached_msi_client():
    mod = _o365_setup()
    r1 = await mod.rayfin_office365users_v1(_o365_payload())
    r2 = await mod.rayfin_office365users_v1(_o365_payload())
    assert r1.status_code == 200 and r2.status_code == 200
    assert len(_FakeO365Client.instances) == 1, "MSI client must be cached"
    client = _FakeO365Client.instances[0]
    assert client.token_provider is None, "no token -> SDK default (MSI)"
    assert client.closed is False, "cached MSI client must not be closed"


async def _check_o365_with_token_uses_fresh_client_and_closes():
    mod = _o365_setup()
    response = await mod.rayfin_office365users_v1(
        _o365_payload(accessToken="abc.def")
    )
    assert response.status_code == 200
    assert b"Boss" in _body(response)
    assert len(_FakeO365Client.instances) == 1
    client = _FakeO365Client.instances[0]
    assert isinstance(client.token_provider, _FakeTokenProvider)
    assert client.token_provider.api_key == "abc.def", "token passed as-is"
    assert client.closed is True, "per-call token client must be closed"
    assert mod._acn_clients == {}, "token client must not be cached"
    called = await client.manager_async(userId="u1")
    assert "accessToken" not in called["kwargs"], "token never reaches SDK kwargs"


async def _check_o365_with_token_closes_on_connector_error():
    mod = _o365_setup()
    original = _FakeO365Client.manager_async

    async def failing(self, **kwargs):
        raise _FakeConnectorException(403, '{"error":"forbidden"}')

    _FakeO365Client.manager_async = failing
    try:
        response = await mod.rayfin_office365users_v1(
            _o365_payload(accessToken="tok")
        )
    finally:
        _FakeO365Client.manager_async = original
    assert response.status_code == 200
    assert response.headers == {"x-fabric-udf-status": "Succeeded"}
    assert json.loads(_body(response)) == {
        "status": "error",
        "error": {"message": "forbidden", "code": "Http403", "category": "api", "details": "403"},
    }
    assert _FakeO365Client.instances[0].closed is True


def test_o365_without_token_uses_cached_msi_client():
    asyncio.run(_check_o365_without_token_uses_cached_msi_client())


def test_o365_with_token_uses_fresh_client_and_closes():
    asyncio.run(_check_o365_with_token_uses_fresh_client_and_closes())


def test_o365_with_token_closes_on_connector_error():
    asyncio.run(_check_o365_with_token_closes_on_connector_error())


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
    test_o365_without_token_uses_cached_msi_client()
    print("  ok: o365 without accessToken uses cached MSI client")
    test_o365_with_token_uses_fresh_client_and_closes()
    print("  ok: o365 with accessToken uses fresh, closed, uncached client")
    test_o365_with_token_closes_on_connector_error()
    print("  ok: o365 token client is closed on connector error")
    print("ALL UDF STREAMING TESTS PASSED")
