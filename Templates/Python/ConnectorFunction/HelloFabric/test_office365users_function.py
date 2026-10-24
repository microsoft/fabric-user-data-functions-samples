"""
Standalone tests for the optional caller `accessToken` on
`rayfin_office365users_v1`.

A minimal `fabric.functions` stub and a fake `azure.connectors` SDK are
injected so `function_app` imports without the Fabric runtime. The tests
assert that without a token the cached managed-identity client is used, and
with a token a fresh per-call client is built, never cached, and always closed.

Run directly (`python3 test_office365users_function.py`) or under pytest.
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
    assert response.status_code == 403
    assert _body(response) == b'{"error":"forbidden"}'
    assert _FakeO365Client.instances[0].closed is True


def test_o365_without_token_uses_cached_msi_client():
    asyncio.run(_check_o365_without_token_uses_cached_msi_client())


def test_o365_with_token_uses_fresh_client_and_closes():
    asyncio.run(_check_o365_with_token_uses_fresh_client_and_closes())


def test_o365_with_token_closes_on_connector_error():
    asyncio.run(_check_o365_with_token_closes_on_connector_error())


if __name__ == "__main__":
    test_o365_without_token_uses_cached_msi_client()
    print("  ok: o365 without accessToken uses cached MSI client")
    test_o365_with_token_uses_fresh_client_and_closes()
    print("  ok: o365 with accessToken uses fresh, closed, uncached client")
    test_o365_with_token_closes_on_connector_error()
    print("  ok: o365 token client is closed on connector error")
    print("ALL OFFICE365USERS TESTS PASSED")
