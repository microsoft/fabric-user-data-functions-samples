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

import ast
import asyncio
import contextlib
import gc
import importlib
import inspect
import io
import logging
import os
import sys
import threading
import tracemalloc
import types
import zipfile
from hashlib import sha256
from pathlib import Path


_ARCHIVE_COMMON_ZIP_INFO = (
    (2026, 8, 21, 0, 0, 0),
    8,
    b"",
    b"",
    0,
    0,
    20,
    0,
    0,
    0,
    0,
    0,
)
_ARCHIVE_NON_TARGET_GOLDEN = {
    ".vscode/extensions.json": (
        "03165c83567b5c86e1567a2b7dc08bc351232b4dafd040572c9040672e90f05c",
        1878686440,
        77,
        101,
    ),
    ".vscode/launch.json": (
        "3e36a191fed0837bcda80609f1cad5d31ba4a95a5efc02bd84c6bb6a356162ad",
        2818476601,
        153,
        275,
    ),
    ".vscode/settings.json": (
        "7a1b67e18a29a03d4b16d09aaf0d9fe6c5a11956326254689697798cb59c357d",
        3649402893,
        225,
        474,
    ),
    ".vscode/tasks.json": (
        "19b73845462d72166ff57157a9fa618c673b874dafec5a6eb426159d58a2f901",
        30700519,
        293,
        855,
    ),
    ".funcignore": (
        "f2cb4ac301787f8ef64476a8cf57fa77c3db8ee19ac52cd919bc7764fbfeab1c",
        3243196604,
        82,
        105,
    ),
    ".gitignore": (
        "4f85711be6b32d8d197b9fb557c900c6979b4eb6603a896e9e3324285a48b9fe",
        2156272879,
        1010,
        1995,
    ),
    "host.json": (
        "ce57f4350cf7948e92d99518e80833f0122d461f95eb79a690804fa59d6a08fe",
        2189393286,
        191,
        308,
    ),
    "local.settings.json": (
        "59b4b43d6f3bd668a41ba4699884e4cef4e36c7ae3a8d48fa06ff80e56b18243",
        2366048237,
        270,
        450,
    ),
    "readme.md": (
        "19818250f0549897b1b1e8950491e1d3dd27723165e2f4afa9086e42fe358bf8",
        1630295394,
        664,
        1252,
    ),
    "requirements.txt": (
        "697d7901805e94305b7b84ba7d23541e7a7fda630d77a135b921917bea399915",
        1772997688,
        203,
        284,
    ),
    "fabric_lib/functions.metadata": (
        "ac256faf0bfc4e92e1fb7ec732111095ce88bd8616ebdb06011c3dafdd2a8030",
        3017962651,
        302,
        1246,
    ),
}


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

    class FabricItem:
        """Minimal type-hint target for standalone tests."""

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
        self.iterations_started = 0

    async def iter_any(self):
        self.iterations_started += 1
        for chunk in self._chunks:
            yield chunk


class _FakeBodyIterator:
    def __init__(self, chunks=(), read_error=None, close_error=None, block=False):
        self._chunks = iter(chunks)
        self._read_error = read_error
        self._close_error = close_error
        self._block = block
        self.read_started = asyncio.Event()
        self.next_count = 0
        self.close_count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.next_count += 1
        self.read_started.set()
        if self._block:
            await asyncio.Future()
        if self._read_error is not None:
            raise self._read_error
        try:
            return next(self._chunks)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self):
        self.close_count += 1
        if self._close_error is not None:
            raise self._close_error


class _FakeIteratorContent:
    def __init__(self, iterator):
        self._iterator = iterator

    def iter_any(self):
        return self._iterator


class _FakeBodyIteratorWithoutClose:
    def __init__(self, chunks):
        self._chunks = iter(chunks)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._chunks)
        except StopIteration:
            raise StopAsyncIteration from None


class _FakeResponse:
    def __init__(self, chunks, status=200, on_release=None, release_error=None):
        self.status = status
        self.headers = {}
        self.content = _FakeContent(chunks)
        self.text_called = False
        self.release_count = 0
        self._on_release = on_release
        self._release_error = release_error

    async def text(self):
        # The 200 path must never call this; failures surface the upstream text.
        self.text_called = True
        return b"".join(self.content._chunks).decode("utf-8")

    def release(self):
        self.release_count += 1
        if self._release_error is not None:
            raise self._release_error
        if self._on_release is not None:
            self._on_release()


class _FakeSession:
    def __init__(self, response):
        self._response = response
        self.captured_headers = None
        self.captured_url = None
        self.close_count = 0

    async def post(self, url, json=None, headers=None):
        self.captured_url = url
        self.captured_headers = headers
        return self._response

    async def close(self):
        self.close_count += 1


class _SingleSlotFakeSession:
    def __init__(self, responses):
        self._responses = iter(responses)
        self.slot_acquired = False
        self.close_count = 0

    async def post(self, _url, json=None, headers=None):
        if self.slot_acquired:
            raise RuntimeError("the only fake pool slot is retained")
        response = next(self._responses)
        self.slot_acquired = True
        response._on_release = self._release_slot
        return response

    def _release_slot(self):
        if not self.slot_acquired:
            raise RuntimeError("the fake pool slot was released twice")
        self.slot_acquired = False

    async def close(self):
        self.close_count += 1


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


async def _invoke_non_200(payload):
    mod = _load_function_app()
    response = _FakeResponse([b'{"error":"upstream"}'], status=503)
    response.headers["Content-Type"] = "application/problem+json"
    session = _FakeSession(response)

    async def _fake_get_session():
        return session

    mod._get_session = _fake_get_session  # type: ignore[attr-defined]
    result = await mod.rayfin_kusto_v1(payload, _FakeKustoClient())
    return result, response


async def _invoke_semantic_model(chunks, status=200):
    mod = _load_function_app()
    response = _FakeResponse(chunks, status=status)
    response.headers["Content-Type"] = "application/problem+json"
    session = _FakeSession(response)

    async def _fake_get_session():
        return session

    mod._get_session = _fake_get_session  # type: ignore[attr-defined]
    payload = {
        "input": {
            "itemId": "model-id",
            "workspaceId": "workspace-id",
            "query": "EVALUATE ROW(\"value\", 1)",
        }
    }
    result = await mod.rayfin_semantic_model_v1(payload, "fake-token")
    return result, response


def _valid_mcp_envelope(headers=None, body='{"jsonrpc":"2.0"}', server_policy=None):
    return {
        "transport": "mcp-streamable-http",
        "version": 1,
        "method": "POST",
        "protocolVersion": "2025-11-25",
        "headers": {} if headers is None else headers,
        "body": body,
        "serverPolicy": server_policy,
    }


def _valid_mcp_server_policy():
    return {
        "id": "fabriciq",
        "url": "https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq",
        "protectedHeaders": {
            "X-Variant": "Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect"
        },
    }


def _managed_mcp_payload(method="tools/list", params=None, request_id=7):
    if params is None:
        params = {}
    return {
        "version": 1,
        "protocolVersion": "2026-07-28",
        "message": {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        },
    }


class _ManagedMcpResponse:
    def __init__(self, body, status=200, release_failures=0):
        self.status = status
        self._body = body
        self.content = _FakeContent([body] if body else [])
        self.release_count = 0
        self.close_count = 0
        self._release_failures = release_failures

    def release(self):
        self.release_count += 1
        if self.release_count <= self._release_failures:
            raise RuntimeError("managed release failed")

    def close(self):
        self.close_count += 1


class _ManagedMcpSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def post(self, url, data=None, headers=None, timeout=None):
        self.calls.append((url, data, headers, timeout))
        return self.response


def _managed_mcp_limits(mod):
    return mod._McpManagedLimits(
        64 * 1024,
        32,
        (200, 204, 400),
        ("working", "completed", "failed", "cancelled"),
        ("completed", "failed", "cancelled"),
        100,
    )


async def _call_managed_mcp(payload, response_body, status=200):
    mod = _load_function_app()
    response = _ManagedMcpResponse(response_body, status)
    session = _ManagedMcpSession(response)
    result = await mod._invoke_managed_mcp(
        payload,
        "secret-test-token",
        _managed_mcp_limits(mod),
        session,
    )
    return mod, result, response, session


def _assert_fixed_error(exception_type, message, action):
    try:
        action()
    except exception_type as error:
        assert type(error) is exception_type
        assert str(error) == message
        return error
    raise AssertionError(f"expected {exception_type.__name__}")


def _fabric_1_0_142_to_async_byte_iterator(content):
    """Mirror fabric-user-data-functions 1.0.142's stream body adapter."""

    def _ensure_bytes(chunk):
        if isinstance(chunk, bytes):
            return chunk
        if isinstance(chunk, str):
            return chunk.encode("utf-8")
        if isinstance(chunk, (bytearray, memoryview)):
            return bytes(chunk)
        raise TypeError(
            f"Stream chunks must be bytes or str, got {type(chunk).__name__!r}."
        )

    if hasattr(content, "__aiter__"):

        async def _from_async():
            async for chunk in content:
                yield _ensure_bytes(chunk)

        return _from_async()

    async def _from_sync():
        for chunk in content:
            yield _ensure_bytes(chunk)

    return _from_sync()


async def _collect_dropped_stream_owners():
    gc.collect()
    for _attempt in range(3):
        await asyncio.sleep(0)


class _ExplodingDict(dict):
    def keys(self):
        raise RuntimeError("customer-secret-dict")


class _ExplodingList(list):
    def __iter__(self):
        raise RuntimeError("customer-secret-list")


class _ExplodingStr(str):
    __hash__ = str.__hash__

    def __eq__(self, _other):
        raise RuntimeError("customer-secret-string")

    def encode(self, *_args, **_kwargs):
        raise RuntimeError("customer-secret-string")

    def lower(self):
        raise RuntimeError("customer-secret-string")


class _ExplodingPolicy:
    def __bool__(self):
        raise RuntimeError("customer-secret-policy")

    def __eq__(self, _other):
        raise RuntimeError("customer-secret-policy")

    def __repr__(self):
        raise RuntimeError("customer-secret-policy")


class _SpoofedProtectedHeaders:
    def __eq__(self, _other):
        return True

    def __iter__(self):
        return iter(())


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
    assert response.release_count == 1, "successful relay must release exactly once"


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


def test_non_200_releases_response_synchronously():
    result, response = asyncio.run(_invoke_non_200(_payload()))
    assert result.status_code == 503
    assert result.media_type == "application/problem+json"
    assert list(result.body) == [b'{"error":"upstream"}']
    assert response.text_called is True
    assert response.release_count == 1


def test_semantic_model_success_and_non_200_release_synchronously():
    async def check():
        success, success_response = await _invoke_semantic_model([b"one", b"two"])
        relayed = []
        async for chunk in success.body:
            relayed.append(chunk)
        assert relayed == [b"one", b"two"]
        assert success_response.release_count == 1

        failure, failure_response = await _invoke_semantic_model(
            [b'{"error":"upstream"}'], status=429
        )
        assert failure.status_code == 429
        assert failure.media_type == "application/problem+json"
        assert list(failure.body) == [b'{"error":"upstream"}']
        assert failure_response.text_called is True
        assert failure_response.release_count == 1

    asyncio.run(check())


def test_fabric_runtime_wrapper_drop_releases_pre_start_response_and_pool_slot():
    async def check():
        mod = _load_function_app()
        first_response = _FakeResponse([b"never-read"])
        second_response = _FakeResponse([b"next"])
        session = _SingleSlotFakeSession((first_response, second_response))
        original_get_session = mod._get_session

        async def _fake_get_session():
            return session

        async def invoke():
            return await mod.rayfin_kusto_v1(_payload(), _FakeKustoClient())

        mod._get_session = _fake_get_session  # type: ignore[attr-defined]
        try:
            result = await invoke()
            body = result.body
            wrapper = _fabric_1_0_142_to_async_byte_iterator(body)
            first_read = asyncio.create_task(anext(wrapper))
            first_read.cancel()
            try:
                await first_read
            except asyncio.CancelledError:
                assert first_read.cancelled()
            else:
                raise AssertionError("outer first read must be cancelled")

            await wrapper.aclose()
            assert first_response.content.iterations_started == 0
            assert first_response.release_count == 0
            del first_read, wrapper, body, result
            await _collect_dropped_stream_owners()

            assert first_response.release_count == 1
            assert session.slot_acquired is False
            next_result = await invoke()
            await next_result.body.aclose()
            assert second_response.release_count == 1
            assert session.slot_acquired is False
            assert session.close_count == 0
        finally:
            mod._get_session = original_get_session

    asyncio.run(check())


def test_fabric_runtime_wrapper_drop_releases_partially_yielded_response():
    async def check():
        mod = _load_function_app()
        first_response = _FakeResponse([b"first", b"unread"])
        second_response = _FakeResponse([b"next"])
        session = _SingleSlotFakeSession((first_response, second_response))
        original_get_session = mod._get_session

        async def _fake_get_session():
            return session

        async def invoke():
            return await mod.rayfin_kusto_v1(_payload(), _FakeKustoClient())

        mod._get_session = _fake_get_session  # type: ignore[attr-defined]
        try:
            result = await invoke()
            body = result.body
            wrapper = _fabric_1_0_142_to_async_byte_iterator(body)
            assert await anext(wrapper) == b"first"
            await wrapper.aclose()
            assert first_response.release_count == 0
            del wrapper, body, result
            await _collect_dropped_stream_owners()

            assert first_response.release_count == 1
            assert session.slot_acquired is False
            next_result = await invoke()
            await next_result.body.aclose()
            assert second_response.release_count == 1
            assert session.slot_acquired is False
        finally:
            mod._get_session = original_get_session

    asyncio.run(check())


def test_real_aiohttp_runtime_wrapper_drop_releases_response_and_pool_slot():
    async def check():
        from aiohttp import web

        mod = _load_function_app()
        stop = asyncio.Event()
        requests_started = 0

        async def endpoint(request):
            nonlocal requests_started
            requests_started += 1
            response = web.StreamResponse()
            await response.prepare(request)
            await response.write(b"x")
            await stop.wait()
            return response

        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", endpoint)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        connector = mod.aiohttp.TCPConnector(limit=1)
        session = mod.aiohttp.ClientSession(connector=connector)
        original_get_session = mod._get_session

        async def _real_get_session():
            return session

        async def invoke():
            payload = _payload()
            payload["input"]["queryServiceUri"] = f"http://127.0.0.1:{port}"
            return await mod.rayfin_kusto_v1(payload, _FakeKustoClient())

        mod._get_session = _real_get_session  # type: ignore[attr-defined]
        try:
            result = await invoke()
            body = result.body
            response = body._response
            original_release = response.release
            release_count = 0

            def counted_release():
                nonlocal release_count
                release_count += 1
                return original_release()

            response.release = counted_release
            wrapper = _fabric_1_0_142_to_async_byte_iterator(body)
            first_read = asyncio.create_task(anext(wrapper))
            first_read.cancel()
            try:
                await first_read
            except asyncio.CancelledError:
                assert first_read.cancelled()
            else:
                raise AssertionError("outer first read must be cancelled")
            await wrapper.aclose()
            del first_read, wrapper, body, result, response
            await _collect_dropped_stream_owners()

            assert release_count == 1
            assert len(connector._acquired) == 0
            next_result = await asyncio.wait_for(invoke(), timeout=2)
            assert requests_started == 2
            await next_result.body.aclose()
            assert len(connector._acquired) == 0
            assert session.closed is False
        finally:
            mod._get_session = original_get_session
            stop.set()
            await session.close()
            await runner.cleanup()

    asyncio.run(check())


def test_stream_aclose_after_pre_start_cancellation_releases_response_once():
    async def check():
        mod = _load_function_app()
        kusto_response = _FakeResponse([b"never-read"])
        kusto_session = _FakeSession(kusto_response)

        async def _fake_get_session():
            return kusto_session

        mod._get_session = _fake_get_session  # type: ignore[attr-defined]
        kusto_result = await mod.rayfin_kusto_v1(_payload(), _FakeKustoClient())
        semantic_result, semantic_response = await _invoke_semantic_model(
            [b"never-read"]
        )

        for result, response in (
            (kusto_result, kusto_response),
            (semantic_result, semantic_response),
        ):
            first_read = asyncio.create_task(anext(result.body))
            first_read.cancel()
            try:
                await first_read
            except asyncio.CancelledError:
                assert first_read.cancelled()
            else:
                raise AssertionError("first body read must be cancelled")

            assert response.content.iterations_started == 0
            await result.body.aclose()
            await result.body.aclose()
            assert response.release_count == 1
            try:
                await anext(result.body)
            except StopAsyncIteration:
                assert result.body._released
            else:
                raise AssertionError("closed body must remain exhausted")

        assert kusto_session.close_count == 0

    asyncio.run(check())


def test_release_failure_retains_ownership_until_retry_reuses_pool_slot():
    async def check():
        mod = _load_function_app()
        release_error = RuntimeError("release failed")
        first_response = _FakeResponse([b"never-read"], release_error=release_error)
        first_iterator = _FakeBodyIterator([b"never-read"])
        first_response.content = _FakeIteratorContent(first_iterator)
        second_response = _FakeResponse([b"next"])
        session = _SingleSlotFakeSession((first_response, second_response))
        original_get_session = mod._get_session

        async def _fake_get_session():
            return session

        mod._get_session = _fake_get_session  # type: ignore[attr-defined]
        try:
            first_result = await mod.rayfin_kusto_v1(_payload(), _FakeKustoClient())
            first_body = first_result.body
            try:
                await first_body.aclose()
            except RuntimeError as error:
                assert error is release_error
            else:
                raise AssertionError("response release failure must propagate")

            assert first_body._response is first_response
            assert first_body._released is False
            assert first_response.release_count == 1
            assert first_iterator.next_count == 0
            assert first_iterator.close_count == 0
            assert session.slot_acquired is True

            first_response._release_error = None
            await first_body.aclose()
            await first_body.aclose()
            assert first_body._response is None
            assert first_body._released is True
            assert first_response.release_count == 2
            assert first_iterator.close_count == 1
            assert session.slot_acquired is False

            second_result = await asyncio.wait_for(
                mod.rayfin_kusto_v1(_payload(), _FakeKustoClient()),
                timeout=2,
            )
            await second_result.body.aclose()
            assert second_response.release_count == 1
            assert session.slot_acquired is False
        finally:
            mod._get_session = original_get_session

    asyncio.run(check())


def test_response_body_iterator_preserves_chunks_and_releases_on_exhaustion():
    async def check():
        mod = _load_function_app()
        chunks = [b"first", b"", b"third"]
        iterator = _FakeBodyIterator(chunks)
        response = _FakeResponse([])
        response.content = _FakeIteratorContent(iterator)
        body = mod._ResponseBodyIterator(response)

        received = []
        async for chunk in body:
            received.append(chunk)

        assert received == chunks
        assert iterator.next_count == len(chunks) + 1
        assert iterator.close_count == 1
        assert response.release_count == 1

    asyncio.run(check())


def test_response_body_iterator_supports_iterator_without_aclose():
    async def check():
        mod = _load_function_app()
        iterator = _FakeBodyIteratorWithoutClose([b"one", b"two"])
        response = _FakeResponse([])
        response.content = _FakeIteratorContent(iterator)
        body = mod._ResponseBodyIterator(response)

        received = [chunk async for chunk in body]

        assert received == [b"one", b"two"]
        assert response.release_count == 1

    asyncio.run(check())


def test_response_body_iterator_releases_and_delegates_on_read_failure():
    async def check():
        mod = _load_function_app()
        read_error = RuntimeError("read failed")
        iterator = _FakeBodyIterator(read_error=read_error)
        response = _FakeResponse([])
        response.content = _FakeIteratorContent(iterator)
        body = mod._ResponseBodyIterator(response)

        try:
            await anext(body)
        except RuntimeError as error:
            assert error is read_error
        else:
            raise AssertionError("body read must propagate its failure")

        await body.aclose()
        assert iterator.close_count == 1
        assert response.release_count == 1

    asyncio.run(check())


def test_response_body_iterator_releases_and_delegates_on_active_cancellation():
    async def check():
        mod = _load_function_app()
        iterator = _FakeBodyIterator(block=True)
        response = _FakeResponse([])
        response.content = _FakeIteratorContent(iterator)
        body = mod._ResponseBodyIterator(response)

        read = asyncio.create_task(anext(body))
        await iterator.read_started.wait()
        read.cancel()
        try:
            await read
        except asyncio.CancelledError:
            assert read.cancelled()
        else:
            raise AssertionError("active body read must be cancelled")

        assert iterator.close_count == 1
        assert response.release_count == 1

    asyncio.run(check())


def test_response_body_iterator_does_not_mask_read_failure_when_close_fails():
    async def check():
        mod = _load_function_app()
        read_error = RuntimeError("read failed")
        close_error = RuntimeError("close failed")
        iterator = _FakeBodyIterator(
            read_error=read_error,
            close_error=close_error,
        )
        response = _FakeResponse([])
        response.content = _FakeIteratorContent(iterator)
        body = mod._ResponseBodyIterator(response)

        try:
            await anext(body)
        except RuntimeError as error:
            assert error is read_error
            assert error.__cause__ is close_error
        else:
            raise AssertionError("body read must remain the primary failure")

        assert iterator.close_count == 1
        assert response.release_count == 1

    asyncio.run(check())


def test_response_body_iterator_release_primitive_is_idempotent_and_surfaces_errors():
    mod = _load_function_app()
    response = _FakeResponse([])
    body = mod._ResponseBodyIterator(response)

    assert body._release_response() is None
    assert body._release_response() is None
    assert response.release_count == 1

    release_error = RuntimeError("release failed")
    failing_response = _FakeResponse([], release_error=release_error)
    failing_body = mod._ResponseBodyIterator(failing_response)
    try:
        failing_body._release_response()
    except RuntimeError as error:
        assert error is release_error
    else:
        raise AssertionError("response release failure must propagate")

    assert failing_body._response is failing_response
    assert failing_body._released is False
    failing_response._release_error = None
    assert failing_body._release_response() is None
    assert failing_body._release_response() is None
    assert failing_body._response is None
    assert failing_body._released is True
    assert failing_response.release_count == 2


def test_response_body_iterator_partial_construction_releases_response():
    mod = _load_function_app()
    response = _FakeResponse([])
    construction_error = RuntimeError("iterator construction failed")

    class ExplodingContent:
        def iter_any(self):
            raise construction_error

    response.content = ExplodingContent()
    try:
        mod._ResponseBodyIterator(response)
    except RuntimeError as error:
        assert error is construction_error
    else:
        raise AssertionError("iterator construction must fail")
    del construction_error
    gc.collect()
    assert response.release_count == 1


def test_managed_mcp_contract_constants_match_ananke_authority():
    mod = _load_function_app()
    assert mod._MCP_INPUT_FIELD_ORDER == ("version", "protocolVersion", "message")
    assert mod._MCP_OUTPUT_FIELD_ORDER == ("version", "message")
    assert mod._MCP_PROTOCOL_VERSION == "2026-07-28"
    assert mod._MCP_ALLOWED_METHODS == frozenset(
        ("server/discover", "tools/list", "tools/call", "tasks/get", "tasks/cancel")
    )
    assert "tasks/result" not in mod._MCP_ALLOWED_METHODS
    assert mod._MCP_REQUEST_LIMIT_BYTES == 5 * 1024 * 1024
    assert mod._MCP_INVOKE_TIMEOUT_SECONDS == 300
    assert mod._MCP_VARIANTS == (
        "Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect"
    )


def test_managed_mcp_requires_exact_ordered_envelope_and_jsonrpc_request():
    mod = _load_function_app()
    valid = _managed_mcp_payload()
    prepared = mod._prepare_managed_mcp_request(valid)
    forwarded = mod.json.loads(prepared.body_bytes)
    assert tuple(forwarded) == ("jsonrpc", "id", "method", "params")
    assert forwarded["params"]["_meta"][
        "io.modelcontextprotocol/protocolVersion"
    ] == "2026-07-28"
    assert forwarded["params"]["_meta"][
        "io.modelcontextprotocol/clientCapabilities"
    ] == {"extensions": {"io.modelcontextprotocol/tasks": {}}}

    invalid = [
        dict(reversed(tuple(valid.items()))),
        {**valid, "extra": None},
        {**valid, "sessionId": "stateful"},
        {"transport": "mcp-streamable-http", **valid},
        _managed_mcp_payload("tasks/result", {"taskId": "task-1"}),
    ]
    nested_session = _managed_mcp_payload(
        "tools/call",
        {"name": "tool", "arguments": {"sessionId": "stateful"}},
    )
    invalid.append(nested_session)
    for payload in invalid:
        _assert_fixed_error(
            mod._McpContractError,
            "Invalid managed MCP request.",
            lambda payload=payload: mod._prepare_managed_mcp_request(payload),
        )


def test_managed_mcp_method_shapes_and_task_routing_are_stateless():
    mod = _load_function_app()
    cases = (
        ("server/discover", {}, None),
        ("tools/list", {}, None),
        ("tools/call", {"name": "PBICopilotAskPowerBI", "arguments": {}}, "PBICopilotAskPowerBI"),
        ("tasks/get", {"taskId": "task-123"}, "task-123"),
        ("tasks/cancel", {"taskId": "task-123"}, "task-123"),
    )
    for method, params, routing_name in cases:
        prepared = mod._prepare_managed_mcp_request(
            _managed_mcp_payload(method, params)
        )
        assert prepared.method == method
        assert prepared.routing_name == routing_name

    for method in ("tasks/get", "tasks/cancel"):
        for task_id in ("", "task\r\nInjected: value"):
            _assert_fixed_error(
                mod._McpContractError,
                "Invalid managed MCP request.",
                lambda method=method, task_id=task_id: mod._prepare_managed_mcp_request(
                    _managed_mcp_payload(method, {"taskId": task_id})
                ),
            )


def test_managed_mcp_owns_endpoint_auth_policy_and_returns_exact_envelope():
    request = _managed_mcp_payload(
        "tasks/get",
        {"taskId": "task-123"},
        request_id="request-1",
    )
    response_message = {
        "jsonrpc": "2.0",
        "id": "request-1",
        "result": {
            "taskId": "task-123",
            "status": "completed",
            "result": {"content": [], "isError": False},
        },
    }
    mod, result, response, session = asyncio.run(
        _call_managed_mcp(
            request,
            mod_json(response_message),
        )
    )
    assert tuple(result) == ("version", "message")
    assert result == {"version": 1, "message": response_message}
    assert response.release_count == 1
    assert len(session.calls) == 1
    url, body, headers, timeout = session.calls[0]
    assert url == "https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq"
    assert headers["Authorization"] == "Bearer secret-test-token"
    assert headers["MCP-Protocol-Version"] == "2026-07-28"
    assert headers["Mcp-Method"] == "tasks/get"
    assert headers["Mcp-Name"] == "task-123"
    assert headers["X-Variants"] == (
        "Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect"
    )
    assert "X-Variant" not in headers
    assert timeout.total == 300
    assert b"secret-test-token" not in body


def mod_json(value):
    import json

    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def test_managed_mcp_tasks_get_forwards_terminal_error_and_null_response():
    error_message = {
        "jsonrpc": "2.0",
        "id": 7,
        "error": {"code": -32001, "message": "task failed"},
    }
    _mod, result, response, _session = asyncio.run(
        _call_managed_mcp(
            _managed_mcp_payload("tasks/get", {"taskId": "task-1"}),
            mod_json(error_message),
            status=400,
        )
    )
    assert result == {"version": 1, "message": error_message}
    assert response.release_count == 1

    _mod, result, response, _session = asyncio.run(
        _call_managed_mcp(_managed_mcp_payload(), b"", status=204)
    )
    assert result == {"version": 1, "message": None}
    assert response.release_count == 1


def test_managed_mcp_limits_are_injected_and_fail_closed(monkeypatch=None):
    mod = _load_function_app()
    names = (
        "FABRIC_MCP_MAX_OUTPUT_BYTES",
        "FABRIC_MCP_MAX_JSON_DEPTH",
        "FABRIC_MCP_ALLOWED_STATUS_CODES",
        "FABRIC_MCP_TASK_STATUSES",
        "FABRIC_MCP_FINAL_TASK_STATUSES",
        "FABRIC_MCP_MAX_POLL_COUNT",
    )
    previous = {name: os.environ.pop(name, None) for name in names}
    try:
        _assert_fixed_error(
            mod._McpConfigurationError,
            "Managed MCP limits are not configured.",
            mod._load_managed_mcp_limits,
        )
        values = (
            "65536",
            "32",
            "200,204,400",
            "working,completed,failed,cancelled",
            "completed,failed,cancelled",
            "100",
        )
        for name, value in zip(names, values):
            os.environ[name] = value
        assert mod._load_managed_mcp_limits() == _managed_mcp_limits(mod)
    finally:
        for name in names:
            os.environ.pop(name, None)
            if previous[name] is not None:
                os.environ[name] = previous[name]


def test_managed_mcp_rejects_unknown_status_oversized_depth_and_session():
    mod = _load_function_app()
    limits = _managed_mcp_limits(mod)
    valid = mod_json({"jsonrpc": "2.0", "id": 7, "result": {}})
    response = _ManagedMcpResponse(valid, status=500)
    session = _ManagedMcpSession(response)
    try:
        asyncio.run(
            mod._invoke_managed_mcp(
                _managed_mcp_payload(),
                "token",
                limits,
                session,
            )
        )
    except mod._McpUpstreamError as error:
        assert str(error) == "Managed MCP upstream response is invalid."
    else:
        raise AssertionError("unknown HTTP status must fail closed")
    assert response.release_count == 1

    invalid_messages = (
        b"x" * (limits.max_output_bytes + 1),
        mod_json({"jsonrpc": "2.0", "id": 7, "result": {"sessionId": "bad"}}),
        mod_json({"jsonrpc": "2.0", "id": 7, "result": {"nested": [[]]}}),
    )
    shallow_limits = limits._replace(max_json_depth=3)
    prepared = mod._prepare_managed_mcp_request(_managed_mcp_payload())
    for body in invalid_messages:
        _assert_fixed_error(
            mod._McpUpstreamError,
            "Managed MCP upstream response is invalid.",
            lambda body=body: mod._validate_managed_mcp_response(
                body,
                prepared,
                7,
                shallow_limits,
            ),
        )


def test_managed_mcp_tasks_v2_enforces_task_continuity_status_and_shapes():
    mod = _load_function_app()
    limits = _managed_mcp_limits(mod)
    request = mod._prepare_managed_mcp_request(
        _managed_mcp_payload("tasks/get", {"taskId": "expected-task"})
    )

    valid = (
        {
            "jsonrpc": "2.0",
            "id": 7,
            "result": {"taskId": "expected-task", "status": "working"},
        },
        {
            "jsonrpc": "2.0",
            "id": 7,
            "result": {
                "taskId": "expected-task",
                "status": "completed",
                "result": {"content": [], "isError": False},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 7,
            "result": {
                "taskId": "expected-task",
                "status": "failed",
                "error": {"code": "TaskFailed"},
            },
        },
    )
    for message in valid:
        assert mod._validate_managed_mcp_response(
            mod_json(message), request, 7, limits
        ) == message

    invalid_results = (
        {"taskId": "attacker-task", "status": "working"},
        {"taskId": "expected-task", "status": "invented"},
        {
            "taskId": "expected-task",
            "status": "working",
            "result": {"premature": True},
        },
        {"taskId": "expected-task", "status": "completed"},
        {
            "taskId": "expected-task",
            "status": "completed",
            "result": {},
            "error": {},
        },
    )
    for result in invalid_results:
        _assert_fixed_error(
            mod._McpUpstreamError,
            "Managed MCP upstream response is invalid.",
            lambda result=result: mod._validate_managed_mcp_response(
                mod_json({"jsonrpc": "2.0", "id": 7, "result": result}),
                request,
                7,
                limits,
            ),
        )


def test_managed_mcp_cancel_accepts_authoritative_completion_envelope():
    mod = _load_function_app()
    request = mod._prepare_managed_mcp_request(
        _managed_mcp_payload("tasks/cancel", {"taskId": "task-123"})
    )
    message = {
        "jsonrpc": "2.0",
        "id": 7,
        "result": {"resultType": "complete"},
    }
    assert mod._validate_managed_mcp_response(
        mod_json(message),
        request,
        7,
        _managed_mcp_limits(mod),
    ) == message


def test_managed_mcp_tools_call_result_type_requires_valid_task_handle():
    mod = _load_function_app()
    request = mod._prepare_managed_mcp_request(
        _managed_mcp_payload(
            "tools/call",
            {"name": "tool", "arguments": {}},
        )
    )
    limits = _managed_mcp_limits(mod)
    synchronous = {
        "jsonrpc": "2.0",
        "id": 7,
        "result": {"content": [], "isError": False},
    }
    task = {
        "jsonrpc": "2.0",
        "id": 7,
        "result": {
            "resultType": "task",
            "taskId": "task-123",
            "status": "working",
        },
    }
    for message in (synchronous, task):
        assert mod._validate_managed_mcp_response(
            mod_json(message), request, 7, limits
        ) == message

    malformed_results = (
        {"resultType": "task"},
        {"resultType": "invented"},
        {"resultType": "complete"},
        {"taskId": "task-123", "status": "working"},
    )
    for result in malformed_results:
        _assert_fixed_error(
            mod._McpUpstreamError,
            "Managed MCP upstream response is invalid.",
            lambda result=result: mod._validate_managed_mcp_response(
                mod_json({"jsonrpc": "2.0", "id": 7, "result": result}),
                request,
                7,
                limits,
            ),
        )


def test_managed_mcp_deep_json_uses_fixed_content_free_errors():
    mod = _load_function_app()
    depth = sys.getrecursionlimit() * 4
    nested = {}
    for _index in range(depth):
        nested = {"nested": nested}
    deep_request = _managed_mcp_payload(
        "tools/call",
        {"name": "tool", "arguments": nested},
    )
    _assert_fixed_error(
        mod._McpContractError,
        "Invalid managed MCP request.",
        lambda: mod._prepare_managed_mcp_request(deep_request),
    )

    prepared = mod._prepare_managed_mcp_request(_managed_mcp_payload())
    deep_response = (
        b'{"jsonrpc":"2.0","id":7,"result":'
        + b"[" * depth
        + b"0"
        + b"]" * depth
        + b"}"
    )
    _assert_fixed_error(
        mod._McpUpstreamError,
        "Managed MCP upstream response is invalid.",
        lambda: mod._validate_managed_mcp_response(
            deep_response,
            prepared,
            7,
            _managed_mcp_limits(mod)._replace(max_json_depth=depth + 10),
        ),
    )


def test_managed_mcp_release_failure_retries_without_losing_pool_ownership():
    mod = _load_function_app()
    message = {"jsonrpc": "2.0", "id": 7, "result": {}}
    response = _ManagedMcpResponse(mod_json(message), release_failures=1)
    session = _ManagedMcpSession(response)
    try:
        asyncio.run(
            mod._invoke_managed_mcp(
                _managed_mcp_payload(),
                "token",
                _managed_mcp_limits(mod),
                session,
            )
        )
    except RuntimeError as error:
        assert str(error) == "managed release failed"
    else:
        raise AssertionError("the original release failure must propagate")
    assert response.release_count == 2
    assert response.close_count == 0

    close_response = _ManagedMcpResponse(mod_json(message), release_failures=2)
    close_session = _ManagedMcpSession(close_response)
    try:
        asyncio.run(
            mod._invoke_managed_mcp(
                _managed_mcp_payload(),
                "token",
                _managed_mcp_limits(mod),
                close_session,
            )
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("release failure must propagate after close fallback")
    assert close_response.release_count == 2
    assert close_response.close_count == 1


def legacy_mcp_contract_constants_match_authoritative_t1():
    mod = _load_function_app()
    assert mod._MCP_T1_FIELD_ORDER == (
        "transport",
        "version",
        "method",
        "protocolVersion",
        "headers",
        "body",
        "serverPolicy",
    )
    assert mod._MCP_T1_FIXED_VALUES == (
        "mcp-streamable-http",
        1,
        "POST",
        "2025-11-25",
    )
    assert mod._MCP_DENIED_HEADER_NAMES == frozenset(
        {
            "authorization",
            "baggage",
            "connection",
            "content-length",
            "cookie",
            "forwarded",
            "host",
            "keep-alive",
            "mcp-session-id",
            "origin",
            "proxy-authenticate",
            "proxy-authorization",
            "proxy-connection",
            "te",
            "traceparent",
            "tracestate",
            "trailer",
            "transfer-encoding",
            "upgrade",
        }
    )
    assert mod._MCP_DENIED_HEADER_PREFIXES == (
        "x-forwarded-",
        "x-ms-",
        "x-rayfin-",
    )


def legacy_mcp_valid_envelope_preserves_bytes_and_is_deeply_immutable():
    mod = _load_function_app()
    values = [" first ", "a,b", "Duplicate", "Duplicate"]
    headers = {"accept": values, "x-empty": []}
    body = '{\n  "escaped": "\\u0061", "text": "café"\n}'

    pending = mod._extract_pending_mcp_t1_transport(
        _valid_mcp_envelope(headers=headers, body=body)
    )

    assert type(pending) is mod._PendingMcpT1Transport
    assert pending == mod._PendingMcpT1Transport(
        "mcp-streamable-http",
        1,
        "POST",
        "2025-11-25",
        (
            ("accept", (" first ", "a,b", "Duplicate", "Duplicate")),
            ("x-empty", ()),
        ),
        body.encode("utf-8"),
    )
    values[0] = "mutated"
    values.append("later")
    headers["after"] = ["mutation"]
    assert pending.headers == (
        ("accept", (" first ", "a,b", "Duplicate", "Duplicate")),
        ("x-empty", ()),
    )
    mutation_was_rejected = False
    try:
        pending.headers[0][1][0] = "nope"
    except TypeError:
        mutation_was_rejected = True
    assert mutation_was_rejected, "nested header values must be immutable tuples"


def legacy_mcp_body_is_not_parsed_or_normalized():
    mod = _load_function_app()
    for body in ("", "{not-json", ' { "n": 1e+00, "s": "\\u0061" } '):
        pending = mod._extract_pending_mcp_t1_transport(
            _valid_mcp_envelope(body=body)
        )
        assert pending.body_bytes == body.encode("utf-8")


def legacy_mcp_body_limit_is_inclusive_and_uses_encoded_bytes():
    mod = _load_function_app()
    maximum = 5 * 1024 * 1024
    at_limit = "é" * (maximum // 2)
    pending = mod._extract_pending_mcp_t1_transport(
        _valid_mcp_envelope(body=at_limit)
    )
    assert len(pending.body_bytes) == maximum

    _assert_fixed_error(
        mod._McpTransportContractError,
        "Invalid MCP transport envelope.",
        lambda: mod._extract_pending_mcp_t1_transport(
            _valid_mcp_envelope(body=at_limit + "a")
        ),
    )


def legacy_mcp_oversized_ascii_body_is_rejected_before_encoding_allocation():
    mod = _load_function_app()
    body = "a" * (64 * 1024 * 1024)
    envelope = _valid_mcp_envelope(body=body)

    tracemalloc.start()
    try:
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda: mod._extract_pending_mcp_t1_transport(envelope),
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < 1024 * 1024, "oversized ASCII must be rejected before UTF-8 copy"


def legacy_mcp_invalid_body_type_and_surrogate_raise_fixed_contract_error():
    mod = _load_function_app()
    for body in (b"{}", _ExplodingStr("customer-secret"), "\ud800"):
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda body=body: mod._extract_pending_mcp_t1_transport(
                _valid_mcp_envelope(body=body)
            ),
        )


def legacy_mcp_envelope_requires_exact_dict_and_ordered_builtin_string_keys():
    mod = _load_function_app()
    valid = _valid_mcp_envelope()
    cases = [
        None,
        [],
        _ExplodingDict(valid),
        {key: value for key, value in list(valid.items())[1:]},
        {**valid, "extra": None},
        dict(reversed(list(valid.items()))),
    ]
    hostile_key = {
        _ExplodingStr("TRANSPORT"): "mcp-streamable-http",
        **dict(list(valid.items())[1:]),
    }
    cases.append(hostile_key)

    for envelope in cases:
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda envelope=envelope: mod._extract_pending_mcp_t1_transport(envelope),
        )


def legacy_mcp_fixed_fields_require_exact_types_and_values():
    mod = _load_function_app()
    invalid_values = {
        "transport": ("MCP-streamable-http", _ExplodingStr("mcp-streamable-http")),
        "version": (True, 1.0, 2),
        "method": ("post", _ExplodingStr("POST")),
        "protocolVersion": ("2025-03-26", _ExplodingStr("2025-11-25")),
    }
    for field, values in invalid_values.items():
        for value in values:
            envelope = _valid_mcp_envelope()
            envelope[field] = value
            _assert_fixed_error(
                mod._McpTransportContractError,
                "Invalid MCP transport envelope.",
                lambda envelope=envelope: mod._extract_pending_mcp_t1_transport(
                    envelope
                ),
            )


def legacy_mcp_header_names_require_sorted_lowercase_ascii_tokens():
    mod = _load_function_app()
    pending = mod._extract_pending_mcp_t1_transport(
        _valid_mcp_envelope(headers={"---": [], "123": [], "a": []})
    )
    assert tuple(name for name, _values in pending.headers) == ("---", "123", "a")

    invalid_headers = (
        [],
        _ExplodingDict(),
        {"z": [], "a": []},
        {"Upper": []},
        {"": []},
        {"é": []},
        {"has space": []},
        {_ExplodingStr("safe"): []},
    )
    for headers in invalid_headers:
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda headers=headers: mod._extract_pending_mcp_t1_transport(
                _valid_mcp_envelope(headers=headers)
            ),
        )


def legacy_mcp_header_order_validation_is_linear_without_sorting():
    mod = _load_function_app()
    source_tree = ast.parse(inspect.getsource(mod._extract_pending_mcp_t1_transport))
    sorting_calls = [
        node
        for node in ast.walk(source_tree)
        if isinstance(node, ast.Call)
        and (
            isinstance(node.func, ast.Name)
            and node.func.id == "sorted"
            or isinstance(node.func, ast.Attribute)
            and node.func.attr in ("sort", "sorted")
        )
    ]
    assert sorting_calls == [], "header order validation must not sort or copy-sort"

    header_count = 50_000
    names = [f"h{index:07x}" for index in range(header_count)]
    names[0], names[1] = names[1], names[0]
    headers = {name: [] for name in names}
    original_is_token = mod._is_ascii_http_token
    names_validated = 0

    def counting_is_token(value):
        nonlocal names_validated
        names_validated += 1
        return original_is_token(value)

    mod._is_ascii_http_token = counting_is_token
    try:
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda: mod._extract_pending_mcp_t1_transport(
                _valid_mcp_envelope(headers=headers)
            ),
        )
    finally:
        mod._is_ascii_http_token = original_is_token

    assert names_validated == header_count


def legacy_mcp_header_values_require_exact_lists_of_exact_strings():
    mod = _load_function_app()
    for headers in (
        {"safe": ()},
        {"safe": _ExplodingList(["value"])},
        {"safe": [1]},
        {"safe": [_ExplodingStr("value")]},
    ):
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda headers=headers: mod._extract_pending_mcp_t1_transport(
                _valid_mcp_envelope(headers=headers)
            ),
        )


def legacy_mcp_static_denials_are_case_insensitive_and_all_enforced():
    mod = _load_function_app()
    assert mod._is_mcp_denied_header_name(1) is False
    assert len(mod._MCP_DENIED_HEADER_NAMES) == 19
    for name in mod._MCP_DENIED_HEADER_NAMES:
        mixed_case = "".join(
            character.upper() if index % 2 else character
            for index, character in enumerate(name)
        )
        assert mod._is_mcp_denied_header_name(mixed_case) is True
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda name=name: mod._extract_pending_mcp_t1_transport(
                _valid_mcp_envelope(headers={name: []})
            ),
        )
    assert mod._is_mcp_denied_header_name("x-safe") is False


def legacy_mcp_prefix_denials_are_case_insensitive_and_all_enforced():
    mod = _load_function_app()
    for prefix in mod._MCP_DENIED_HEADER_PREFIXES:
        mixed_case = prefix.upper() + "customer"
        assert mod._is_mcp_denied_header_name(mixed_case) is True
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda prefix=prefix: mod._extract_pending_mcp_t1_transport(
                _valid_mcp_envelope(headers={prefix + "customer": []})
            ),
        )


def legacy_mcp_protocol_version_header_must_exactly_match_envelope_version():
    mod = _load_function_app()
    pending = mod._extract_pending_mcp_t1_transport(
        _valid_mcp_envelope(
            headers={"mcp-protocol-version": ["2025-11-25"]}
        )
    )
    assert pending.headers == (
        ("mcp-protocol-version", ("2025-11-25",)),
    )
    for values in ([], ["2025-03-26"], ["2025-11-25", "2025-11-25"]):
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda values=values: mod._extract_pending_mcp_t1_transport(
                _valid_mcp_envelope(headers={"mcp-protocol-version": values})
            ),
        )


def legacy_mcp_connection_nomination_helpers_are_independently_enforced():
    mod = _load_function_app()
    headers = (
        ("Connection", (" keep-alive,\tX-Hop ",)),
        ("X-Hop", ("customer-value",)),
    )
    assert mod._mcp_connection_nominations(headers) == ("keep-alive", "x-hop")
    assert mod._has_mcp_connection_nominated_header(headers) is True
    assert mod._has_mcp_connection_nominated_header(
        (("Connection", ("keep-alive",)), ("x-safe", ("value",)))
    ) is False

    for value in ("", "keep-alive,,x-hop", "x hop", "x-hop,\vnext"):
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda value=value: mod._mcp_connection_nominations(
                (("Connection", (value,)),)
            ),
        )
    for headers in (
        [],
        (["connection", ("x-hop",)],),
        (("connection", ["x-hop"]),),
        (("connection", (1,)),),
    ):
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda headers=headers: mod._mcp_connection_nominations(headers),
        )


def legacy_mcp_extractor_rejects_connection_and_nominated_supplied_headers():
    mod = _load_function_app()
    for headers in (
        {"connection": ["keep-alive, x-hop"], "x-hop": ["value"]},
        {"connection": ["keep-alive,,x-hop"], "x-hop": ["value"]},
    ):
        _assert_fixed_error(
            mod._McpTransportContractError,
            "Invalid MCP transport envelope.",
            lambda headers=headers: mod._extract_pending_mcp_t1_transport(
                _valid_mcp_envelope(headers=headers)
            ),
        )


def legacy_mcp_extractor_never_inspects_or_retains_server_policy():
    mod = _load_function_app()
    for policy in (False, 0, {}, [], _ExplodingPolicy()):
        _assert_fixed_error(
            mod._McpServerPolicyUnresolved,
            "MCP server policy is unresolved.",
            lambda policy=policy: mod._extract_pending_mcp_t1_transport(
                _valid_mcp_envelope(server_policy=policy)
            ),
        )

    pending = mod._extract_pending_mcp_t1_transport(_valid_mcp_envelope())
    assert not hasattr(pending, "server_policy")
    assert not hasattr(pending, "url")
    assert not hasattr(pending, "credential")
    assert not hasattr(pending, "send")

    mixed_invalid = _valid_mcp_envelope(server_policy=_ExplodingPolicy())
    mixed_invalid["version"] = 2
    _assert_fixed_error(
        mod._McpServerPolicyUnresolved,
        "MCP server policy is unresolved.",
        lambda: mod._extract_pending_mcp_t1_transport(mixed_invalid),
    )


def legacy_mcp_candidate_contract_constants_match_authoritative_t2_handoff():
    mod = _load_function_app()
    assert mod._MCP_T2_CANDIDATE_EVIDENCE == (
        "ananke:454357b4696d5a8669596209ed88bf10daeb0844",
    )
    assert mod._MCP_T2_CANDIDATE_CONTRACT == mod._McpT2CandidateContract(
        "ananke:454357b4696d5a8669596209ed88bf10daeb0844",
        "fabriciq",
        "https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq",
        (
            (
                "X-Variant",
                "Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect",
            ),
        ),
    )


def legacy_mcp_prepare_accepts_exact_t2_policy_and_snapshots_immutable_transport():
    mod = _load_function_app()
    policy = _valid_mcp_server_policy()
    policy["protectedHeaders"]["X-Variant"] = (
        "Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect"
    )
    body = (
        '{"jsonrpc":"2.0","serverPolicy":{"url":"https://evil.invalid"},'
        '"destination":"https://evil.invalid"}'
    )
    envelope = _valid_mcp_envelope(
        headers={"accept": ["application/json"], "x-empty": []},
        body=body,
        server_policy=dict(reversed(tuple(policy.items()))),
    )

    prepared = mod._prepare_mcp_transport(envelope)

    assert prepared == mod._PreparedMcpTransport(
        "ananke:454357b4696d5a8669596209ed88bf10daeb0844",
        "https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq",
        (
            ("accept", ("application/json",)),
            ("x-empty", ()),
            (
                "X-Variant",
                ("Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect",),
            ),
        ),
        body.encode("utf-8"),
    )
    policy["url"] = "https://evil.invalid"
    policy["protectedHeaders"]["X-Variant"] = "evil"
    envelope["headers"]["accept"].append("text/event-stream")
    envelope["body"] = "mutated"
    assert prepared.endpoint == "https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq"
    assert prepared.headers[0][1] == ("application/json",)
    assert prepared.body_bytes == body.encode("utf-8")
    envelope["serverPolicy"]["id"] = "mutated"
    envelope["serverPolicy"]["url"] = "https://evil.invalid"
    envelope["serverPolicy"]["protectedHeaders"]["X-Variant"] = "mutated"
    assert prepared.endpoint == "https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq"
    assert prepared.headers[-1] == (
        "X-Variant",
        ("Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect",),
    )
    assert not hasattr(prepared, "credential")
    assert not hasattr(prepared, "send")
    assert not hasattr(prepared, "session")


def legacy_mcp_policy_shape_and_literals_fail_closed():
    mod = _load_function_app()

    class DictSubclass(dict):
        pass

    valid = _valid_mcp_server_policy()
    cases = [
        False,
        0,
        [],
        {},
        DictSubclass(valid),
        *[
            {key: value for key, value in valid.items() if key != missing}
            for missing in ("id", "url", "protectedHeaders")
        ],
        {**valid, "extra": "value"},
        {
            "Id": valid["id"],
            "url": valid["url"],
            "protectedHeaders": valid["protectedHeaders"],
        },
        {**valid, "id": ""},
        {**valid, "id": _ExplodingStr("fabriciq")},
        {**valid, "id": "fabric-iq"},
        {**valid, "url": ""},
        {**valid, "url": _ExplodingStr(valid["url"])},
        {**valid, "url": "https://FABRICIQ.svc.cloud.microsoft/v1/mcp/fabriciq"},
        {**valid, "url": valid["url"] + "/"},
        {**valid, "url": valid["url"] + "?query=1"},
        {**valid, "url": valid["url"] + "#fragment"},
        {**valid, "protectedHeaders": []},
        {**valid, "protectedHeaders": {}},
        {**valid, "protectedHeaders": DictSubclass(valid["protectedHeaders"])},
        {
            **valid,
            "protectedHeaders": {
                _ExplodingStr("X-Variant"): valid["protectedHeaders"]["X-Variant"]
            },
        },
        {**valid, "protectedHeaders": {"x-variant": valid["protectedHeaders"]["X-Variant"]}},
        {
            **valid,
            "protectedHeaders": {
                **valid["protectedHeaders"],
                "X-Other": "value",
            },
        },
        {**valid, "protectedHeaders": {"X-Variant": ""}},
        {
            **valid,
            "protectedHeaders": {
                "X-Variant": _ExplodingStr(
                    "Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect"
                )
            },
        },
        {**valid, "protectedHeaders": {"X-Variant": "different"}},
    ]
    for policy in cases:
        _assert_fixed_error(
            mod._McpServerPolicyInvalid,
            "Invalid MCP server policy.",
            lambda policy=policy: mod._prepare_mcp_transport(
                _valid_mcp_envelope(server_policy=policy)
            ),
        )


def legacy_mcp_candidate_source_version_mismatch_fails_closed():
    mod = _load_function_app()
    original_contract = mod._MCP_T2_CANDIDATE_CONTRACT
    mod._MCP_T2_CANDIDATE_CONTRACT = original_contract._replace(
        source_version="project-rayfin:unpublished"
    )
    try:
        _assert_fixed_error(
            mod._McpServerPolicyInvalid,
            "Invalid MCP server policy.",
            lambda: mod._prepare_mcp_transport(
                _valid_mcp_envelope(server_policy=_valid_mcp_server_policy())
            ),
        )
    finally:
        mod._MCP_T2_CANDIDATE_CONTRACT = original_contract


def legacy_mcp_candidate_contract_fields_cannot_spoof_equality_or_iteration():
    mod = _load_function_app()
    original_contract = mod._MCP_T2_CANDIDATE_CONTRACT
    spoofed_contracts = (
        original_contract._replace(
            source_version=_ExplodingStr(original_contract.source_version)
        ),
        original_contract._replace(
            profile_id=_ExplodingStr(original_contract.profile_id)
        ),
        original_contract._replace(endpoint=_ExplodingStr(original_contract.endpoint)),
        original_contract._replace(protected_headers=_SpoofedProtectedHeaders()),
        original_contract._replace(
            protected_headers=(
                [
                    "X-Variant",
                    "Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect",
                ],
            )
        ),
    )
    try:
        for spoofed_contract in spoofed_contracts:
            mod._MCP_T2_CANDIDATE_CONTRACT = spoofed_contract
            _assert_fixed_error(
                mod._McpServerPolicyInvalid,
                "Invalid MCP server policy.",
                lambda: mod._prepare_mcp_transport(
                    _valid_mcp_envelope(server_policy=_valid_mcp_server_policy())
                ),
            )
    finally:
        mod._MCP_T2_CANDIDATE_CONTRACT = original_contract


def legacy_mcp_policy_resolution_never_rereads_live_outer_policy():
    mod = _load_function_app()
    source_tree = ast.parse(inspect.getsource(mod._resolve_mcp_t2_candidate_policy))
    live_subscripts = [
        node
        for node in ast.walk(source_tree)
        if isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == "server_policy"
    ]
    item_snapshots = [
        node
        for node in ast.walk(source_tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "server_policy"
        and node.attr == "items"
    ]
    assert live_subscripts == []
    assert len(item_snapshots) == 1


def legacy_mcp_concurrent_policy_mutation_has_only_fixed_failure_or_snapshot():
    mod = _load_function_app()
    policy = _valid_mcp_server_policy()
    stop = threading.Event()

    def mutate():
        while not stop.is_set():
            policy.pop("id", None)
            policy["id"] = "fabriciq"

    previous_interval = sys.getswitchinterval()
    sys.setswitchinterval(0.000001)
    mutator = threading.Thread(target=mutate)
    mutator.start()
    try:
        for _attempt in range(2_000):
            try:
                prepared = mod._prepare_mcp_transport(
                    _valid_mcp_envelope(server_policy=policy)
                )
            except mod._McpServerPolicyInvalid as error:
                assert str(error) == "Invalid MCP server policy."
            else:
                assert prepared.endpoint == (
                    "https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq"
                )
    finally:
        stop.set()
        mutator.join()
        sys.setswitchinterval(previous_interval)


def legacy_mcp_protected_header_collision_is_rejected_not_overwritten():
    mod = _load_function_app()
    _assert_fixed_error(
        mod._McpServerPolicyInvalid,
        "Invalid MCP server policy.",
        lambda: mod._prepare_mcp_transport(
            _valid_mcp_envelope(
                headers={"x-variant": ["caller-value"]},
                server_policy=_valid_mcp_server_policy(),
            )
        ),
    )


def legacy_mcp_policy_resolution_and_preparation_perform_no_io_or_logging():
    mod = _load_function_app()
    records = []

    class CaptureHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("provisional MCP preparation must not perform I/O")

    root_logger = logging.getLogger()
    previous_level = root_logger.level
    handler = CaptureHandler()
    original_get_session = mod._get_session
    original_client_session = mod.aiohttp.ClientSession
    stdout = io.StringIO()
    stderr = io.StringIO()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)
    mod._get_session = fail_if_called
    mod.aiohttp.ClientSession = fail_if_called
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            prepared = mod._prepare_mcp_transport(
                _valid_mcp_envelope(server_policy=_valid_mcp_server_policy())
            )
            assert prepared.endpoint.endswith("/v1/mcp/fabriciq")
            _assert_fixed_error(
                mod._McpServerPolicyInvalid,
                "Invalid MCP server policy.",
                lambda: mod._prepare_mcp_transport(
                    _valid_mcp_envelope(
                        server_policy={
                            **_valid_mcp_server_policy(),
                            "url": "https://customer-secret.invalid",
                        }
                    )
                ),
            )
    finally:
        mod._get_session = original_get_session
        mod.aiohttp.ClientSession = original_client_session
        root_logger.removeHandler(handler)
        root_logger.setLevel(previous_level)
    assert records == []
    assert stdout.getvalue() == ""
    assert stderr.getvalue() == ""
    assert not hasattr(mod, "_send_mcp_transport")


def legacy_mcp_prepare_remains_synchronous_and_null_policy_fails_closed():
    mod = _load_function_app()
    assert inspect.iscoroutinefunction(mod._extract_pending_mcp_t1_transport) is False
    assert inspect.iscoroutinefunction(mod._prepare_mcp_transport) is False
    _assert_fixed_error(
        mod._McpServerPolicyUnresolved,
        "MCP server policy is unresolved.",
        lambda: mod._prepare_mcp_transport(_valid_mcp_envelope()),
    )
    _assert_fixed_error(
        mod._McpTransportContractError,
        "Invalid MCP transport envelope.",
        lambda: mod._prepare_mcp_transport(None),
    )


def legacy_mcp_errors_are_fixed_and_do_not_leak_customer_canaries():
    mod = _load_function_app()
    canaries = ("customer-secret-body", "customer-secret-header")
    cases = (
        _valid_mcp_envelope(body="\ud800customer-secret-body"),
        _valid_mcp_envelope(headers={"safe": ["customer-secret-header", 1]}),
    )

    records = []

    class CaptureHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("transport validation must not perform I/O")

    stdout = io.StringIO()
    stderr = io.StringIO()
    root_logger = logging.getLogger()
    previous_level = root_logger.level
    handler = CaptureHandler()
    original_get_session = mod._get_session
    original_client_session = mod.aiohttp.ClientSession
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)
    mod._get_session = fail_if_called
    mod.aiohttp.ClientSession = fail_if_called
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            for envelope in cases:
                error = _assert_fixed_error(
                    mod._McpTransportContractError,
                    "Invalid MCP transport envelope.",
                    lambda envelope=envelope: mod._extract_pending_mcp_t1_transport(
                        envelope
                    ),
                )
                assert all(canary not in str(error) for canary in canaries)
    finally:
        mod._get_session = original_get_session
        mod.aiohttp.ClientSession = original_client_session
        root_logger.removeHandler(handler)
        root_logger.setLevel(previous_level)

    assert stdout.getvalue() == ""
    assert stderr.getvalue() == ""
    assert records == []


def test_connector_archives_match_source_metadata_and_each_other():
    root = Path(__file__).resolve().parent

    def normalize_line_endings(content):
        return content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")

    expected_members = {
        "function_app.py": normalize_line_endings(
            (root / "function_app.py").read_bytes()
        ),
        "fabric_lib/functions.metadata": normalize_line_endings(
            (root / "functions.metadata").read_bytes()
        ),
    }
    archive_paths = (root / "Deploy.zip", root / "SourceCode.zip")
    archive_bytes = []
    member_names = []
    for archive_path in archive_paths:
        archive_bytes.append(archive_path.read_bytes())
        with zipfile.ZipFile(archive_path) as archive:
            assert archive.testzip() is None
            assert archive.comment == b""
            member_names.append(tuple(info.filename for info in archive.infolist()))
            non_target_names = tuple(
                info.filename
                for info in archive.infolist()
                if info.filename != "function_app.py"
            )
            assert non_target_names == tuple(_ARCHIVE_NON_TARGET_GOLDEN)
            for info in archive.infolist():
                assert info.create_system == 0
                assert info.external_attr == 0
                common_info = (
                    info.date_time,
                    info.compress_type,
                    info.comment,
                    info.extra,
                    info.create_system,
                    info.create_version,
                    info.extract_version,
                    info.reserved,
                    info.flag_bits,
                    info.volume,
                    info.internal_attr,
                    info.external_attr,
                )
                assert common_info == _ARCHIVE_COMMON_ZIP_INFO
                if info.filename == "function_app.py":
                    continue
                content = archive.read(info.filename)
                assert (
                    sha256(content).hexdigest(),
                    info.CRC,
                    info.compress_size,
                    info.file_size,
                ) == _ARCHIVE_NON_TARGET_GOLDEN[info.filename]
            for member_name, expected_content in expected_members.items():
                assert normalize_line_endings(archive.read(member_name)) == expected_content

    assert archive_bytes[0] == archive_bytes[1]
    assert member_names[0] == member_names[1]


def test_existing_udf_symbols_remain_and_managed_mcp_udf_is_present():
    mod = _load_function_app()
    assert hasattr(mod, "rayfin_semantic_model_v1")
    assert hasattr(mod, "rayfin_kusto_v1")
    assert hasattr(mod, "rayfin_fabric_mcp_v1")


if __name__ == "__main__":
    tests = [
        value
        for name, value in tuple(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"  ok: {test.__name__}")
    print(f"ALL {len(tests)} TESTS PASSED")
