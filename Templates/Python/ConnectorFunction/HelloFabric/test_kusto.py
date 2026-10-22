"""Isolated Kusto promotion tests using the real Fabric SDK response and binding APIs."""

import asyncio
import importlib.util
import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import aiohttp
import fabric.functions as fn
import pytest


ROOT = Path(__file__).parent


def load_app():
    spec = importlib.util.spec_from_file_location(
        "kusto_test_app", ROOT / "function_app.py"
    )
    app = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(app)
    return app


@pytest.fixture(autouse=True)
def no_live_requests(monkeypatch):
    def unexpected(*_args, **_kwargs):
        pytest.fail("Live HTTP and token requests are not allowed.")

    monkeypatch.setattr(aiohttp.ClientSession, "_request", unexpected)
    monkeypatch.setattr(fn.FabricItem, "get_access_token", unexpected)


@pytest.fixture
def app(monkeypatch):
    # Bypass host invocation, but keep the real StreamResponse implementation.
    monkeypatch.setattr(fn, "UserDataFunctions", lambda: SimpleNamespace(
        generic_connection=lambda **kwargs: lambda function: function,
        streaming_function=lambda: lambda function: function,
    ))
    monkeypatch.delenv("POWERBI_API_BASE", raising=False)
    return load_app()


def payload(operation="executeQuery", **fields):
    request = {
        "input": {
            "queryServiceUri": "https://cluster.kusto.fabric.microsoft.com/",
            "databaseName": "db",
            "clientRequestId": "KPC.rayfin_kusto_v1;test-id",
            **fields,
        },
    }
    if operation is not None:
        request["operation"] = operation
    return request


class KustoClient:
    def __init__(self):
        self.credential_calls = 0
        self.token_calls = 0

    def get_access_token(self):
        self.credential_calls += 1
        return self

    def get_token(self):
        self.token_calls += 1
        return SimpleNamespace(token="synthetic-kusto-token")


class Response:
    def __init__(self, chunks=None, status=200, headers=None):
        self.chunks = chunks if chunks is not None else (
            b'{"Tables":[', b'{"Rows":[[1]]}', b"]}",
        )
        self.status = status
        self.headers = headers if headers is not None else {}
        self.content = self
        self.chunk_reads = 0
        self.text_calls = 0
        self.release_calls = 0
        self.detail = "synthetic upstream error: \u00e9"

    async def iter_any(self):
        for chunk in self.chunks:
            self.chunk_reads += 1
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk

    async def text(self):
        self.text_calls += 1
        return self.detail

    async def release(self):
        self.release_calls += 1


class Session:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.closed = False

    async def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response

    async def close(self):
        pytest.fail("The shared session must not close after an invocation.")


def bind_session(app, monkeypatch, response):
    session = Session(response)

    async def get_session():
        return session

    monkeypatch.setattr(app, "_get_session", get_session)
    return session


async def invoke(app, request, client):
    result = await app.rayfin_kusto_v1(request, client)
    return result, [chunk async for chunk in result.content]


@pytest.mark.parametrize("operation, fields, verb, csl", [
    ("executeQuery", {"query": "T | take 1", "command": ".show tables"},
     "query", "T | take 1"),
    ("executeCommand", {"command": ".show tables", "query": "T | take 1"},
     "mgmt", ".show tables"),
    (None, {"query": "T | take 1"}, "query", "T | take 1"),
    ("", {"query": "T | take 1"}, "query", "T | take 1"),
    ("  executeCommand  ", {"command": ".show tables"}, "mgmt", ".show tables"),
    ("executeQuery", {"command": "T | take 1"}, "query", "T | take 1"),
    ("executeCommand", {"query": ".show tables"}, "mgmt", ".show tables"),
    ("executeQuery", {"query": "", "command": "T | take 1"}, "query", "T | take 1"),
    ("executeCommand", {"command": "", "query": ".show tables"}, "mgmt", ".show tables"),
    ("legacyOperation", {"query": "T | take 1"}, "query", "T | take 1"),
])
def test_query_and_command_routing(app, monkeypatch, operation, fields, verb, csl):
    request = payload(operation, **fields)
    before = json.dumps(request)
    response = Response()
    session = bind_session(app, monkeypatch, response)
    client = KustoClient()

    result, chunks = asyncio.run(invoke(app, request, client))

    assert isinstance(result, fn.StreamResponse)
    assert result.status_code == 200
    assert result.media_type == "application/json"
    assert chunks == list(response.chunks)
    assert session.calls == [(
        f"https://cluster.kusto.fabric.microsoft.com/v1/rest/{verb}",
        {
            "json": {"db": "db", "csl": csl, "properties": {}},
            "headers": {
                "Authorization": "Bearer synthetic-kusto-token",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "x-ms-client-request-id": "KPC.rayfin_kusto_v1;test-id",
            },
        },
    )]
    assert client.credential_calls == client.token_calls == 1
    assert response.text_calls == 0
    assert response.release_calls == 1
    assert session.closed is False
    assert json.dumps(request) == before


def test_stream_starts_before_body_is_read(app, monkeypatch):
    response = Response(chunks=(b"\x00first", b"\xffsecond", b"third"))
    bind_session(app, monkeypatch, response)

    async def check():
        result = await app.rayfin_kusto_v1(payload(query="T | take 1"), KustoClient())
        assert response.chunk_reads == response.release_calls == 0
        stream = result.content
        first = await stream.__anext__()
        assert response.chunk_reads == 1
        assert response.release_calls == 0
        return [first] + [chunk async for chunk in stream]

    assert asyncio.run(check()) == list(response.chunks)
    assert response.text_calls == 0
    assert response.release_calls == 1


@pytest.mark.parametrize("ending", ["close", "error", "cancel"])
def test_interrupted_stream_releases_response(app, monkeypatch, ending):
    failure = (asyncio.CancelledError() if ending == "cancel"
               else aiohttp.ClientPayloadError("synthetic stream failure"))
    response = Response(chunks=(b"first", failure))
    bind_session(app, monkeypatch, response)

    async def check():
        result = await app.rayfin_kusto_v1(payload(query="T | take 1"), KustoClient())
        stream = result.content
        assert await stream.__anext__() == b"first"
        if ending == "close":
            await stream.aclose()
        else:
            with pytest.raises(type(failure)):
                await stream.__anext__()

    asyncio.run(check())
    assert response.text_calls == 0
    assert response.release_calls == 1


@pytest.mark.parametrize("operation", ["executeQuery", "executeCommand"])
@pytest.mark.parametrize("status", [201, 302, 400, 401, 403, 429, 500])
@pytest.mark.parametrize("content_type", [None, "text/plain; charset=utf-8"])
def test_non_200_response_preserves_status_body_and_type(
    app, monkeypatch, operation, status, content_type
):
    headers = {} if content_type is None else {"Content-Type": content_type}
    response = Response(status=status, headers=headers)
    bind_session(app, monkeypatch, response)

    result = asyncio.run(app.rayfin_kusto_v1(
        payload(operation, query="T | take 1", command=".show tables"), KustoClient()
    ))

    assert result.status_code == status
    assert result.media_type == (content_type or "application/json")
    assert b"".join(result.content) == response.detail.encode("utf-8")
    assert response.text_calls == response.release_calls == 1
    assert response.chunk_reads == 0


@pytest.mark.parametrize("field", ["queryServiceUri", "databaseName", "query"])
@pytest.mark.parametrize("value", [None, ""])
def test_required_input_fails_before_credentials(app, field, value):
    request = payload(query="T | take 1")
    request["input"][field] = value
    with pytest.raises(ValueError, match="queryServiceUri, databaseName and query/command"):
        asyncio.run(app.rayfin_kusto_v1(request, object()))


@pytest.mark.parametrize("supplied", [{}, {"operation": "executeCommand"}, {"input": {}}])
def test_missing_input_fails_before_credentials(app, supplied):
    with pytest.raises(ValueError, match="queryServiceUri, databaseName and query/command"):
        asyncio.run(app.rayfin_kusto_v1(supplied, object()))


@pytest.mark.parametrize("supplied_id", [None, ""])
def test_missing_request_id_uses_kusto_uuid_prefix(app, monkeypatch, supplied_id):
    request = payload(query="T | take 1")
    if supplied_id is None:
        del request["input"]["clientRequestId"]
    else:
        request["input"]["clientRequestId"] = supplied_id
    generated = uuid.UUID("00112233-4455-4677-8899-aabbccddeeff")
    monkeypatch.setattr(app.uuid, "uuid4", lambda: generated)
    session = bind_session(app, monkeypatch, Response())

    asyncio.run(invoke(app, request, KustoClient()))

    assert session.calls[0][1]["headers"]["x-ms-client-request-id"] == (
        f"KPC.rayfin_kusto_v1;{generated}"
    )


@pytest.mark.parametrize("stage, failure", [
    ("credentials", RuntimeError("synthetic credential failure")),
    ("post", aiohttp.ClientConnectionError("synthetic connection failure")),
    ("post", asyncio.TimeoutError()),
])
def test_credential_and_transport_failures_propagate(app, monkeypatch, stage, failure):
    response = Response()
    session = bind_session(app, monkeypatch, response)
    client = KustoClient()

    def fail(*_args, **_kwargs):
        raise failure

    if stage == "credentials":
        monkeypatch.setattr(client, "get_access_token", fail)
    else:
        monkeypatch.setattr(session, "post", fail)

    with pytest.raises(type(failure)):
        asyncio.run(app.rayfin_kusto_v1(payload(query="T | take 1"), client))
    assert response.chunk_reads == response.text_calls == response.release_calls == 0


def test_metadata_matches_real_runtime_binding(monkeypatch):
    monkeypatch.delenv("POWERBI_API_BASE", raising=False)
    app = load_app()
    assert app._POWERBI_BASE == "https://api.powerbi.com/v1.0/myorg"
    entries = json.loads((ROOT / "functions.metadata").read_text())
    functions = {f.get_function_name(): f for f in app.udf.get_functions()}
    assert set(functions) == {entry["name"] for entry in entries} == {
        "rayfin_semantic_model_v1", "rayfin_fabriciq_mcp_v1", "rayfin_kusto_v1",
    }
    entry = next(entry for entry in entries if entry["name"] == "rayfin_kusto_v1")
    assert entry["scriptFile"] == "function_app.py"
    assert entry["bindings"] == [
        {
            "name": "req", "direction": "In", "type": "httpTrigger",
            "methods": ["post"], "route": entry["name"], "authLevel": "Anonymous",
        },
        {
            "name": "kustoClient", "direction": "In", "type": "FabricItem",
            "audienceType": "Kusto",
        },
    ]
    assert entry["fabricProperties"] == {
        "fabricFunctionReturnType": "StreamResponse",
        "fabricFunctionParameters": [{"name": "payload", "dataType": "dict"}],
    }
    runtime = functions[entry["name"]].get_dict_repr()
    assert runtime["scriptFile"] == entry["scriptFile"]
    bindings = runtime["bindings"]
    # The SDK also adds HTTP output and invocation-context bindings.
    assert len([binding for binding in bindings if binding["type"] == "FabricItem"]) == 1
    for expected in entry["bindings"]:
        actual = next(binding for binding in bindings if binding["type"] == expected["type"])
        for name, value in expected.items():
            if name == "direction":
                assert actual[name].name.lower() == value.lower()
            elif name == "methods":
                assert [method.value.lower() for method in actual[name]] == value
            elif name == "authLevel":
                assert actual[name].value.lower() == value.lower()
            else:
                assert actual[name] == value


@pytest.mark.parametrize("filename", ["SourceCode.zip", "Deploy.zip"])
def test_packaged_source_matches_production_tree(filename):
    with ZipFile(ROOT / filename) as archive:
        assert archive.testzip() is None
        assert archive.read("function_app.py") == (ROOT / "function_app.py").read_bytes()
        assert archive.read("fabric_lib/functions.metadata") == (
            ROOT / "functions.metadata"
        ).read_bytes()
