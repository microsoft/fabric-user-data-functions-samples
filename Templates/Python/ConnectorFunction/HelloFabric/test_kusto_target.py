import asyncio
import json
import socket
from copy import deepcopy

import aiohttp
import pytest

from test_function_app import (
    _FakeKustoClient,
    _FakeResponse,
    _FakeSession,
    _command_payload,
    _load_function_app,
    _payload,
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def unexpected(*_args, **_kwargs):
        pytest.fail("Live network requests are not allowed.")

    monkeypatch.setattr(aiohttp.ClientSession, "_request", unexpected)
    monkeypatch.setattr(socket, "getaddrinfo", unexpected)


@pytest.fixture
def app():
    return _load_function_app()


@pytest.mark.parametrize("endpoint", [
    None,
    "",
    42,
    True,
    [],
    {},
    "http://cluster.kusto.fabric.microsoft.com",
    "https://example.invalid",
    "https://kusto.fabric.microsoft.com",
    "https://cluster.kusto.fabric.microsoft.com.example.invalid",
    "https://clusterkusto.fabric.microsoft.com",
    "https://cluster.kusto.windows.net",
    "https://127.0.0.1",
    "https://[::1]",
    "https://user@cluster.kusto.fabric.microsoft.com",
    "https://user:password@cluster.kusto.fabric.microsoft.com",
    "https://cluster.kusto.fabric.microsoft.com:444",
    "https://cluster.kusto.fabric.microsoft.com:0443",
    "https://cluster.kusto.fabric.microsoft.com:",
    "https://cluster.kusto.fabric.microsoft.com/v1/rest/query",
    "https://cluster.kusto.fabric.microsoft.com//",
    "https://cluster.kusto.fabric.microsoft.com/../",
    "https://cluster.kusto.fabric.microsoft.com/%2e/",
    "https://cluster.kusto.fabric.microsoft.com?",
    "https://cluster.kusto.fabric.microsoft.com/?token=value",
    "https://cluster.kusto.fabric.microsoft.com#",
    "https://cluster.kusto.fabric.microsoft.com/#fragment",
    "https://cluster.kusto.fabric.microsoft.com\\",
    "https://cluster.kusto.fabric.microsoft.com\n",
    "https://clu\tster.kusto.fabric.microsoft.com",
    " https://cluster.kusto.fabric.microsoft.com",
    "https://cluster.kusto.fabric.microsoft.com ",
    "https://-cluster.kusto.fabric.microsoft.com",
    "https://cluster-.kusto.fabric.microsoft.com",
    "https://cluster..kusto.fabric.microsoft.com",
    "https://cluster.kusto.fabric.microsoft.com.",
    "https://cluster.kusto.fabric.microsoft.com%2f.example.invalid",
    "https://clu\u212aster.kusto.fabric.microsoft.com",
    "https://" + ("a" * 64) + ".kusto.fabric.microsoft.com",
    "https://" + ".".join(["a" * 63] * 4) + ".kusto.fabric.microsoft.com",
])
@pytest.mark.parametrize("operation", ["executeQuery", "executeCommand"])
def test_invalid_endpoint_stops_before_credentials_and_http(
    app, monkeypatch, endpoint, operation
):
    class NoCredentials:
        def get_access_token(self):
            pytest.fail("An invalid endpoint must not acquire credentials.")

    async def no_session():
        pytest.fail("An invalid endpoint must not create an HTTP session.")

    monkeypatch.setattr(app, "_get_session", no_session)
    request = _command_payload() if operation == "executeCommand" else _payload()
    request["input"]["queryServiceUri"] = endpoint

    with pytest.raises(ValueError, match="queryServiceUri"):
        asyncio.run(app.rayfin_kusto_v1(request, NoCredentials()))


@pytest.mark.parametrize("endpoint", [
    "https://cluster.kusto.fabric.microsoft.com",
    "https://cluster.kusto.fabric.microsoft.com/",
    "https://cluster.kusto.fabric.microsoft.com:443",
    "https://cluster.kusto.fabric.microsoft.com:443/",
    "HTTPS://CLUSTER.KUSTO.FABRIC.MICROSOFT.COM/",
])
@pytest.mark.parametrize("operation,route", [
    ("executeQuery", "query"),
    ("executeCommand", "mgmt"),
])
def test_valid_endpoint_is_normalized_without_redirects(
    app, monkeypatch, endpoint, operation, route
):
    response = _FakeResponse([b'{"Tables":[]}'])
    session = _FakeSession(response)

    async def get_session():
        return session

    monkeypatch.setattr(app, "_get_session", get_session)
    request = _command_payload() if operation == "executeCommand" else _payload()
    request["input"]["queryServiceUri"] = endpoint

    async def invoke():
        result = await app.rayfin_kusto_v1(request, _FakeKustoClient())
        return [chunk async for chunk in result.body]

    assert asyncio.run(invoke()) == [b'{"Tables":[]}']
    assert session.captured_url == (
        f"https://cluster.kusto.fabric.microsoft.com/v1/rest/{route}"
    )
    assert session.captured_allow_redirects is False
    assert session.captured_json["db"] == "db"
    assert session.captured_json["csl"] == (
        request["input"]["command"] if operation == "executeCommand"
        else request["input"]["query"]
    )
    assert request["input"]["queryServiceUri"] == endpoint


def test_redirect_is_returned_without_a_follow_up_request(app, monkeypatch):
    response = _FakeResponse([], status=302)
    response.headers = {"Location": "https://receiver.invalid"}
    session = _FakeSession(response)

    async def get_session():
        return session

    monkeypatch.setattr(app, "_get_session", get_session)

    result = asyncio.run(app.rayfin_kusto_v1(_payload(), _FakeKustoClient()))

    assert result.status_code == 302
    assert session.captured_allow_redirects is False
    assert session.captured_url == "https://cluster.kusto.fabric.microsoft.com/v1/rest/query"
    assert response.released is True


@pytest.mark.parametrize("operation,text,route", [
    (None, "T | take 1", "query"),
    ("executeQuery", "T | take 1", "query"),
    ("executeCommand", ".show tables", "mgmt"),
    ("executeCommand", ".show database schema", "mgmt"),
])
@pytest.mark.parametrize("caller_properties", [
    None,
    {"Options": {"request_readonly": False, "request_readonly_hardline": False}},
])
def test_read_only_options_are_server_owned(
    app, monkeypatch, operation, text, route, caller_properties
):
    response = _FakeResponse([b'{"Tables":[]}'])
    session = _FakeSession(response)

    async def get_session():
        return session

    monkeypatch.setattr(app, "_get_session", get_session)
    request = _command_payload() if operation == "executeCommand" else _payload()
    if operation is not None:
        request["operation"] = operation
    field = "command" if operation == "executeCommand" else "query"
    request["input"][field] = text
    if caller_properties is not None:
        request["properties"] = deepcopy(caller_properties)
        request["input"]["properties"] = deepcopy(caller_properties)
    original = deepcopy(request)

    async def invoke():
        result = await app.rayfin_kusto_v1(request, _FakeKustoClient())
        return [chunk async for chunk in result.body]

    assert asyncio.run(invoke()) == [b'{"Tables":[]}']
    assert session.captured_url.endswith(f"/v1/rest/{route}")
    assert session.captured_json == {
        "db": "db",
        "csl": text,
        "properties": json.dumps({
            "Options": {
                "request_readonly": True,
                "request_readonly_hardline": True,
            },
            "Parameters": {},
        }),
    }
    assert request == original
    assert response.text_called is False
    assert response.released is True


def test_engine_read_only_error_is_returned(app, monkeypatch):
    response = _FakeResponse([], status=400)
    session = _FakeSession(response)

    async def get_session():
        return session

    async def error_text():
        return '{"error":{"code":"ReadOnlyRequest"}}'

    monkeypatch.setattr(app, "_get_session", get_session)
    monkeypatch.setattr(response, "text", error_text)
    request = _command_payload()
    request["input"]["command"] = ".drop table T"

    result = asyncio.run(app.rayfin_kusto_v1(request, _FakeKustoClient()))

    assert result.status_code == 400
    assert list(result.body) == [b'{"error":{"code":"ReadOnlyRequest"}}']
    assert json.loads(session.captured_json["properties"])["Options"] == {
        "request_readonly": True,
        "request_readonly_hardline": True,
    }
    assert response.released is True


@pytest.mark.parametrize("operation", ["executeQuery", "executeCommand"])
def test_query_parameters_are_bound_without_changing_text(app, monkeypatch, operation):
    response = _FakeResponse([b'{"Tables":[]}'])
    session = _FakeSession(response)

    async def get_session():
        return session

    monkeypatch.setattr(app, "_get_session", get_session)
    request = _command_payload() if operation == "executeCommand" else _payload()
    field = "command" if operation == "executeCommand" else "query"
    parameters = {
        "s": '"hello"',
        "n": "long(42)",
        "r": "real(1.25)",
        "b": "true",
        "at": "datetime(2026-10-26T10:00:00.0000000Z)",
        "id": "guid(12345678-1234-1234-abcd-1234567890ab)",
        "label": json.dumps('"; database("OtherDb").SecretTable'),
    }
    request["input"]["parameters"] = parameters
    original = deepcopy(request)

    async def invoke():
        result = await app.rayfin_kusto_v1(request, _FakeKustoClient())
        return [chunk async for chunk in result.body]

    assert asyncio.run(invoke()) == [b'{"Tables":[]}']
    assert session.captured_json["csl"] == original["input"][field]
    properties = json.loads(session.captured_json["properties"])
    assert properties["Parameters"] == parameters
    assert properties["Options"] == {
        "request_readonly": True,
        "request_readonly_hardline": True,
    }
    assert request == original
    assert response.text_called is False
    assert response.released is True


@pytest.mark.parametrize("parameters", [
    None,
    [],
    "",
    1,
    {"since": None},
    {"since": 123},
    {"since": True},
    {"since": []},
    {"since": {}},
    {"": "1"},
    {"with space": "1"},
    {"a;print": "1"},
    {"\u212a": "1"},
])
@pytest.mark.parametrize("operation", ["executeQuery", "executeCommand"])
def test_invalid_query_parameters_stop_before_credentials_and_http(
    app, monkeypatch, parameters, operation
):
    class NoCredentials:
        def get_access_token(self):
            pytest.fail("Invalid parameters must not acquire credentials.")

    async def no_session():
        pytest.fail("Invalid parameters must not create an HTTP session.")

    monkeypatch.setattr(app, "_get_session", no_session)
    request = _command_payload() if operation == "executeCommand" else _payload()
    request["input"]["parameters"] = parameters

    with pytest.raises(ValueError, match="parameters"):
        asyncio.run(app.rayfin_kusto_v1(request, NoCredentials()))
