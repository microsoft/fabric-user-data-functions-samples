import asyncio
import inspect
import json
import os
import socket
from copy import deepcopy

import aiohttp
import pytest

from test_function_app import (
    _FakeKustoClient,
    _FakeResponse,
    _FakeSession,
    _load_function_app,
)

DATABASE = "813f7fc1-66ff-4faa-aee5-0f0686f74217"
ENDPOINT = "https://trd-cluster.z6.kusto.fabric.microsoft.com"
TEMPLATE_MARKER = "TEMPLATE_TEXT_MARKER"
VALUE_MARKER = "PARAMETER_VALUE_MARKER"
FILTER_TEXT = (
    f"// {TEMPLATE_MARKER}\ndeclare query_parameters(state:string);\n"
    "StormEvents\n| where State == state"
)
READ_ONLY = {
    "request_readonly": True,
    "request_readonly_hardline": True,
    "request_remote_entities_disabled": True,
}
DEFAULT = object()
MISSING = object()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def unexpected(*_args, **_kwargs):
        pytest.fail("Live network requests are not allowed.")

    monkeypatch.setattr(aiohttp.ClientSession, "_request", unexpected)
    monkeypatch.setattr(socket, "getaddrinfo", unexpected)


@pytest.fixture
def app():
    # Python 3.9 needs a running loop for the module-level asyncio.Lock when
    # another test file has forced a fresh import.
    async def load():
        return _load_function_app()

    return asyncio.run(load())


class _Session(_FakeSession):
    def __init__(self, response):
        super().__init__(response)
        self.calls = 0
        self.captured_json = None
        self.captured_allow_redirects = None

    async def post(self, url, json=None, headers=None, *, allow_redirects=True):
        self.calls += 1
        self.captured_url = url
        self.captured_headers = headers
        self.captured_json = json
        self.captured_allow_redirects = allow_redirects
        return self._response


class _ErrorResponse(_FakeResponse):
    def __init__(self, status, detail, content_type):
        super().__init__([], status=status)
        self.headers = {"Content-Type": content_type}
        self._detail = detail

    async def text(self):
        self.text_called = True
        return self._detail


class _NoCredentials:
    def get_access_token(self):
        pytest.fail("Credentials must not be requested before validation passes.")


def _template(operation="executeQuery", text=FILTER_TEXT, parameters=None,
              roles=("authenticated",)):
    return {
        "operation": operation,
        "text": text,
        "parameters": {"state": "string"} if parameters is None else parameters,
        "roles": list(roles),
    }


def _settings(queries=None):
    return {
        "queryServiceUri": ENDPOINT,
        "database": DATABASE,
        "queries": {"Q": _template()} if queries is None else queries,
    }


def _caller(roles=(), authenticated=True):
    return {"isAuthenticated": authenticated, "roles": list(roles)}


def _payload(parameters=None, query_id="Q", operation="executeQuery", **extra):
    request_input = {"queryId": query_id}
    if parameters is not None:
        request_input["parameters"] = parameters
    request_input.update(extra)
    return {"operation": operation, "input": request_input}


def _texas():
    return _payload({"state": "TEXAS"})


def _binding(payload):
    return {
        "appId": "app", "workspaceId": "workspace", "connector": "events",
        "operation": payload.get("operation"), "function": "rayfin_eventhouse_v1",
        "artifactId": "artifact", "revision": 1, "sha256": "0" * 64,
    }


def _host(payload, settings=DEFAULT, caller=MISSING, binding=MISSING):
    if not isinstance(payload, dict):
        return payload
    request = deepcopy(payload)
    for name, value, default in (
        ("additionalPayload", settings, _settings),
        ("binding", binding, lambda: _binding(payload)),
        ("callerContext", caller, _caller),
    ):
        if value is DEFAULT:
            request[name] = default()
        elif value is not MISSING:
            request[name] = value
    return request


def _accept(app, monkeypatch, payload, settings=DEFAULT, caller=MISSING, response=None,
            binding=MISSING):
    session = _Session(response or _FakeResponse([b'{"Tables":[]}']))

    async def get_session():
        return session

    monkeypatch.setattr(app, "_get_session", get_session)

    async def invoke():
        result = await app.rayfin_eventhouse_v1(
            _host(payload, settings, caller, binding), _FakeKustoClient()
        )
        if hasattr(result.body, "__aiter__"):
            return result, [chunk async for chunk in result.body]
        return result, list(result.body)

    result, chunks = asyncio.run(invoke())
    assert session.calls == 1
    return result, chunks, session


def _reject(app, monkeypatch, payload, settings=DEFAULT, caller=MISSING, binding=MISSING):
    async def no_session():
        pytest.fail("No HTTP session may be created before validation passes.")

    monkeypatch.setattr(app, "_get_session", no_session)

    async def invoke():
        result = await app.rayfin_eventhouse_v1(
            _host(payload, settings, caller, binding), _NoCredentials()
        )
        return result, b"".join(list(result.body))

    result, raw = asyncio.run(invoke())
    assert result.status_code == 200
    text = raw.decode("utf-8")
    assert TEMPLATE_MARKER not in text
    assert VALUE_MARKER not in text
    assert "fake-kusto-token" not in text
    document = json.loads(text)
    assert result.headers["x-fabric-udf-status"] == document["status"]
    return document["error"]["code"], document["status"]


def _code(app, monkeypatch, payload, settings=DEFAULT, caller=MISSING, binding=MISSING):
    return _reject(app, monkeypatch, payload, settings, caller, binding)[0]


def _properties(session):
    return json.loads(session.captured_json["properties"])


# --- host contract -----------------------------------------------------------


def test_signature_matches_function_metadata(app):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "functions.metadata")
    with open(path, encoding="utf-8") as handle:
        entries = {entry["name"]: entry for entry in json.load(handle)}

    assert "rayfin_kusto_v1" not in entries
    entry = entries["rayfin_eventhouse_v1"]
    assert entry["scriptFile"] == "function_app.py"
    assert entry["bindings"] == [
        {"name": "req", "direction": "In", "type": "httpTrigger", "methods": ["post"],
         "route": "rayfin_eventhouse_v1", "authLevel": "Anonymous"},
        {"name": "kustoClient", "direction": "In", "type": "FabricItem", "audienceType": "Kusto"},
    ]
    assert entry["fabricProperties"] == {
        "fabricFunctionReturnType": "StreamResponse",
        "fabricFunctionParameters": [{"name": "payload", "dataType": "dict"}],
    }
    assert list(inspect.signature(app.rayfin_eventhouse_v1).parameters) == ["payload", "kustoClient"]
    assert not hasattr(app, "rayfin_kusto_v1")


@pytest.mark.parametrize("settings", [MISSING, None, {}])
def test_missing_settings_require_a_policy(app, monkeypatch, settings):
    assert _reject(app, monkeypatch, _texas(), settings=settings) == (
        "QUERY_POLICY_REQUIRED", "BadRequest"
    )


@pytest.mark.parametrize("settings", ["settings", 1, [], [_settings()], True])
def test_malformed_settings_are_rejected(app, monkeypatch, settings):
    assert _code(app, monkeypatch, _texas(), settings=settings) == "INVALID_QUERY_POLICY"


@pytest.mark.parametrize("name", ["config", "operations", "databaseName", "roles", "Queries"])
def test_settings_reject_unknown_fields(app, monkeypatch, name):
    settings = _settings()
    settings[name] = {}
    assert _code(app, monkeypatch, _texas(), settings=settings) == "INVALID_QUERY_POLICY"


def test_queries_under_config_are_rejected(app, monkeypatch):
    settings = {"queryServiceUri": ENDPOINT, "database": DATABASE,
                "config": {"queries": {"Q": _template()}}}
    assert _code(app, monkeypatch, _texas(), settings=settings) == "INVALID_QUERY_POLICY"


@pytest.mark.parametrize("caller", [
    None,
    [],
    "authenticated",
    {},
    {"roles": []},
    {"isAuthenticated": True},
    {"isAuthenticated": "true", "roles": []},
    {"isAuthenticated": 1, "roles": []},
    {"isAuthenticated": None, "roles": []},
    {"isAuthenticated": True, "roles": None},
    {"isAuthenticated": True, "roles": "authenticated"},
    {"isAuthenticated": True, "roles": ("authenticated",)},
    {"isAuthenticated": True, "roles": [1]},
    {"isAuthenticated": True, "roles": [None]},
])
def test_malformed_caller_context_fails_closed(app, monkeypatch, caller):
    async def no_session():
        pytest.fail("No HTTP session may be created before validation passes.")

    monkeypatch.setattr(app, "_get_session", no_session)
    result = asyncio.run(app.rayfin_eventhouse_v1(_host(_texas(), caller=caller), _NoCredentials()))
    document = json.loads(b"".join(list(result.body)))
    assert document["error"]["code"] == "INVALID_CALLER_CONTEXT"
    assert document["status"] == "Failed"


@pytest.mark.parametrize("arguments", [
    {"additionalPayload": {}}, {"binding": {}}, {"callerContext": {}},
    {"additionalPayload": {}, "binding": {}, "callerContext": {}},
])
def test_host_fields_are_not_top_level_parameters(app, arguments):
    with pytest.raises(TypeError):
        app.rayfin_eventhouse_v1(_texas(), kustoClient=_FakeKustoClient(), **arguments)
    with pytest.raises(TypeError):
        app.rayfin_eventhouse_v1(_texas(), *arguments.values(), _FakeKustoClient())


def test_binding_is_optional(app, monkeypatch):
    _, _, session = _accept(app, monkeypatch, _texas())
    assert session.captured_json["db"] == DATABASE


def test_full_binding_is_accepted(app, monkeypatch):
    _accept(app, monkeypatch, _texas(), binding=_binding(_texas()))


@pytest.mark.parametrize("binding", [
    None, "rayfin_eventhouse_v1", [], ["rayfin_eventhouse_v1"], True, 1,
])
def test_binding_must_be_an_object(app, monkeypatch, binding):
    assert _reject(app, monkeypatch, _texas(), binding=binding) == (
        "INVALID_QUERY_POLICY", "BadRequest"
    )


@pytest.mark.parametrize("changes", [
    {"operation": "executeCommand"},
    {"operation": "executequery"},
    {"operation": None},
    {"operation": ["executeQuery"]},
    {"function": "rayfin_kusto_v1"},
    {"function": "rayfin_kusto_v2"},
    {"function": "RAYFIN_KUSTO_V1"},
    {"function": None},
    {"function": {"name": "rayfin_eventhouse_v1"}},
])
def test_binding_must_match_the_function_and_operation(app, monkeypatch, changes):
    binding = dict(_binding(_texas()), **changes)
    assert _code(app, monkeypatch, _texas(), binding=binding) == "INVALID_QUERY_POLICY"


@pytest.mark.parametrize("binding", [
    {},
    {"function": "rayfin_eventhouse_v1"},
    {"operation": "executeQuery"},
    {"appId": "app", "artifactId": "a"},
    dict(_binding(_texas()), extra="ignored"),
])
def test_binding_fields_other_than_function_and_operation_are_optional(
    app, monkeypatch, binding
):
    _accept(app, monkeypatch, _texas(), binding=binding)


def test_binding_is_checked_before_the_caller(app, monkeypatch):
    binding = dict(_binding(_texas()), function="other")
    assert _code(app, monkeypatch, _texas(), caller=None, binding=binding) == (
        "INVALID_QUERY_POLICY"
    )


def test_settings_are_checked_before_the_binding(app, monkeypatch):
    binding = dict(_binding(_texas()), function="other")
    assert _code(app, monkeypatch, _texas(), settings=MISSING, binding=binding) == (
        "QUERY_POLICY_REQUIRED"
    )


def test_missing_caller_context_is_a_signed_in_user(app, monkeypatch):
    _, _, session = _accept(app, monkeypatch, _texas(),
                            settings=_settings({"Q": _template(roles=["authenticated"])}))
    assert session.captured_json["csl"] == FILTER_TEXT


@pytest.mark.parametrize("roles", [["admin"], ["Admin", "viewer"], ["anonymous"]])
def test_missing_caller_context_has_no_custom_roles(app, monkeypatch, roles):
    settings = _settings({"Q": _template(roles=roles)})
    assert _reject(app, monkeypatch, _texas(), settings=settings) == (
        "QUERY_ROLE_NOT_ALLOWED", "BadRequest"
    )


@pytest.mark.parametrize("caller", [MISSING, _caller([]), _caller(["admin"])])
def test_template_roles_default_to_authenticated(app, monkeypatch, caller):
    template = _template()
    del template["roles"]
    _accept(app, monkeypatch, _texas(), settings=_settings({"Q": template}), caller=caller)


@pytest.mark.parametrize("caller,code", [
    (_caller([], authenticated=False), "QUERY_ROLE_NOT_ALLOWED"),
    (_caller(["anonymous"]), "QUERY_ROLE_NOT_ALLOWED"),
    (None, "INVALID_CALLER_CONTEXT"),
    ({}, "INVALID_CALLER_CONTEXT"),
])
def test_template_without_roles_still_checks_a_present_caller(app, monkeypatch, caller, code):
    template = _template()
    del template["roles"]
    assert _code(app, monkeypatch, _texas(), settings=_settings({"Q": template}),
                 caller=caller) == code


def test_caller_context_ignores_extra_fields(app, monkeypatch):
    caller = dict(_caller(), tenantId="t", userId="u", isAnonymous=False)
    _accept(app, monkeypatch, _texas(), caller=caller)


# --- target --------------------------------------------------------------------


@pytest.mark.parametrize("endpoint", [
    None,
    "",
    42,
    [],
    "http://cluster.kusto.fabric.microsoft.com",
    "https://kusto.fabric.microsoft.com",
    "https://cluster.kusto.windows.net",
    "https://help.kusto.windows.net",
    "https://cluster.kusto.fabric.microsoft.com.example.invalid",
    "https://user@cluster.kusto.fabric.microsoft.com",
    "https://cluster.kusto.fabric.microsoft.com:444",
    "https://cluster.kusto.fabric.microsoft.com/v1/rest/query",
    "https://cluster.kusto.fabric.microsoft.com//",
    "https://cluster.kusto.fabric.microsoft.com/?x=1",
    "https://cluster.kusto.fabric.microsoft.com/#f",
    "https://cluster.kusto.fabric.microsoft.com\\",
    "https://cluster.kusto.fabric.microsoft.com\n",
    " https://cluster.kusto.fabric.microsoft.com",
    "https://-cluster.kusto.fabric.microsoft.com",
    "https://cluster..kusto.fabric.microsoft.com",
    "https://cluster.kusto.fabric.microsoft.com.",
    "https://clu\u212aster.kusto.fabric.microsoft.com",
    "https://" + ("a" * 64) + ".kusto.fabric.microsoft.com",
    "https://" + ".".join(["a" * 63] * 4) + ".kusto.fabric.microsoft.com",
])
def test_invalid_endpoint_is_a_target_error(app, monkeypatch, endpoint):
    settings = _settings()
    settings["queryServiceUri"] = endpoint
    assert _reject(app, monkeypatch, _texas(), settings=settings) == (
        "INVALID_CONNECTOR_TARGET", "BadRequest"
    )


def test_missing_endpoint_is_a_target_error(app, monkeypatch):
    settings = _settings()
    del settings["queryServiceUri"]
    assert _code(app, monkeypatch, _texas(), settings=settings) == "INVALID_CONNECTOR_TARGET"


@pytest.mark.parametrize("database", [
    None, "", "Samples", 42, "{" + DATABASE + "}", DATABASE.replace("-", ""),
    DATABASE + "\n", " " + DATABASE, DATABASE[:-1] + "g",
])
def test_invalid_database_is_a_target_error(app, monkeypatch, database):
    settings = _settings()
    settings["database"] = database
    assert _code(app, monkeypatch, _texas(), settings=settings) == "INVALID_CONNECTOR_TARGET"


@pytest.mark.parametrize("endpoint", [
    ENDPOINT, ENDPOINT + "/", ENDPOINT + ":443", ENDPOINT + ":443/", ENDPOINT.upper(),
])
def test_valid_endpoint_is_normalized(app, monkeypatch, endpoint):
    settings = _settings()
    settings["queryServiceUri"] = endpoint
    _, _, session = _accept(app, monkeypatch, _texas(), settings=settings)
    assert session.captured_url == ENDPOINT.lower() + "/v1/rest/query"


@pytest.mark.parametrize("item_id", [DATABASE, DATABASE.upper()])
def test_injected_item_id_must_match_the_database(app, monkeypatch, item_id):
    payload = _payload({"state": "TEXAS"}, workspaceId="w", itemId=item_id, baasItemId="b")
    _, _, session = _accept(app, monkeypatch, payload)
    assert session.captured_json["db"] == DATABASE


@pytest.mark.parametrize("item_id", [
    "00000000-0000-0000-0000-000000000000", "Samples", "", None, 5,
])
def test_mismatched_item_id_is_a_target_error(app, monkeypatch, item_id):
    payload = _payload({"state": "TEXAS"}, itemId=item_id)
    assert _code(app, monkeypatch, payload) == "INVALID_CONNECTOR_TARGET"


# --- saved policy ----------------------------------------------------------------


@pytest.mark.parametrize("queries", [None, {}, [], "Q", [_template()]])
def test_missing_queries_require_a_policy(app, monkeypatch, queries):
    settings = _settings()
    if queries is None:
        del settings["queries"]
    else:
        settings["queries"] = queries
    assert _code(app, monkeypatch, _texas(), settings=settings) == "QUERY_POLICY_REQUIRED"


def test_template_count_is_limited(app, monkeypatch):
    queries = {f"Q{index}": _template() for index in range(256)}
    queries["Q"] = queries.pop("Q0")
    _accept(app, monkeypatch, _texas(), settings=_settings(queries))
    queries["Q256"] = _template()
    assert _code(app, monkeypatch, _texas(), settings=_settings(queries)) == "INVALID_QUERY_POLICY"


@pytest.mark.parametrize("query_id", [
    "", "1Q", "a-b", "a b", "a.b", "\u00e4", "Q\n", "a" * 129,
])
def test_invalid_template_ids_reject_the_policy(app, monkeypatch, query_id):
    queries = {"Q": _template(), query_id: _template()}
    code, status = _reject(app, monkeypatch, _texas(), settings=_settings(queries))
    assert (code, status) == ("INVALID_QUERY_POLICY", "BadRequest")


def test_longest_template_id_is_accepted(app, monkeypatch):
    query_id = "_" + "a" * 127
    _accept(app, monkeypatch, _payload({"state": "x"}, query_id=query_id),
            settings=_settings({query_id: _template()}))


def _invalid_template_variants():
    base = _template()
    missing = {key: deepcopy(base) for key in base if key != "roles"}
    for key, template in missing.items():
        del template[key]
    return [
        None,
        "template",
        [],
        *missing.values(),
        dict(base, parmeters={"state": "string"}),
        {"operation": "executeQuery", "query": FILTER_TEXT, "parameters": {}, "roles": ["authenticated"]},
        dict(base, operation="executeQuerry"),
        dict(base, operation="ExecuteQuery"),
        dict(base, operation=None),
        dict(base, operation=["executeQuery"]),
        dict(base, operation={"executeQuery": True}),
        dict(base, operation=1),
        dict(base, operation="executeCommand"),
        dict(base, text=None),
        dict(base, text=5),
        dict(base, text=""),
        dict(base, text=" \t\r\n"),
        dict(base, text="\u3000\u00a0"),
        dict(base, text="\u2000\u2005\u200a\u205f\u2028\x85"),
        dict(base, text="T | take 1\0"),
        dict(base, text="T" * 65537),
        dict(base, text="T" * 65535 + "\U0001f600"),
        dict(base, parameters=None),
        dict(base, parameters=[]),
        dict(base, parameters={f"p{index}": "string" for index in range(129)}),
        dict(base, parameters={"1p": "string"}),
        dict(base, parameters={"p-q": "string"}),
        dict(base, parameters={"p": "int"}),
        dict(base, parameters={"p": "String"}),
        dict(base, parameters={"p": "Dynamic"}),
        dict(base, parameters={"p": "TimeSpan"}),
        dict(base, parameters={"p": "decimal"}),
        dict(base, parameters={"p": "datetime "}),
        dict(base, parameters={"p": None}),
        dict(base, parameters={"p": 1}),
        dict(base, roles=None),
        dict(base, roles="authenticated"),
        dict(base, roles=[]),
        dict(base, roles=["r"] * 2),
        dict(base, roles=[f"r{index}" for index in range(129)]),
        dict(base, roles=[""]),
        dict(base, roles=[" "]),
        dict(base, roles=[" admin"]),
        dict(base, roles=["admin "]),
        dict(base, roles=["admin\u3000"]),
        dict(base, roles=["admin\u2009"]),
        dict(base, roles=["\u2007"]),
        dict(base, roles=["ad\x01min"]),
        dict(base, roles=["ad\x85min"]),
        dict(base, roles=["a" * 129]),
        dict(base, roles=["\U0001f600" * 65]),
        dict(base, roles=[None]),
        dict(base, roles=[1]),
    ]


@pytest.mark.parametrize("template", _invalid_template_variants())
def test_invalid_templates_reject_every_invocation(app, monkeypatch, template):
    queries = {"Q": _template(), "Other": template}
    assert _code(app, monkeypatch, _texas(), settings=_settings(queries)) == "INVALID_QUERY_POLICY"


@pytest.mark.parametrize("template", [
    _template(text="T" * 65536, parameters={}),
    _template(text="T" * 65534 + "\U0001f600", parameters={}),
    _template(parameters={f"p{index}": "string" for index in range(128)}),
    _template(parameters={"t": "timespan", "v": "dynamic"}),
    _template(roles=[f"r{index}" for index in range(128)]),
    _template(roles=["a" * 128]),
    _template(roles=["\U0001f600" * 64]),
    _template(roles=["Admin", "admin"]),
    _template(roles=["\u05de\u05e0\u05d4\u05dc"]),
])
def test_boundary_templates_are_accepted(app, monkeypatch, template):
    queries = {"Q": _template(), "Other": template}
    _accept(app, monkeypatch, _texas(), settings=_settings(queries))


ALLOWED_COMMANDS = [
    ".show tables",
    ".SHOW TABLES",
    "  \t\r\n.show   tables \n",
    ".show table StormEvents",
    ".show table StormEvents schema",
    ".show table StormEvents schema as json",
    ".show table StormEvents cslschema",
    ".show table ['Storm Events-1.x'] schema as json",
    ".show database schema",
    ".show database schema as json",
    ".show database cslschema",
    ".show functions",
    ".show function MyFunction",
    ".show function ['My Function']",
    ".show materialized-views",
]


@pytest.mark.parametrize("command", ALLOWED_COMMANDS)
def test_allowed_metadata_commands(app, monkeypatch, command):
    queries = {"C": _template("executeCommand", command, {})}
    _, _, session = _accept(app, monkeypatch, _payload({}, query_id="C", operation="executeCommand"),
                            settings=_settings(queries))
    assert session.captured_url == ENDPOINT + "/v1/rest/mgmt"
    assert session.captured_json["csl"] == command


@pytest.mark.parametrize("command", [
    ".show tabels",
    ".show databases",
    ".show principals",
    ".show cluster",
    ".show tables details",
    ".show table StormEvents details",
    ".show table StormEvents extents",
    ".show external tables",
    ".show database Other schema",
    ".show database schema as csl",
    ".show function MyFunction with (x=1)",
    ".show table ['a;b'] schema",
    ".show table ['a'] ['b']",
    ".show tables | count",
    ".show tables; .show tables",
    ".show tables\n.show tables",
    ".show tables // comment",
    "// comment\n.show tables",
    ".show\u00a0tables",
    ".show tables\u3000",
    ".\u0455how tables",
    ".drop table StormEvents",
    ".set StormCopy <| StormEvents | take 10",
    ".create table T (a:string)",
    ".show tables\0",
    "StormEvents | take 1",
    "show tables",
])
def test_rejected_metadata_commands(app, monkeypatch, command):
    queries = {"Q": _template(), "C": _template("executeCommand", command, {})}
    assert _code(app, monkeypatch, _texas(), settings=_settings(queries)) == "INVALID_QUERY_POLICY"


@pytest.mark.parametrize("text", [
    ".drop table StormEvents",
    "  .show tables",
    "\u3000.show tables",
    "\ufeff.show tables",
    "\u2028.show tables",
    "// comment\n.show tables",
    "// comment\r.show tables",
    "/* comment */.show tables",
    "/* outer /* inner */ still outer */ .show tables",
    "// only a comment",
    "/* unterminated",
    "/* outer /* inner */ unterminated",
    "/* comment */",
])
def test_query_templates_reject_commands_and_comment_only_text(app, monkeypatch, text):
    queries = {"Q": _template(), "Other": _template(text=text, parameters={})}
    assert _code(app, monkeypatch, _texas(), settings=_settings(queries)) == "INVALID_QUERY_POLICY"


@pytest.mark.parametrize("text", [
    "print 1",
    "// comment\nStormEvents | take 1",
    "/* outer /* inner */ */ StormEvents",
    "\u3000StormEvents",
    "\x1cStormEvents",
    "let x = 1; print x",
    "StormEvents | where Note == '.show tables'",
])
def test_query_templates_accept_query_text(app, monkeypatch, text):
    queries = {"Q": _template(text=text, parameters={})}
    _, _, session = _accept(app, monkeypatch, _payload({}), settings=_settings(queries))
    assert session.captured_json["csl"] == text


# --- invocation shape --------------------------------------------------------------


@pytest.mark.parametrize("payload", [
    None,
    [],
    "payload",
    {},
    {"operation": "executeQuery"},
    {"input": {"queryId": "Q"}},
    {"operation": None, "input": {"queryId": "Q"}},
    {"operation": 1, "input": {"queryId": "Q"}},
    {"operation": "executeQuery", "input": None},
    {"operation": "executeQuery", "input": []},
    {"operation": "executeQuery", "input": "Q"},
])
def test_malformed_payloads_are_rejected(app, monkeypatch, payload):
    assert _code(app, monkeypatch, payload) == "INVALID_QUERY_INPUT"


@pytest.mark.parametrize("name", [
    "queryServiceUri", "databaseName", "query", "command", "roles", "role", "options",
    "properties", "accesstoken", "accessToken", "kustoClient", "AdditionalPayload",
    "callercontext", "Binding", "settings",
])
def test_payload_root_fields_are_rejected(app, monkeypatch, name):
    payload = _texas()
    payload[name] = ["admin"] if name == "roles" else "x"
    assert _code(app, monkeypatch, payload) == "INVALID_QUERY_INPUT"


@pytest.mark.parametrize("name,value", [
    ("additionalPayload", _settings({"Q": _template(roles=["anyone"])})),
    ("callerContext", {"isAuthenticated": True, "roles": ["admin"]}),
    ("binding", {"function": "rayfin_eventhouse_v1", "operation": "executeQuery"}),
    ("roles", ["admin"]),
    ("role", "admin"),
    ("query", "StormEvents | take 1000"),
    ("command", ".show databases"),
    ("csl", "StormEvents"),
    ("text", "StormEvents"),
    ("queryServiceUri", "https://help.kusto.windows.net"),
    ("databaseName", "Samples"),
    ("database", "Samples"),
    ("db", "Samples"),
    ("connectionRuntimeUrl", "https://example.invalid"),
    ("accessToken", "token"),
    ("options", {"request_readonly": False}),
    ("properties", "{}"),
    ("Parameters", {}),
    ("QueryId", "Q"),
])
def test_input_fields_outside_the_contract_are_rejected(app, monkeypatch, name, value):
    payload = _texas()
    payload["input"][name] = value
    assert _code(app, monkeypatch, payload) == "INVALID_QUERY_INPUT"


@pytest.mark.parametrize("query_id", [None, 5, "", "a-b", "Q ", "\u00e4", ["Q"]])
def test_query_id_must_be_an_identifier(app, monkeypatch, query_id):
    payload = _texas()
    payload["input"]["queryId"] = query_id
    assert _code(app, monkeypatch, payload) == "INVALID_QUERY_INPUT"


def test_query_id_is_required(app, monkeypatch):
    assert _code(app, monkeypatch, {"operation": "executeQuery", "input": {}}) == "INVALID_QUERY_INPUT"


@pytest.mark.parametrize("payload", [
    _payload({"state": "TEXAS"}, query_id="DropAll"),
    _payload({"state": "TEXAS"}, query_id="q"),
    _payload({"state": "TEXAS"}, operation="executeCommand"),
    _payload({"state": "TEXAS"}, operation="executequery"),
    _payload({"state": "TEXAS"}, operation="drop"),
])
def test_unknown_template_or_operation_is_not_allowed(app, monkeypatch, payload):
    assert _reject(app, monkeypatch, payload) == ("QUERY_NOT_ALLOWED", "BadRequest")


def test_command_template_cannot_run_as_query(app, monkeypatch):
    queries = {"C": _template("executeCommand", ".show tables", {})}
    payload = _payload({}, query_id="C")
    assert _code(app, monkeypatch, payload, settings=_settings(queries)) == "QUERY_NOT_ALLOWED"


# --- roles -----------------------------------------------------------------------


@pytest.mark.parametrize("template_roles,caller", [
    (["authenticated"], _caller([], authenticated=False)),
    (["authenticated"], _caller(["authenticated"], authenticated=False)),
    (["anonymous"], _caller([], authenticated=False)),
    (["Anonymous"], _caller(["anonymous"], authenticated=False)),
    (["admin"], _caller([])),
    (["admin"], _caller(["Admin"])),
    (["admin"], _caller(["admin "])),
    (["admin"], _caller(["viewer"])),
    (["anonymous"], _caller([])),
    (["viewer"], _caller(["authenticated"])),
])
def test_roles_that_do_not_match_are_rejected(app, monkeypatch, template_roles, caller):
    settings = _settings({"Q": _template(roles=template_roles)})
    assert _reject(app, monkeypatch, _texas(), settings=settings, caller=caller) == (
        "QUERY_ROLE_NOT_ALLOWED", "BadRequest"
    )


@pytest.mark.parametrize("template_roles,caller_roles", [
    (["authenticated"], []),
    (["Authenticated"], []),
    (["AUTHENTICATED"], ["authenticated"]),
    (["authenticated"], ["AuThEnTiCaTeD"]),
    (["authenticated"], [""]),
    (["authenticated"], [" "]),
    (["admin"], ["admin"]),
    (["admin"], ["viewer", "admin"]),
    (["viewer", "admin"], ["admin"]),
    (["GetDataAnalyst"], ["GetDataAnalyst"]),
    (["authenticated"], ["admin"]),
    (["authenticated"], ["viewer"]),
    (["Authenticated"], ["viewer", "GetDataAnalyst"]),
])
def test_matching_roles_are_accepted(app, monkeypatch, template_roles, caller_roles):
    settings = _settings({"Q": _template(roles=template_roles)})
    _accept(app, monkeypatch, _texas(), settings=settings, caller=_caller(caller_roles))


@pytest.mark.parametrize("template_roles,caller_roles", [
    (["Admin"], ["admin"]),
    (["admin"], ["ADMIN"]),
    (["GetDataAnalyst"], ["getdataanalyst"]),
    (["admin"], ["authenticated"]),
])
def test_custom_roles_are_case_sensitive_and_not_implied(app, monkeypatch, template_roles,
                                                          caller_roles):
    settings = _settings({"Q": _template(roles=template_roles)})
    assert _code(app, monkeypatch, _texas(), settings=settings, caller=_caller(caller_roles)) == (
        "QUERY_ROLE_NOT_ALLOWED"
    )


@pytest.mark.parametrize("template_roles,caller_roles", [
    (["anonymous"], ["anonymous"]),
    (["anonymous"], ["Anonymous"]),
    (["anonymous"], ["ANONYMOUS"]),
    (["Anonymous"], ["anonymous"]),
    (["admin"], ["anonymous", "admin"]),
    (["admin"], ["admin", "Anonymous"]),
    (["authenticated"], ["anonymous"]),
    (["authenticated", "anonymous"], ["anonymous"]),
])
def test_anonymous_callers_are_rejected_even_when_authenticated(
    app, monkeypatch, template_roles, caller_roles
):
    settings = _settings({"Q": _template(roles=template_roles)})
    assert _reject(app, monkeypatch, _texas(), settings=settings,
                   caller=_caller(caller_roles)) == ("QUERY_ROLE_NOT_ALLOWED", "BadRequest")


@pytest.mark.parametrize("role", ["authent\u0131cated", "AUTHENT\u0130CATED", "anonymou\u017f"])
def test_built_in_role_names_match_only_ascii_case_variants(app, monkeypatch, role):
    # A lookalike is a custom role: it neither grants nor matches a built-in role.
    settings = _settings({"Q": _template(roles=[role])})
    assert _code(app, monkeypatch, _texas(), settings=settings,
                 caller=_caller(["authenticated", "viewer"])) == (
        "QUERY_ROLE_NOT_ALLOWED"
    )
    settings = _settings({"Q": _template(roles=["admin"])})
    assert _code(app, monkeypatch, _texas(), settings=settings, caller=_caller([role])) == (
        "QUERY_ROLE_NOT_ALLOWED"
    )


def test_anonymous_lookalike_is_a_custom_role(app, monkeypatch):
    settings = _settings({"Q": _template(roles=["anonymou\u017f"])})
    _accept(app, monkeypatch, _texas(), settings=settings, caller=_caller(["anonymou\u017f"]))


# --- parameters ------------------------------------------------------------------


@pytest.mark.parametrize("payload", [
    _payload(["TEXAS"]),
    _payload("state=TEXAS"),
    _payload(5),
    {"operation": "executeQuery", "input": {"queryId": "Q", "parameters": None}},
    _payload({}),
    {"operation": "executeQuery", "input": {"queryId": "Q"}},
    _payload({"state": "TEXAS", "limit": 1000}),
    _payload({"State": "TEXAS"}),
    _payload({"state": None}),
])
def test_parameters_must_match_the_template_exactly(app, monkeypatch, payload):
    assert _code(app, monkeypatch, payload) == "INVALID_QUERY_INPUT"


def test_parameters_are_optional_when_none_are_declared(app, monkeypatch):
    queries = {"Q": _template(text="print 1", parameters={})}
    _, _, session = _accept(app, monkeypatch, {"operation": "executeQuery", "input": {"queryId": "Q"}},
                            settings=_settings(queries))
    assert _properties(session) == {"Options": READ_ONLY, "Parameters": {}}


def test_roles_are_checked_before_parameters(app, monkeypatch):
    settings = _settings({"Q": _template(roles=["admin"])})
    payload = _payload({"state": 5, "extra": 1})
    assert _code(app, monkeypatch, payload, settings=settings) == "QUERY_ROLE_NOT_ALLOWED"


LITERALS = [
    ("timespan", "7d", "timespan(7d)"),
    ("timespan", "-1h", "timespan(-1h)"),
    ("timespan", "100ms", "timespan(100ms)"),
    ("timespan", "30m", "timespan(30m)"),
    ("timespan", "45s", "timespan(45s)"),
    ("timespan", "1.5h", "timespan(1.5h)"),
    ("timespan", "9999999d", "timespan(9999999d)"),
    ("timespan", "-9999999.9999999d", "timespan(-9999999.9999999d)"),
    ("timespan", "00:30:00", "timespan(00:30:00)"),
    ("timespan", "23:59:59", "timespan(23:59:59)"),
    ("timespan", "1.02:03:04.5", "timespan(1.02:03:04.5)"),
    ("timespan", "-1.02:03:04", "timespan(-1.02:03:04)"),
    ("timespan", "9999999.23:59:59.9999999", "timespan(9999999.23:59:59.9999999)"),
    ("dynamic", [], "dynamic([])"),
    ("dynamic", ["TEXAS", "FLORIDA"], 'dynamic(["TEXAS","FLORIDA"])'),
    ("dynamic", [1, 2.5, True, None, -3, False], "dynamic([1,2.5,true,null,-3,false])"),
    ("dynamic", [9223372036854775807, -9223372036854775808],
     "dynamic([9223372036854775807,-9223372036854775808])"),
    ("dynamic", [1e300, 5e-324, -0.0], "dynamic([1e+300,5e-324,-0.0])"),
    ("dynamic", ['a"b', "c\\d", "x)y", "'q'", "line\nbreak", "e\x00f", "", "])|take 1"],
     'dynamic(["a\\"b","c\\\\d","x)y","\'q\'","line\\nbreak","e\\u0000f","","])|take 1"])'),
    ("dynamic", ["\u05e9\u05dc\u05d5\u05dd", "\U0001f600"],
     'dynamic(["\\u05e9\\u05dc\\u05d5\\u05dd","\\ud83d\\ude00"])'),
    ("string", "TEXAS", '"TEXAS"'),
    ("string", "", '""'),
    ("string", 'say "hi"', '"say \\"hi\\""'),
    ("string", "back\\slash", '"back\\\\slash"'),
    ("string", "line1\nline2\r\tend", '"line1\\nline2\\r\\tend"'),
    ("string", "a\x00b", '"a\\u0000b"'),
    ("string", "\x08\x0c\x1f\x7f\x85", '"\\u0008\\u000C\\u001F\\u007F\\u0085"'),
    ("string", "<>&'+`~ ", "\"<>&'+`~ \""),
    ("string", "\u212a\u00e9", '"\\u212A\\u00E9"'),
    ("string", "\u05e9\u05dc\u05d5\u05dd", '"\\u05E9\\u05DC\\u05D5\\u05DD"'),
    ("string", "\U0001f44b\U0001f3fd\u200d\U0001f4bb", '"\\uD83D\\uDC4B\\uD83C\\uDFFD\\u200D\\uD83D\\uDCBB"'),
    ("string", "\U0010ffff", '"\\uDBFF\\uDFFF"'),
    ("string", '"; StormEvents | take 1000 //', '"\\"; StormEvents | take 1000 //"'),
    ("long", 5, "long(5)"),
    ("long", 0, "long(0)"),
    ("long", -1, "long(-1)"),
    ("long", 9223372036854775807, "long(9223372036854775807)"),
    ("long", -9223372036854775808, "long(-9223372036854775808)"),
    ("real", 1.25, "real(1.25)"),
    ("real", 5, "real(5.0)"),
    ("real", -0.0, "real(-0.0)"),
    ("real", 0.1, "real(0.1)"),
    ("real", 1e16, "real(1e+16)"),
    ("real", 1.7976931348623157e308, "real(1.7976931348623157e+308)"),
    ("real", 5e-324, "real(5e-324)"),
    ("real", 9223372036854775809, "real(9.223372036854776e+18)"),
    ("bool", True, "true"),
    ("bool", False, "false"),
    ("datetime", "2007-07-01T00:00:00Z", "datetime(2007-07-01T00:00:00.0000000Z)"),
    ("datetime", "2007-07-01T00:00Z", "datetime(2007-07-01T00:00:00.0000000Z)"),
    ("datetime", "2007-07-01T00:00:00+05:30", "datetime(2007-06-30T18:30:00.0000000Z)"),
    ("datetime", "2007-07-01T12:34:56.789-08:00", "datetime(2007-07-01T20:34:56.7890000Z)"),
    ("datetime", "2007-07-01T00:00:00.1234567Z", "datetime(2007-07-01T00:00:00.1234567Z)"),
    ("datetime", "2007-07-01T23:59:59.9999999-00:30", "datetime(2007-07-02T00:29:59.9999999Z)"),
    ("datetime", "2026-10-21T09:00:00+02:00", "datetime(2026-10-21T07:00:00.0000000Z)"),
    ("datetime", "2024-02-29T00:00:00+14:00", "datetime(2024-02-28T10:00:00.0000000Z)"),
    ("datetime", "2024-02-29T23:00:00-14:00", "datetime(2024-03-01T13:00:00.0000000Z)"),
    ("datetime", "0001-01-01T00:00:00Z", "datetime(0001-01-01T00:00:00.0000000Z)"),
    ("datetime", "9999-12-31T23:59:59.9999999Z", "datetime(9999-12-31T23:59:59.9999999Z)"),
    ("guid", "4a6b0c8e-1d2f-4a3b-9c5d-7e8f9a0b1c2d", "guid(4a6b0c8e-1d2f-4a3b-9c5d-7e8f9a0b1c2d)"),
    ("guid", "4A6B0C8E-1D2F-4A3B-9C5D-7E8F9A0B1C2D", "guid(4a6b0c8e-1d2f-4a3b-9c5d-7e8f9a0b1c2d)"),
]


@pytest.mark.parametrize("kind,value,literal", LITERALS)
def test_values_become_typed_kusto_literals(app, monkeypatch, kind, value, literal):
    queries = {"Q": _template(text=f"declare query_parameters(v:{kind});\nprint v", parameters={"v": kind})}
    _, _, session = _accept(app, monkeypatch, _payload({"v": value}), settings=_settings(queries))
    assert _properties(session) == {"Options": READ_ONLY, "Parameters": {"v": literal}}


def _dynamic_template():
    return _template(text="declare query_parameters(v:dynamic);\nprint v", parameters={"v": "dynamic"})


def test_dynamic_boundaries(app, monkeypatch):
    settings = _settings({"Q": _dynamic_template()})
    _, _, session = _accept(app, monkeypatch, _payload({"v": list(range(1000))}), settings=settings)
    assert json.loads(_properties(session)["Parameters"]["v"][8:-1]) == list(range(1000))
    longest = ["x" * 99987]
    _, _, session = _accept(app, monkeypatch, _payload({"v": longest}), settings=settings)
    assert len(_properties(session)["Parameters"]["v"]) == 100000
    assert _code(app, monkeypatch, _payload({"v": list(range(1001))}), settings=settings) == (
        "INVALID_QUERY_INPUT"
    )
    assert _code(app, monkeypatch, _payload({"v": ["x" * 99988]}), settings=settings) == (
        "INVALID_QUERY_INPUT"
    )


def test_dynamic_literal_is_always_one_json_array(app, monkeypatch):
    values = ["]) | take 1000 //", ")", "dynamic([1]) | take 1", '"]);print 1;//']
    settings = _settings({"Q": _dynamic_template()})
    _, _, session = _accept(app, monkeypatch, _payload({"v": values}), settings=settings)
    literal = _properties(session)["Parameters"]["v"]
    assert literal.startswith("dynamic([") and literal.endswith("])")
    assert json.loads(literal[len("dynamic("):-1]) == values


def test_long_strings_are_accepted(app, monkeypatch):
    queries = {"Q": _template(text="declare query_parameters(v:string);\nprint v", parameters={"v": "string"})}
    _, _, session = _accept(app, monkeypatch, _payload({"v": "x" * 100000}), settings=_settings(queries))
    assert _properties(session)["Parameters"]["v"] == '"' + "x" * 100000 + '"'


INVALID_VALUES = [
    ("timespan", ""), ("timespan", "+"), ("timespan", "+7d"), ("timespan", "7"), ("timespan", 7),
    ("timespan", 7.5), ("timespan", True), ("timespan", None), ("timespan", ["7d"]),
    ("timespan", "7D"), ("timespan", "7 d"), ("timespan", " 7d"), ("timespan", "7d "),
    ("timespan", "7d\n"), ("timespan", "7days"), ("timespan", "7dd"), ("timespan", "1w"),
    ("timespan", "1y"), ("timespan", "1us"), ("timespan", "1tick"), ("timespan", "1microsecond"),
    ("timespan", "12345678d"), ("timespan", "1.12345678h"), ("timespan", ".5h"), ("timespan", "5.h"),
    ("timespan", "--1h"), ("timespan", "1.2.3h"), ("timespan", "P1D"), ("timespan", "PT1H"),
    ("timespan", "24:00:00"), ("timespan", "1:00:00"), ("timespan", "00:60:00"),
    ("timespan", "00:00:60"), ("timespan", "00:00"), ("timespan", "00:00:00.12345678"),
    ("timespan", "12345678.00:00:00"), ("timespan", "1.00:00:00."), ("timespan", "7d) | take 1"),
    ("timespan", "\uff17d"), ("timespan", "\u0667d"), ("timespan", f"{VALUE_MARKER}"),
    ("dynamic", ""), ("dynamic", "[]"), ("dynamic", '["a"]'), ("dynamic", None), ("dynamic", 5),
    ("dynamic", True), ("dynamic", {}), ("dynamic", {"a": 1}), ("dynamic", ("a",)),
    ("dynamic", [[1]]), ("dynamic", [{"a": 1}]), ("dynamic", [[]]), ("dynamic", [float("nan")]),
    ("dynamic", [float("inf")]), ("dynamic", [float("-inf")]), ("dynamic", [2 ** 63]),
    ("dynamic", [-(2 ** 63) - 1]), ("dynamic", ["\ud800"]), ("dynamic", ["a\udc00b"]),
    ("dynamic", [1, ["x"]]), ("dynamic", [VALUE_MARKER, {"x": 1}]),
    ("string", 5), ("string", True), ("string", ["a"]), ("string", {"a": 1}),
    ("string", "\ud800"), ("string", "a\udc00b"), ("string", "\udc00\ud800"),
    ("long", ""), ("long", "5"), ("long", 5.0), ("long", 5.5), ("long", True),
    ("long", 9223372036854775808), ("long", -9223372036854775809), ("long", [5]),
    ("long", f"5 | {VALUE_MARKER}"),
    ("real", ""), ("real", "1.5"), ("real", True), ("real", float("inf")),
    ("real", float("-inf")), ("real", float("nan")), ("real", 10 ** 400), ("real", [1.5]),
    ("bool", ""), ("bool", "true"), ("bool", 1), ("bool", 0), ("bool", [True]),
    ("datetime", ""), ("datetime", 5), ("datetime", "2007-07-01"), ("datetime", "2007-07-01T00:00:00"),
    ("datetime", "2007-07-01T00:00:00.123"), ("datetime", "2007-07-01 00:00:00Z"),
    ("datetime", "2007-07-01t00:00:00z"), ("datetime", "2007-07-01T00:00:00z"),
    ("datetime", "2007-07-01T00:00:00+0530"), ("datetime", "2007-07-01T00:00:00+05"),
    ("datetime", "2007-07-01T00:00:00+14:01"), ("datetime", "2007-07-01T00:00:00+15:00"),
    ("datetime", "2007-07-01T00:00:00+05:60"), ("datetime", "2007-07-01T00:00:00.12345678Z"),
    ("datetime", "2007-07-01T00:00:00.Z"), ("datetime", "2007-02-30T00:00:00Z"),
    ("datetime", "2007-13-01T00:00:00Z"), ("datetime", "2007-07-01T24:00:00Z"),
    ("datetime", "2007-07-01T00:60:00Z"), ("datetime", "2007-07-01T00:00:60Z"),
    ("datetime", "0000-01-01T00:00:00Z"), ("datetime", "0001-01-01T00:00:00+01:00"),
    ("datetime", "9999-12-31T23:59:59-01:00"), ("datetime", " 2007-07-01T00:00:00Z"),
    ("datetime", "2007-07-01T00:00:00Z\n"), ("datetime", "\uff12007-07-01T00:00:00Z"),
    ("datetime", "2007-07-01T00:00:00\u0660Z"), ("datetime", "07-07-01T00:00:00Z"),
    ("datetime", "+2007-07-01T00:00:00Z"), ("datetime", f"{VALUE_MARKER}"),
    ("guid", ""), ("guid", 5), ("guid", "{4a6b0c8e-1d2f-4a3b-9c5d-7e8f9a0b1c2d}"),
    ("guid", "4a6b0c8e1d2f4a3b9c5d7e8f9a0b1c2d"), ("guid", "4a6b0c8e-1d2f-4a3b-9c5d-7e8f9a0b1c2g"),
    ("guid", "4a6b0c8e-1d2f-4a3b-9c5d-7e8f9a0b1c2d\n"), ("guid", "(4a6b0c8e-1d2f-4a3b-9c5d-7e8f9a0b1c2d)"),
]


@pytest.mark.parametrize("kind,value", INVALID_VALUES)
def test_values_that_do_not_match_the_type_are_rejected(app, monkeypatch, kind, value):
    queries = {"Q": _template(text=f"declare query_parameters(v:{kind});\nprint v", parameters={"v": kind})}
    assert _code(app, monkeypatch, _payload({"v": value}), settings=_settings(queries)) == (
        "INVALID_QUERY_INPUT"
    )


def test_parameters_follow_the_template_order(app, monkeypatch):
    queries = {"Q": _template(text="print 1", parameters={"b": "long", "a": "bool"})}
    _, _, session = _accept(app, monkeypatch, _payload({"a": True, "b": 2}), settings=_settings(queries))
    assert list(_properties(session)["Parameters"].items()) == [("b", "long(2)"), ("a", "true")]


# --- client request ID -------------------------------------------------------------


def test_client_request_id_is_forwarded(app, monkeypatch):
    payload = _payload({"state": "TEXAS"}, clientRequestId="KPC.app;1234")
    _, _, session = _accept(app, monkeypatch, payload)
    assert session.captured_headers["x-ms-client-request-id"] == "KPC.app;1234"


@pytest.mark.parametrize("request_id", [
    "KPC.rayfin_eventhouse_v1;1234", "KPC.rayfin_kusto_v1;1234", "any;value",
])
def test_caller_request_ids_pass_through_unchanged(app, monkeypatch, request_id):
    payload = _payload({"state": "TEXAS"}, clientRequestId=request_id)
    _, _, session = _accept(app, monkeypatch, payload)
    assert session.captured_headers["x-ms-client-request-id"] == request_id


def test_client_request_id_defaults_to_the_function_prefix(app, monkeypatch):
    _, _, session = _accept(app, monkeypatch, _texas())
    prefix, _, suffix = session.captured_headers["x-ms-client-request-id"].partition(";")
    assert prefix == "KPC.rayfin_eventhouse_v1"
    assert len(suffix) == 36


@pytest.mark.parametrize("request_id", [
    None, 5, "", "   ", "a" * 257, "a\nb", "a\rb", "a\x00b", "a\x7fb", "\u00e9", "\u3000",
])
def test_invalid_client_request_ids_are_rejected(app, monkeypatch, request_id):
    payload = _payload({"state": "TEXAS"}, clientRequestId=request_id)
    assert _code(app, monkeypatch, payload) == "INVALID_QUERY_INPUT"


def test_longest_client_request_id_is_accepted(app, monkeypatch):
    payload = _payload({"state": "TEXAS"}, clientRequestId="a" * 256)
    _accept(app, monkeypatch, payload)


# --- execution -----------------------------------------------------------------------


@pytest.mark.parametrize("operation,route,text", [
    ("executeQuery", "query", FILTER_TEXT),
    ("executeCommand", "mgmt", ".show database schema as json"),
])
def test_request_is_bound_to_the_saved_template(app, monkeypatch, operation, route, text):
    parameters = {"state": "string"} if operation == "executeQuery" else {}
    values = {"state": "TEXAS"} if operation == "executeQuery" else {}
    queries = {"Q": _template(operation, text, parameters)}
    _, _, session = _accept(app, monkeypatch, _payload(values, operation=operation),
                            settings=_settings(queries))

    assert session.captured_url == f"{ENDPOINT}/v1/rest/{route}"
    assert session.captured_allow_redirects is False
    assert session.captured_headers["Authorization"] == "Bearer fake-kusto-token"
    assert session.captured_headers["Content-Type"] == "application/json"
    assert session.captured_headers["Accept"] == "application/json"
    assert set(session.captured_json) == {"db", "csl", "properties"}
    assert session.captured_json["db"] == DATABASE
    assert session.captured_json["csl"] == text
    expected = {"state": '"TEXAS"'} if values else {}
    assert _properties(session) == {"Options": READ_ONLY, "Parameters": expected}


@pytest.mark.parametrize("operation,route,text", [
    ("executeQuery", "query", "print 1"),
    ("executeCommand", "mgmt", ".show tables"),
])
def test_every_route_sends_read_only_and_local_only_options(app, monkeypatch, operation, route, text):
    queries = {"Q": _template(operation, text, {})}
    _, _, session = _accept(app, monkeypatch, _payload({}, operation=operation),
                            settings=_settings(queries))
    assert session.captured_url.endswith(f"/v1/rest/{route}")
    assert _properties(session)["Options"] == {
        "request_readonly": True,
        "request_readonly_hardline": True,
        "request_remote_entities_disabled": True,
    }


def test_values_never_change_the_template_text(app, monkeypatch):
    value = 'TEXAS" or 1==1 //\n| union StormEvents'
    _, _, session = _accept(app, monkeypatch, _payload({"state": value}))
    assert session.captured_json["csl"] == FILTER_TEXT
    assert json.loads(_properties(session)["Parameters"]["state"]) == value


def test_success_streams_chunks_unchanged(app, monkeypatch):
    chunks = [b'{"Tables":[', b'{"TableName":"Table_0","Rows":[[1]]}', b"]}"]
    response = _FakeResponse(chunks)
    result, body, _ = _accept(app, monkeypatch, _texas(), response=response)
    assert body == chunks
    assert result.media_type == "application/json"
    assert result.status_code in (None, 200)
    assert response.text_called is False
    assert response.released is True


@pytest.mark.parametrize("status", [301, 302, 307, 308, 400, 401, 403, 404, 429, 500, 503])
def test_non_success_status_is_relayed_without_following(app, monkeypatch, status):
    response = _ErrorResponse(status, '{"error":{"code":"Upstream"}}', "application/json; charset=utf-8")
    result, body, session = _accept(app, monkeypatch, _texas(), response=response)
    assert session.captured_allow_redirects is False
    assert result.status_code == status
    assert result.media_type == "application/json; charset=utf-8"
    assert body == [b'{"error":{"code":"Upstream"}}']
    assert response.released is True


@pytest.mark.parametrize("raw_input", [
    {"queryServiceUri": ENDPOINT, "databaseName": "db", "query": "T | take 1"},
    {"queryServiceUri": ENDPOINT, "databaseName": "db", "command": ".show tables"},
    {"query": "StormEvents | take 1000"},
    {"command": ".show databases"},
])
@pytest.mark.parametrize("operation", ["executeQuery", "executeCommand", None])
def test_raw_kql_requests_are_rejected(app, monkeypatch, raw_input, operation):
    payload = {"input": dict(raw_input)}
    if operation is not None:
        payload["operation"] = operation
    assert _code(app, monkeypatch, payload) == "INVALID_QUERY_INPUT"


ODD_VALUES = [None, True, 0, 1.5, "", "x", [], ["x"], {}, {"x": 1}]


def _odd_requests():
    for value in ODD_VALUES:
        for path in [
            ("payload",), ("payload", "operation"), ("payload", "input"),
            ("payload", "input", "queryId"), ("payload", "input", "parameters"),
            ("payload", "input", "parameters", "state"), ("payload", "input", "clientRequestId"),
            ("payload", "input", "itemId"), ("settings",), ("settings", "queryServiceUri"),
            ("settings", "database"), ("settings", "queries"), ("settings", "queries", "Q"),
            ("settings", "queries", "Q", "operation"), ("settings", "queries", "Q", "text"),
            ("settings", "queries", "Q", "parameters"), ("settings", "queries", "Q", "parameters", "state"),
            ("settings", "queries", "Q", "roles"), ("settings", "queries", "Q", "roles", 0),
            ("caller",), ("caller", "isAuthenticated"), ("caller", "roles"), ("caller", "roles", 0),
            ("binding",), ("binding", "operation"), ("binding", "function"),
        ]:
            yield path, value


@pytest.mark.parametrize("path,value", list(_odd_requests()))
def test_unexpected_json_types_never_raise(app, monkeypatch, path, value):
    request = {
        "payload": _payload({"state": "TEXAS"}, clientRequestId="id", itemId=DATABASE),
        "settings": _settings(),
        "caller": _caller(["authenticated"]),
        "binding": _binding(_texas()),
    }
    if len(path) == 1:
        request[path[0]] = value
    else:
        container = request[path[0]]
        for key in path[1:-1]:
            container = container[key]
        container[path[-1]] = value

    session = _Session(_FakeResponse([b"{}"]))

    async def get_session():
        return session

    monkeypatch.setattr(app, "_get_session", get_session)

    async def invoke():
        result = await app.rayfin_eventhouse_v1(
            _host(request["payload"], request["settings"], request["caller"], request["binding"]),
            _FakeKustoClient(),
        )
        if hasattr(result.body, "__aiter__"):
            return result, [chunk async for chunk in result.body]
        return result, list(result.body)

    result, body = asyncio.run(invoke())
    if session.calls == 0:
        assert json.loads(b"".join(body))["error"]["code"]
    else:
        assert body == [b"{}"]
