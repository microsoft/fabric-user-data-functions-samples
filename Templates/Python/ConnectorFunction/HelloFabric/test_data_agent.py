"""Deterministic adapter tests; run with python -B test_data_agent.py."""

import ast
import asyncio
import copy
from contextlib import contextmanager
import importlib.util
import inspect
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch
import zipfile
import zipimport

import aiohttp
from aiohttp import web
from multidict import CIMultiDict


@contextmanager
def _without_api_environment():
    # Touch only these settings: clearing/restoring the whole Windows environment
    # can drop empty-valued Git configuration entries inherited by the test runner.
    previous = {name: os.environ.pop(name, None) for name in ("FABRIC_API_BASE", "POWERBI_API_BASE")}
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _load_function_app():
    fabric = types.ModuleType("fabric")
    functions = types.ModuleType("fabric.functions")

    class UserDataFunctions:
        def generic_connection(self, **_kwargs):
            return lambda function: function

        def streaming_function(self):
            return lambda function: function

    class StreamResponse:
        def __init__(self, body, media_type=None, status_code=None):
            self.body = body
            self.media_type = media_type
            self.status_code = status_code

    functions.UserDataFunctions = UserDataFunctions
    functions.StreamResponse = StreamResponse
    functions.FabricItem = type("FabricItem", (), {})
    fabric.functions = functions
    sys.modules["fabric"] = fabric
    sys.modules["fabric.functions"] = functions
    spec = importlib.util.spec_from_file_location("function_app", Path(__file__).with_name("function_app.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeResponse:
    def __init__(self, chunks, status=200):
        self.chunks = chunks
        self.status = status
        self.headers = {}
        self.content = self
        self.text_called = False
        self.released = False

    async def iter_any(self):
        for chunk in self.chunks:
            yield chunk

    async def text(self):
        self.text_called = True
        return b"".join(self.chunks).decode()

    async def release(self):
        # Existing semantic-model behavior; the new adapter uses sync release.
        self.released = True


class _FakeSession:
    def __init__(self, response):
        self.response = response
        self.captured_headers = None
        self.captured_url = None

    async def post(self, url, json=None, headers=None):
        self.captured_url = url
        self.captured_headers = headers
        return self.response


with _without_api_environment():
    APP = _load_function_app()
ROOT = Path(__file__).resolve().parent
BASELINE_REF = "origin/connector-function-v1"
TEST_ORIGIN = "https://powerbiapi.analysis-df.windows.net"
WORKSPACE = "11111111-1111-4111-8111-111111111111"
ITEM = "22222222-2222-4222-8222-222222222222"
TASK = "resp_opaque/not-a-guid"
INITIALIZE = {
    "protocolVersion": "2025-06-18",
    "serverInfo": {"name": "published-agent", "version": "1"},
    "capabilities": {"tasks": {"requests": {"tools": {"call": {}}}, "cancel": {}}},
}
TOOLS = {"tools": [{
    "name": "published_tool",
    "inputSchema": {
        "type": "object",
        "properties": {"optionalHint": {"type": "string"}, "userQuestion": {"type": "string"}},
        "required": ["userQuestion"],
    },
    "execution": {"taskSupport": "optional"},
}]}
ANSWER = {
    "content": [{"type": "text", "text": "answer"}, {"type": "image", "data": "opaque"}],
    "structuredContent": {"artifactName": "Test agent", "deepLinkUrl": "https://example.test/answer"},
    "_meta": {"openai/outputTemplate": "ui://test", "unknown": [1, {"nested": True}]},
    "resultType": "complete",
}


def payload(operation="getInfo", **values):
    return {"operation": operation, "input": {"workspaceId": WORKSPACE, "itemId": ITEM, **values}}


def rpc(result, request_id):
    return {"jsonrpc": "2.0", "id": request_id, "result": copy.deepcopy(result)}


class Response:
    def __init__(self, document=None, *, status=200, content_type="application/json",
                 headers=None, chunks=None, failure=None):
        self.document = document
        self.status = status
        self.headers = CIMultiDict({"Content-Type": content_type, **(headers or {})})
        self.chunks = chunks
        self.failure = failure
        self.content = self
        self.releases = 0

    def prepare(self, message):
        document = self.document(message) if callable(self.document) else self.document
        if self.chunks is None:
            self.chunks = [json.dumps(document).encode("utf-8")]

    async def iter_any(self):
        for chunk in self.chunks:
            await asyncio.sleep(0)
            yield chunk
        if self.failure:
            raise self.failure

    def release(self):
        self.releases += 1


def reply(result, **kwargs):
    return Response(lambda message: rpc(result, message.get("id")), **kwargs)


def handshake(initialize=None, tools=None, **kwargs):
    return [
        reply(INITIALIZE if initialize is None else initialize, **kwargs),
        Response(status=202, content_type="text/plain", chunks=[b"Accepted"]),
        reply(TOOLS if tools is None else tools),
    ]


class Session:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def post(self, url, **kwargs):
        self.calls.append((url, copy.deepcopy(kwargs)))
        response = self.responses[len(self.calls) - 1]
        if isinstance(response, BaseException):
            raise response
        response.prepare(kwargs["json"])
        return response


class Client:
    def __init__(self, token="test-token"):
        self.token = token
        self.calls = 0

    def get_access_token(self):
        self.calls += 1
        return self

    def get_token(self):
        return self


async def decode(response):
    if hasattr(response.body, "__aiter__"):
        chunks = [chunk async for chunk in response.body]
    else:
        chunks = list(response.body)
    assert response.media_type == "application/json"
    assert response.status_code == 200
    return json.loads(b"".join(chunks))


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.info = self.enterContext(patch.object(logging, "info"))
        self.warning = self.enterContext(patch.object(logging, "warning"))

    async def invoke(self, data=None, responses=None, client=None):
        session = Session(handshake() if responses is None else responses)
        client = client or Client()
        shared = type("Shared", (), {"connector": object()})()
        with patch.object(APP, "_get_session", AsyncMock(return_value=shared)), \
                patch.object(APP.aiohttp, "ClientSession", return_value=session) as factory:
            result = await APP.rayfin_data_agent_v1(payload() if data is None else data, client)
        if factory.called:
            self.assertIs(factory.call_args.kwargs["connector"], shared.connector)
            self.assertFalse(factory.call_args.kwargs["connector_owner"])
            self.assertIsInstance(factory.call_args.kwargs["cookie_jar"], aiohttp.DummyCookieJar)
            self.assertTrue(session.closed)
        for response in session.responses[:len(session.calls)]:
            if isinstance(response, Response):
                self.assertEqual(response.releases, 1)
        return await decode(result), session, client

    async def test_get_info_handshake_and_binding(self):
        result, session, client = await self.invoke(
            responses=handshake(headers={"Mcp-Session-Id": "session-A"}))
        self.assertEqual(result, {"initialize": INITIALIZE, "tools": TOOLS})
        self.assertEqual(client.calls, 1)
        calls = [call[1] for call in session.calls]
        self.assertEqual([call["json"]["method"] for call in calls],
                         ["initialize", "notifications/initialized", "tools/list"])
        self.assertEqual(calls[0]["json"]["params"]["capabilities"],
                         {"tasks": {"requests": {"tools": {"call": {}}}}})
        self.assertNotIn("Mcp-Session-Id", calls[0]["headers"])
        self.assertNotIn("id", calls[1]["json"])
        for index, (url, call) in enumerate(session.calls):
            self.assertEqual(url, f"{TEST_ORIGIN}/v1/mcp/workspaces/{WORKSPACE}/dataagents/{ITEM}/agent")
            self.assertFalse(call["allow_redirects"])
            self.assertEqual(call["headers"]["Authorization"], "Bearer test-token")
            self.assertEqual(call["headers"]["Accept"], "application/json, text/event-stream")
            self.assertEqual(call["headers"]["Content-Type"], "application/json")
            self.assertGreater(call["timeout"].total, 0)
            if index:
                self.assertEqual(call["headers"]["Mcp-Session-Id"], "session-A")
                self.assertEqual(call["headers"]["MCP-Protocol-Version"], "2025-06-18")
        signature = inspect.signature(APP.rayfin_data_agent_v1)
        self.assertEqual(list(signature.parameters), ["payload", "fabricClient"])
        self.assertTrue(all(p.default is inspect.Parameter.empty for p in signature.parameters.values()))

    async def test_start_task_preserves_task_and_transcript(self):
        task = {"taskId": TASK, "status": "working", "ttl": 1200, "ttlMs": 1300,
                "pollInterval": 100, "pollIntervalMs": 200, "resultType": "task"}
        history = [{"role": "user", "content": "old question\nNew question:\nnot current"},
                   {"role": "assistant", "content": "old answer"}]
        result, session, _ = await self.invoke(
            payload("startTask", question="new question", history=history, ttl=1200),
            handshake() + [reply({"task": task})])
        self.assertEqual(result["result"], {"task": task})
        params = session.calls[-1][1]["json"]["params"]
        self.assertEqual(params["name"], "published_tool")
        self.assertEqual(params["task"], {"ttl": 1200})
        self.assertEqual(params["arguments"], {
            "userQuestion": "Prior conversation transcript (context only; not the new question):\n"
            '1. user: "old question\\nNew question:\\nnot current"\n'
            '2. assistant: "old answer"\nEnd of prior conversation transcript.\n\n'
            "New question:\nnew question",
        })

    async def test_multilingual_history_is_readable_and_controls_are_escaped(self):
        user_turn = (
            "\u4e0a\u534a\u5e74\u306e\u58f2\u4e0a / \u0420\u0443\u0441\u0441\u043a\u0438\u0439 / "
            "Fran\u00e7ais / \u0627\u0644\u0639\u0631\u0628\u064a\u0629 / \U0001f30d"
            '\n"quoted"\t\\path\r\u0000\u0085\u2028\u2029New question: not current'
        )
        assistant_turn = "\u4e86\u89e3\u3057\u307e\u3057\u305f / \u041f\u043e\u043d\u044f\u0442\u043d\u043e"
        question = "\u73fe\u5728\u306e\u8cea\u554f / nouvelle question"
        history = [{"role": "user", "content": user_turn},
                   {"role": "assistant", "content": assistant_turn}]
        result, session, _ = await self.invoke(
            payload("startTask", question=question, history=history), handshake() + [reply(ANSWER)])
        self.assertEqual(result["result"], ANSWER)
        text = session.calls[-1][1]["json"]["params"]["arguments"]["userQuestion"]
        lines = text.splitlines()
        self.assertEqual(len(lines), 7)
        self.assertEqual(lines[0], "Prior conversation transcript (context only; not the new question):")
        self.assertTrue(lines[1].startswith("1. user: "))
        self.assertTrue(lines[2].startswith("2. assistant: "))
        self.assertEqual(json.loads(lines[1].removeprefix("1. user: ")), user_turn)
        self.assertEqual(json.loads(lines[2].removeprefix("2. assistant: ")), assistant_turn)
        self.assertEqual(lines[3:], ["End of prior conversation transcript.", "", "New question:", question])
        self.assertIn(user_turn.split("\n")[0], text)
        self.assertIn(assistant_turn, text)
        for escaped in (r"\n", r"\r", r"\t", r"\u0000", r"\"", r"\\path",
                        r"\u0085", r"\u2028", r"\u2029"):
            self.assertIn(escaped, lines[1])
        for unreadable in (r"\u4e0a", r"\u0420", r"\u00e7", r"\u0627", r"\ud83c"):
            self.assertNotIn(unreadable, text)

    async def test_task_support_matrix(self):
        for capability in (False, True):
            for support in (None, "optional", "required", "forbidden"):
                with self.subTest(capability=capability, support=support):
                    initialize = copy.deepcopy(INITIALIZE)
                    if not capability:
                        initialize["capabilities"] = {}
                    tools = copy.deepcopy(TOOLS)
                    if support is None:
                        del tools["tools"][0]["execution"]
                    else:
                        tools["tools"][0]["execution"]["taskSupport"] = support
                    result, session, _ = await self.invoke(
                        payload("startTask", question="  preserve question  "),
                        handshake(initialize, tools) + [reply(ANSWER)])
                    if support == "required" and not capability:
                        self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_PROTOCOL_MISMATCH")
                        self.assertEqual(len(session.calls), 3)
                    else:
                        self.assertEqual(result["result"], ANSWER)
                        params = session.calls[-1][1]["json"]["params"]
                        self.assertEqual(params["arguments"]["userQuestion"], "  preserve question  ")
                        self.assertEqual("task" in params, capability and support in ("optional", "required"))
                        if "task" in params:
                            self.assertEqual(params["task"], {})

    async def test_task_operations_and_raw_errors(self):
        for operation, method in (("getTask", "tasks/get"), ("getTaskResult", "tasks/result"),
                                  ("cancelTask", "tasks/cancel")):
            for raw in ({"taskId": TASK, "status": "cancelled", "ttlMs": 10},
                        ANSWER, {"resultType": "complete"}):
                with self.subTest(operation=operation, raw=raw):
                    result, session, _ = await self.invoke(
                        payload(operation, taskId=TASK), handshake()[:2] + [reply(raw)])
                    self.assertEqual(result["result"], raw)
                    self.assertEqual(session.calls[-1][1]["json"]["method"], method)
                    self.assertEqual(session.calls[-1][1]["json"]["params"], {"taskId": TASK})
        error = {"code": -32601, "message": "not supported", "data": {"unknown": 1}}
        result, _, _ = await self.invoke(payload("cancelTask", taskId=TASK), handshake()[:2] + [
            Response(lambda msg: {"jsonrpc": "2.0", "id": msg["id"], "error": error})])
        self.assertEqual(result["error"], error)

    async def test_all_setup_and_tool_errors_are_preserved(self):
        for step in range(4):
            for status in (200, 202, 400):
                with self.subTest(step=step, status=status):
                    error = {"code": -32602, "message": "upstream", "data": {"artifactName": "name"}}
                    responses = handshake() + [reply(ANSWER)]
                    responses[step] = Response(
                        lambda msg: {"jsonrpc": "2.0", "id": msg.get("id"), "error": error},
                        status=status)
                    result, session, _ = await self.invoke(payload("startTask", question="question"), responses)
                    self.assertEqual(result["error"], error)
                    self.assertEqual(len(session.calls), step + 1)
        flagged = {**ANSWER, "isError": True}
        result, _, _ = await self.invoke(payload("startTask", question="question"), handshake() + [reply(flagged)])
        self.assertEqual(result["result"], flagged)

    async def test_input_rejection_before_credentials_or_network(self):
        bad = [[], {}, {"operation": "getInfo", "input": []}]
        for operation in ("ask", "executeQuery", "executeCommand", "", " startTask", None, []):
            bad.append(payload(operation))
        for key in ("mcpEndpoint", "accessToken", "accesstoken", "headers", "action",
                    "apiBase", "host", "connectionRuntimeUrl", "queryServiceUri", "sessionId"):
            bad += [payload(**{key: "untrusted"}), {**payload(), key: "untrusted"}]
        for key in ("workspaceId", "itemId"):
            for value in (None, 123, "", "not-guid", WORKSPACE + "/../", "{" + WORKSPACE + "}"):
                bad.append(payload(**{key: value}))
        for question in (None, 123, "", " \n"):
            bad.append(payload("startTask", question=question))
        for history in (None, {}, "old", [None], [{"role": "system", "content": "x"}],
                        [{"role": "user", "content": 1}], [{"role": "user", "content": "x", "url": "x"}]):
            bad.append(payload("startTask", question="q", history=history))
        for ttl in (None, -1, 0, 1.5, True, "1", float("nan"), float("inf")):
            bad.append(payload("startTask", question="q", ttl=ttl))
        for task_id in ("", " ", None, 5):
            bad.append(payload("getTask", taskId=task_id))
        bad += [payload(refresh="yes"), payload(clientRequestId="x\r\ninjected: value")]
        for data in bad:
            with self.subTest(data=data):
                result, session, client = await self.invoke(data)
                self.assertTrue(result["errors"])
                self.assertEqual(session.calls, [])
                self.assertEqual(client.calls, 0)

    async def test_environment_origin_and_guid_validation(self):
        self.assertEqual(APP._DATA_AGENT_ORIGINS, {TEST_ORIGIN})
        _, session, _ = await self.invoke()
        self.assertTrue(session.calls[0][0].startswith(TEST_ORIGIN + "/v1/mcp/"))
        for origin in ("", "http://powerbiapi.analysis-df.windows.net", TEST_ORIGIN + "/",
                       TEST_ORIGIN + ":443", "https://user@powerbiapi.analysis-df.windows.net",
                       TEST_ORIGIN + "?x=y", TEST_ORIGIN + "#", TEST_ORIGIN + "?",
                       TEST_ORIGIN + ".evil.test", TEST_ORIGIN + "\\@evil.test",
                       " " + TEST_ORIGIN, TEST_ORIGIN + "\n", "https://[::1]",
                       "https://api.fabric.microsoft.com", "https://dailyapi.fabric.microsoft.com",
                       "https://dxtapi.fabric.microsoft.com", "https://msitapi.fabric.microsoft.com",
                       "https://onebox-redirect.analysis.windows-int.net"):
            with self.subTest(origin=origin), patch.object(APP, "_FABRIC_API_BASE", origin):
                result, session, client = await self.invoke()
                self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_UNAVAILABLE")
                self.assertEqual(session.calls, [])
                self.assertEqual(client.calls, 0)

    async def test_branch_defaults_work_without_deployment_environment_setting(self):
        with _without_api_environment():
            module = _load_function_app()
        self.assertEqual(module._FABRIC_API_BASE, TEST_ORIGIN)
        self.assertEqual(module._POWERBI_BASE, TEST_ORIGIN + "/v1.0/myorg")
        session = Session(handshake())
        shared = type("Shared", (), {"connector": object()})()
        with patch.object(module, "_get_session", AsyncMock(return_value=shared)), \
                patch.object(module.aiohttp, "ClientSession", return_value=session):
            self.assertEqual(await decode(await module.rayfin_data_agent_v1(payload(), Client())),
                             {"initialize": INITIALIZE, "tools": TOOLS})
        self.assertTrue(all(url.startswith(TEST_ORIGIN + "/v1/mcp/") for url, _ in session.calls))

    async def test_source_and_archives_ignore_environment_routing_overrides(self):
        for origin in ("https://dxtapi.fabric.microsoft.com", "https://msitapi.fabric.microsoft.com",
                       "https://api.fabric.microsoft.com", "https://dailyapi.fabric.microsoft.com",
                       "https://untrusted.test"):
            with self.subTest(origin=origin), _without_api_environment():
                os.environ["FABRIC_API_BASE"] = origin
                os.environ["POWERBI_API_BASE"] = origin + "/v1.0/myorg"
                modules = [_load_function_app()]
                for name in ("SourceCode.zip", "Deploy.zip"):
                    loader = zipimport.zipimporter(str(ROOT / name))
                    spec = importlib.util.spec_from_loader("function_app", loader)
                    module = importlib.util.module_from_spec(spec)
                    loader.exec_module(module)
                    modules.append(module)
                for module in modules:
                    self.assertEqual(module._FABRIC_API_BASE, TEST_ORIGIN)
                    self.assertEqual(module._DATA_AGENT_ORIGINS, {TEST_ORIGIN})
                    session = Session(handshake())
                    shared = type("Shared", (), {"connector": object()})()
                    with patch.object(module, "_get_session", AsyncMock(return_value=shared)), \
                            patch.object(module.aiohttp, "ClientSession", return_value=session):
                        self.assertEqual(await decode(await module.rayfin_data_agent_v1(payload(), Client())),
                                         {"initialize": INITIALIZE, "tools": TOOLS})
                    self.assertTrue(all(url.startswith(TEST_ORIGIN + "/v1/mcp/") for url, _ in session.calls))

    async def test_schema_rejection(self):
        invalid = [{}, {"tools": []}, {"tools": [TOOLS["tools"][0]] * 2},
                   {**TOOLS, "nextCursor": "another-page"}]
        for change in ({}, {"type": "array"}, {"type": "object", "properties": []},
                       {"type": "object", "properties": {"question": {"type": "number"}}, "required": ["question"]},
                       {"type": "object", "properties": {"q": {"type": "string"}}, "required": "q"},
                       {"type": "object", "properties": {"q": {"type": "string"}}, "required": []},
                       {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "string"}},
                        "required": ["a", "b"]},
                       {"type": "object", "properties": {"q": {"type": "string"}}, "required": ["q", "missing"]}):
            tool = copy.deepcopy(TOOLS["tools"][0])
            tool["inputSchema"] = change
            invalid.append({"tools": [tool]})
        for tools in invalid:
            with self.subTest(tools=tools):
                result, session, _ = await self.invoke(payload("startTask", question="q"), handshake(tools=tools))
                self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_PROTOCOL_MISMATCH")
                self.assertEqual(len(session.calls), 3)

    async def test_json_sse_and_202_responses(self):
        for content_type in ("application/json", "text/event-stream; charset=utf-8"):
            for status in (200, 202):
                responses = handshake() + [reply(ANSWER, status=status, content_type=content_type)]
                if content_type.startswith("text/event-stream"):
                    def sse(msg):
                        message = json.dumps(rpc(ANSWER, msg["id"]), ensure_ascii=False).replace("answer", "caf\u00e9")
                        stream = (": heartbeat\r\n\r\ndata: " + json.dumps({"jsonrpc": "2.0", "method": "notifications/progress"})
                                  + "\r\n\r\ndata: " + json.dumps(rpc({}, "other-id"))
                                  + "\r\n\r\nevent: message\r\ndata: " + message + "\r\n\r\n").encode("utf-8")
                        responses[-1].chunks = [stream[i:i + 1] for i in range(len(stream))]
                    responses[-1].document = sse
                with self.subTest(content_type=content_type, status=status):
                    result, _, _ = await self.invoke(payload("startTask", question="q"), responses)
                    if content_type.startswith("text/event-stream"):
                        self.assertEqual(result["result"]["content"][0]["text"], "caf\u00e9")
                    else:
                        self.assertEqual(result["result"], ANSWER)
        for chunks in ([b"data: {\"jsonrpc\":\"2.0\",\ndata: \"id\":\"ID\",\"result\":{}}\n\n"],
                       [b"data: {\"jsonrpc\":\"2.0\",\"id\":\"ID\",\"result\":{}}\r\r"]):
            response = Response(content_type="text/event-stream", chunks=chunks)
            def prepare(msg):
                response.chunks = [chunk.replace(b"ID", msg["id"].encode()) for chunk in response.chunks]
            response.document = prepare
            result, _, _ = await self.invoke(payload("getTask", taskId=TASK), handshake()[:2] + [response])
            self.assertEqual(result["result"], {})

    async def test_malformed_responses_and_acks(self):
        responses = [
            Response(chunks=[b"not JSON"]),
            Response(chunks=[b"\xff"]),
            Response(chunks=[b'{"jsonrpc":"2.0","id":1,"result":{"n":NaN}}']),
            Response(chunks=[b'{"jsonrpc":"2.0","id":1,"result":{"n":1e999}}']),
            Response(chunks=[b'{"jsonrpc":"2.0","id":1,"result":{"n":' + b"9" * 5000 + b"}}"]),
            Response(document=[]),
            Response(document={"jsonrpc": "2.0", "id": "wrong", "result": {}}),
            Response(lambda msg: {"jsonrpc": "2.0", "id": msg["id"], "result": [], "error": {}}),
            Response(status=202, content_type="text/plain", chunks=[b"Accepted"]),
            Response(status=204, chunks=[]),
            Response(content_type="text/event-stream", chunks=[b"data: invalid\n\n"]),
            Response(content_type="text/event-stream", chunks=[b'data: {"jsonrpc":"2.0","method":"notifications/x"}\n\n']),
        ]
        for response in responses:
            with self.subTest(response=response):
                result, _, _ = await self.invoke(payload("getTask", taskId=TASK), handshake()[:2] + [response])
                self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_INVALID_RESPONSE")

    async def test_http_failures_correlation_and_retry_safety(self):
        for status, code in ((301, "INVALID_RESPONSE"), (400, "INVALID_QUESTION"),
                             (401, "FORBIDDEN"), (403, "FORBIDDEN"), (404, "NOT_FOUND"),
                             (408, "TIMEOUT"), (413, "RESULT_TOO_LARGE"), (422, "INVALID_QUESTION"),
                             (429, "THROTTLED"), (500, "UNAVAILABLE"), (504, "TIMEOUT")):
            response = Response(status=status, content_type="text/html", chunks=[b"private upstream detail"],
                                headers={"x-ms-request-id": "upstream-correlation", "Retry-After": "12",
                                         "Location": "https://untrusted.test/"})
            result, session, _ = await self.invoke(payload("startTask", question="private question",
                                                          clientRequestId="client-correlation"),
                                                    handshake() + [response])
            error = result["errors"][0]
            self.assertEqual(error["code"], "DATA_AGENT_" + code)
            self.assertEqual(error["httpStatus"], status)
            self.assertEqual(error["correlationId"], "upstream-correlation")
            self.assertEqual(error["retryAfterSeconds"], 12)
            self.assertFalse(error["retryable"])
            self.assertNotIn("private", json.dumps(result))
            self.assertEqual(len(session.calls), 4)
            self.assertTrue(all(c[1]["headers"]["x-ms-client-request-id"] == "client-correlation"
                                for c in session.calls))

    async def test_timeouts_disconnects_cancellation_and_unknown_errors(self):
        for failure, code in ((asyncio.TimeoutError(), "TIMEOUT"),
                              (aiohttp.ClientConnectionError("private"), "UNAVAILABLE"),
                              (aiohttp.ClientPayloadError("private"), "INVALID_RESPONSE")):
            for during_body in (True, False):
                response = Response(chunks=[], failure=failure) if during_body else failure
                result, session, _ = await self.invoke(payload("startTask", question="q"),
                                                       handshake() + [response])
                self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_" + code)
                self.assertEqual(len(session.calls), 4)
        response = Response(chunks=[], failure=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await self.invoke(payload("getTask", taskId=TASK), handshake()[:2] + [response])
        self.assertEqual(response.releases, 1)
        with self.assertRaises(RuntimeError):
            await self.invoke(responses=[RuntimeError("programming error")])

    async def test_protocol_and_session_validation(self):
        for initialize in ({}, {**INITIALIZE, "protocolVersion": "unknown"},
                           {**INITIALIZE, "capabilities": []}):
            result, session, _ = await self.invoke(responses=handshake(initialize=initialize))
            self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_PROTOCOL_MISMATCH")
            self.assertEqual(len(session.calls), 1)
        for session_id in ("", "injected\r\nheader", "non ascii \u00e9"):
            result, _, _ = await self.invoke(responses=handshake(headers={"Mcp-Session-Id": session_id}))
            self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_INVALID_RESPONSE")

    async def test_malformed_capabilities_and_execution(self):
        for tasks in (None, [], {"requests": []}, {"requests": {"tools": []}},
                      {"requests": {"tools": {"call": True}}}):
            initialize = {**INITIALIZE, "capabilities": {"tasks": tasks}}
            result, _, _ = await self.invoke(responses=handshake(initialize=initialize))
            self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_PROTOCOL_MISMATCH")
        for execution in (None, [], {"taskSupport": "unsupported"}, {"taskSupport": True}):
            tools = copy.deepcopy(TOOLS)
            tools["tools"][0]["execution"] = execution
            result, _, _ = await self.invoke(responses=handshake(tools=tools))
            self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_PROTOCOL_MISMATCH")

    async def test_notification_json_is_not_an_empty_ack(self):
        for document in ({"jsonrpc": "2.0", "id": "unrelated", "result": {}}, [],
                         {"jsonrpc": "2.0", "id": None, "error": {}}):
            responses = handshake()
            responses[1] = Response(document=document, status=202)
            result, session, _ = await self.invoke(responses=responses)
            self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_INVALID_RESPONSE")
            self.assertEqual(len(session.calls), 2)

    async def test_invalid_credentials_and_safe_logging(self):
        for token in ("", None, "token\r\ninjected: value"):
            result, session, _ = await self.invoke(client=Client(token))
            self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_FORBIDDEN")
            self.assertEqual(session.calls, [])
        await self.invoke(payload("startTask", question="private-question",
                                  history=[{"role": "user", "content": "private-history"}]),
                          handshake() + [reply(ANSWER)], Client("private-token"))
        await self.invoke(payload("unknown-private-operation"))
        logs = str(self.info.call_args_list) + str(self.warning.call_args_list)
        for forbidden in ("private", "artifactName", WORKSPACE, ITEM, "answer"):
            self.assertNotIn(forbidden, logs)

    async def test_size_limit(self):
        with patch.object(APP, "_DATA_AGENT_MAX_RESPONSE_BYTES", 8):
            result, _, _ = await self.invoke(responses=[Response(chunks=[b"12345", b"67890"])])
        self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_RESULT_TOO_LARGE")

    async def test_no_history_means_unmodified_question(self):
        result, session, _ = await self.invoke(payload("startTask", question="q", history=[]),
                                               handshake() + [reply(ANSWER)])
        self.assertEqual(result["result"], ANSWER)
        self.assertEqual(session.calls[-1][1]["json"]["params"]["arguments"], {"userQuestion": "q"})

    async def test_invocations_do_not_share_headers(self):
        async def call(token, session_id):
            return await self.invoke(responses=handshake(headers={"Mcp-Session-Id": session_id}),
                                     client=Client(token))
        for token, session_id in (("user-A", "session-A"), ("user-B", "session-B")):
            _, session, _ = await call(token, session_id)
            self.assertNotIn("Mcp-Session-Id", session.calls[0][1]["headers"])
            self.assertEqual(session.calls[-1][1]["headers"]["Mcp-Session-Id"], session_id)
            self.assertEqual(session.calls[-1][1]["headers"]["Authorization"], "Bearer " + token)


class ArchiveTests(unittest.TestCase):
    def test_existing_app_statements_are_unchanged(self):
        repository = ROOT.parents[3]
        relative = (ROOT / "function_app.py").relative_to(repository).as_posix()
        before = ast.parse(subprocess.check_output(["git", "-C", str(repository), "show", BASELINE_REF + ":" + relative]))
        after = ast.parse((ROOT / "function_app.py").read_bytes())
        remaining = iter(ast.dump(node) for node in after.body)
        for node in before.body:
            self.assertTrue(any(candidate == ast.dump(node) for candidate in remaining),
                            "Changed existing app statement: " + ast.dump(node)[:120])
        udf_creations = [node for node in ast.walk(after) if isinstance(node, ast.Call)
                         and isinstance(node.func, ast.Attribute) and node.func.attr == "UserDataFunctions"]
        self.assertEqual(len(udf_creations), 1)
        self.assertFalse(any(isinstance(node, ast.AsyncFunctionDef) and node.name == "rayfin_kusto_v1"
                             for node in after.body))

    def test_packed_source_metadata_and_import(self):
        metadata = (ROOT / "functions.metadata").read_bytes().replace(b"\r\n", b"\n")
        definitions = json.loads(metadata)
        self.assertEqual({entry["name"] for entry in definitions},
                         {"rayfin_semantic_model_v1", "rayfin_office365users_v1", "rayfin_data_agent_v1"})
        repository = ROOT.parents[3]
        baseline = json.loads(subprocess.check_output([
            "git", "-C", str(repository), "show",
            BASELINE_REF + ":" + (ROOT / "functions.metadata").relative_to(repository).as_posix(),
        ]))
        self.assertEqual(definitions[:-1], baseline)
        entry = next(d for d in definitions if d["name"] == "rayfin_data_agent_v1")
        self.assertEqual(entry["fabricProperties"]["fabricFunctionParameters"], [{"name": "payload", "dataType": "dict"}])
        self.assertIn({"name": "fabricClient", "direction": "In", "type": "FabricItem", "audienceType": "Fabric"},
                      entry["bindings"])
        source = (ROOT / "function_app.py").read_bytes().replace(b"\r\n", b"\n")
        for name in ("SourceCode.zip", "Deploy.zip"):
            path = ROOT / name
            with zipfile.ZipFile(path) as archive:
                self.assertEqual(archive.read("function_app.py"), source)
                self.assertEqual(archive.read("fabric_lib/functions.metadata"), metadata)
                self.assertNotIn(b"\r\n", archive.read("function_app.py"))
                self.assertNotIn(b"\r\n", archive.read("fabric_lib/functions.metadata"))
                self.assertFalse(any("test_" in member or "__pycache__" in member
                                     or ".github/" in member or ".git/" in member
                                     for member in archive.namelist()))
            loader = zipimport.zipimporter(str(path))
            spec = importlib.util.spec_from_loader("function_app", loader)
            module = importlib.util.module_from_spec(spec)
            with _without_api_environment():
                loader.exec_module(module)
            self.assertEqual(module._FABRIC_API_BASE, TEST_ORIGIN)
            self.assertEqual(module._POWERBI_BASE, TEST_ORIGIN + "/v1.0/myorg")
            self.assertTrue(inspect.iscoroutinefunction(module.rayfin_data_agent_v1))
            session = Session(handshake())
            shared = type("Shared", (), {"connector": object()})()

            async def invoke_packed():
                with patch.object(module, "_get_session", AsyncMock(return_value=shared)), \
                        patch.object(module.aiohttp, "ClientSession", return_value=session):
                    return await decode(await module.rayfin_data_agent_v1(payload(), Client()))

            self.assertEqual(asyncio.run(invoke_packed()), {"initialize": INITIALIZE, "tools": TOOLS})
            self.assertTrue(all(url.startswith(TEST_ORIGIN + "/v1/mcp/") for url, _ in session.calls))

    def test_archives_preserve_existing_members_and_permissions(self):
        import io

        repository = ROOT.parents[3]
        for name in ("SourceCode.zip", "Deploy.zip"):
            relative = (ROOT / name).relative_to(repository).as_posix()
            original = subprocess.check_output(["git", "-C", str(repository), "show", BASELINE_REF + ":" + relative])
            with zipfile.ZipFile(io.BytesIO(original)) as before, zipfile.ZipFile(ROOT / name) as after:
                self.assertEqual(before.namelist(), after.namelist())
                self.assertEqual(before.comment, after.comment)
                for entry in before.infolist():
                    packed = after.getinfo(entry.filename)
                    for attr in ("external_attr", "internal_attr", "create_system", "create_version",
                                 "extract_version", "date_time", "compress_type", "comment", "extra"):
                        self.assertEqual(getattr(entry, attr), getattr(packed, attr), (name, entry.filename, attr))
                    if entry.filename not in ("function_app.py", "fabric_lib/functions.metadata"):
                        self.assertEqual(before.read(entry), after.read(entry.filename))


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.records = []
        self.blocked = asyncio.Event()

        async def handler(request):
            message = await request.json()
            self.records.append((message, dict(request.headers)))
            method = message["method"]
            user = request.headers["Authorization"]
            session_id = "session-" + user.split()[-1]
            if method == "initialize":
                result = web.json_response(rpc(INITIALIZE, message["id"]),
                                           headers={"Mcp-Session-Id": session_id})
                result.set_cookie("mcp-session", session_id)
                return result
            self.assertEqual(request.headers["Mcp-Session-Id"], session_id)
            if method == "notifications/initialized":
                return web.Response(status=202, text="Accepted")
            if method == "tasks/result":
                response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
                await response.prepare(request)
                await response.write(b": waiting\n\n")
                self.blocked.set()
                await self.unblock.wait()
                return response
            if method == "tasks/cancel":
                return web.Response(status=307, headers={"Location": self.url + "/redirect-target"})
            if method == "tools/list":
                return web.json_response(rpc(TOOLS, message["id"]))
            result = web.StreamResponse(status=202, headers={"Content-Type": "text/event-stream"})
            await result.prepare(request)
            for chunk in (b": heartbeat\r\n\r\n", b"data: ", json.dumps(rpc(ANSWER, message["id"])).encode(), b"\r\n\r\n"):
                await result.write(chunk)
            await result.write_eof()
            return result

        self.unblock = asyncio.Event()
        app = web.Application()
        app.router.add_post("/agent", handler)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = self.runner.addresses[0][1]
        self.url = f"http://127.0.0.1:{port}/agent"
        self.shared = aiohttp.ClientSession(headers={"Mcp-Session-Id": "shared-session", "Authorization": "shared-token"},
                                            cookies={"mcp-session": "shared-cookie"})
        self.enterContext(patch.object(APP, "_get_session", AsyncMock(return_value=self.shared)))
        self.enterContext(patch.object(APP, "_data_agent_endpoint", return_value=self.url))
        self.enterContext(patch.object(logging, "warning"))
        self.enterContext(patch.object(logging, "info"))

    async def asyncTearDown(self):
        self.unblock.set()
        await self.runner.cleanup()
        self.assertFalse(self.shared.closed)
        self.assertFalse(self.shared.connector.closed)
        self.assertFalse(self.shared.connector._acquired)
        await self.shared.close()

    async def test_real_aiohttp_concurrent_cookie_and_header_isolation(self):
        async def call(user):
            return await decode(await APP.rayfin_data_agent_v1(
                payload("startTask", question="q"), Client(user)))

        results = await asyncio.gather(call("A"), call("B"))
        for result in results:
            self.assertEqual(result["result"], ANSWER)
        self.assertEqual(len(self.records), 8)
        ids = [message["id"] for message, _ in self.records if "id" in message]
        self.assertEqual(len(ids), len(set(ids)))
        for message, headers in self.records:
            self.assertNotIn("Cookie", headers)
            self.assertNotIn("shared", str(headers))
            if message["method"] == "initialize":
                self.assertNotIn("Mcp-Session-Id", headers)

    async def test_real_aiohttp_redirect_is_not_followed(self):
        result = await decode(await APP.rayfin_data_agent_v1(payload("cancelTask", taskId=TASK), Client()))
        self.assertEqual(result["errors"][0]["httpStatus"], 307)
        self.assertEqual(len(self.records), 3)

    async def test_real_aiohttp_cancellation_releases_connection(self):
        pending = asyncio.create_task(APP.rayfin_data_agent_v1(payload("getTaskResult", taskId=TASK), Client()))
        await asyncio.wait_for(self.blocked.wait(), timeout=5)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending

    async def test_actual_invocation_deadline(self):
        with patch.object(APP, "_DATA_AGENT_TIMEOUT_SECONDS", 1):
            result = await decode(await APP.rayfin_data_agent_v1(payload("getTaskResult", taskId=TASK), Client()))
        self.assertEqual(result["errors"][0]["code"], "DATA_AGENT_TIMEOUT")
        self.assertTrue(self.blocked.is_set())


class PeerAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_semantic_model_routes_and_byte_pump(self):
        for artifact in (None, "test-artifact"):
            chunks = [b"first", b"second"]
            response = _FakeResponse(chunks)
            session = _FakeSession(response)
            data = {"input": {"workspaceId": WORKSPACE, "itemId": ITEM, "query": "EVALUATE {1}"}}
            if artifact:
                data["input"]["baasItemId"] = artifact
            with patch.object(APP, "_get_session", AsyncMock(return_value=session)):
                stream = await APP.rayfin_semantic_model_v1(data, "peer-token")
                self.assertEqual([chunk async for chunk in stream.body], chunks)
            self.assertFalse(response.text_called)
            self.assertTrue(response.released)
            self.assertEqual(stream.media_type, APP._ARROW_MEDIA_TYPE)
            if artifact:
                self.assertTrue(session.captured_url.endswith(f"/models/{ITEM}/executeDaxQueriesInternal"))
                self.assertEqual(session.captured_headers["X-Rayfin-ArtifactObjectId"], artifact)
            else:
                self.assertTrue(session.captured_url.endswith(f"/datasets/{ITEM}/executeDaxQueries"))

    async def test_office365users_operation_mapping(self):
        connectors = types.ModuleType("azure.connectors")
        connectors.ConnectorException = type("ConnectorException", (Exception,), {})
        inputs = {
            "managerAsync": {"userId": "test-user"},
            "userProfileAsync": {"userId": "test-user"},
            "directReportsAsync": {"userId": "test-user"},
            "relevantPeopleAsync": {"userId": "test-user"},
            "myProfileAsync": {},
            "searchUserAsync": {"searchTerm": "test"},
        }
        with patch.dict(sys.modules, {"azure.connectors": connectors}):
            for operation, values in inputs.items():
                client = types.SimpleNamespace()
                method, kwargs = APP._OFFICE365USERS_OPERATIONS[operation]
                handler = AsyncMock(return_value={"result": operation})
                setattr(client, method, handler)
                with patch.object(APP, "_get_office365users_client", AsyncMock(return_value=client)):
                    result = await APP.rayfin_office365users_v1({
                        "operation": operation,
                        "input": {"connectionRuntimeUrl": "https://example.test/connection", **values},
                    })
                self.assertEqual(await decode(result), {"result": operation})
                handler.assert_awaited_once_with(**kwargs(values))


if __name__ == "__main__":
    unittest.main()
