"""Run with python -B test_data_agent.py.

Set DATA_AGENT_BASELINE_DIR to an exported base template directory to also
check peer statements and untouched archive members. No git refs are needed.
"""

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
        def __init__(self, body, media_type=None, status_code=200, headers=None):
            self.body, self.media_type, self.status_code = body, media_type, status_code
            self.headers = CIMultiDict(headers or {})

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


with _without_api_environment():
    APP = _load_function_app()
ROOT = Path(__file__).resolve().parent
TEST_ORIGIN = "https://powerbiapi.analysis-df.windows.net"
WORKSPACE = "11111111-1111-4111-8111-111111111111"
ITEM = "22222222-2222-4222-8222-222222222222"
TASK = "resp_opaque/not-a-guid"
INITIALIZE = {
    "protocolVersion": "2025-06-18",
    "serverInfo": {"name": "published-agent", "version": "1"},
    "capabilities": {"tasks": {"requests": {"tools": {"call": {}}}, "cancel": {}}},
}
TOOLS = {"tools": [{"name": "published_tool", "inputSchema": {
    "type": "object", "properties": {"userQuestion": {"type": "string"}},
    "required": ["userQuestion"],
}, "execution": {"taskSupport": "optional"}}]}
ANSWER = {
    "content": [{"type": "text", "text": "answer"}, {"type": "image", "data": "opaque"}],
    "structuredContent": {"artifactName": "Test agent", "deepLinkUrl": "https://example.test/answer"},
    "_meta": {"openai/outputTemplate": "ui://test", "unknown": [1, {"nested": True}]},
    "resultType": "complete",
}


def payload(operation="getInfo", **values):
    defaults = {"workspaceId": WORKSPACE, "itemId": ITEM}
    if operation == "startTask":
        defaults.update(toolName="published_tool", questionProperty="userQuestion", useTask=True, question="q")
    return {"operation": operation, "input": {**defaults, **values}}


def rpc(result, request_id):
    return {"jsonrpc": "2.0", "id": request_id, "result": copy.deepcopy(result)}


class Response:
    def __init__(self, document=None, *, status=200, content_type="application/json",
                 headers=None, chunks=None, failure=None):
        self.document, self.status = document, status
        self.headers = CIMultiDict({"Content-Type": content_type, **(headers or {})})
        self.chunks, self.failure = chunks, failure
        self.content, self.releases = self, 0

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


def handshake(include_tools=True, initialize=None, **kwargs):
    result = [
        reply(INITIALIZE if initialize is None else initialize, **kwargs),
        Response(status=202, content_type="text/plain", chunks=[b"Accepted"]),
    ]
    return result + [reply(TOOLS)] if include_tools else result


class Session:
    def __init__(self, responses, cleanup=None):
        self.responses, self.cleanup = responses, cleanup
        self.calls, self.deletes = [], []
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

    async def delete(self, url, **kwargs):
        self.deletes.append((url, copy.deepcopy(kwargs)))
        if isinstance(self.cleanup, BaseException):
            raise self.cleanup
        return self.cleanup or Response(status=204, chunks=[])


class Client:
    def __init__(self, token="test-token"):
        self.token, self.calls = token, 0

    def get_access_token(self):
        self.calls += 1
        return self

    def get_token(self):
        return self


async def body_bytes(response):
    if hasattr(response.body, "__aiter__"):
        return b"".join([chunk async for chunk in response.body])
    return b"".join(response.body)


async def decode(response):
    return json.loads(await body_bytes(response))


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.info = self.enterContext(patch.object(logging, "info"))
        self.warning = self.enterContext(patch.object(logging, "warning"))

    async def invoke(self, data=None, responses=None, client=None, cleanup=None):
        session = Session(handshake() if responses is None else responses, cleanup)
        client = client or Client()
        shared = types.SimpleNamespace(connector=object())
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
        return result, session, client

    async def test_get_info_handshake_binding_and_cleanup(self):
        result, session, client = await self.invoke(responses=handshake(headers={"Mcp-Session-Id": "session-A"}))
        self.assertEqual(await decode(result), {"initialize": INITIALIZE, "tools": TOOLS})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(client.calls, 1)
        self.assertEqual([call[1]["json"]["method"] for call in session.calls],
                         ["initialize", "notifications/initialized", "tools/list"])
        self.assertNotIn("id", session.calls[1][1]["json"])
        self.assertNotIn("Mcp-Session-Id", session.calls[0][1]["headers"])
        for index, (url, call) in enumerate(session.calls):
            self.assertEqual(url, f"{TEST_ORIGIN}/v1/mcp/workspaces/{WORKSPACE}/dataagents/{ITEM}/agent")
            self.assertFalse(call["allow_redirects"])
            self.assertEqual(call["headers"]["Authorization"], "Bearer test-token")
            self.assertEqual(call["headers"]["Accept"], "application/json, text/event-stream")
            self.assertGreater(call["timeout"].sock_read, 0)
            if index:
                self.assertEqual(call["headers"]["Mcp-Session-Id"], "session-A")
                self.assertEqual(call["headers"]["MCP-Protocol-Version"], "2025-06-18")
        self.assertEqual(len(session.deletes), 1)
        self.assertEqual(session.deletes[0][1]["headers"]["Mcp-Session-Id"], "session-A")
        self.assertFalse(session.deletes[0][1]["allow_redirects"])
        signature = inspect.signature(APP.rayfin_data_agent_v1)
        self.assertEqual(list(signature.parameters), ["payload", "fabricClient"])
        self.assertTrue(all(p.default is inspect.Parameter.empty for p in signature.parameters.values()))

    async def test_start_task_relays_sdk_selection_without_discovery_or_prompt_shaping(self):
        question = 'Prior question: "caf\u00e9\\n"\nNew question:\nQ?'
        for use_task in (True, False):
            for ttl in ({}, {"ttl": 1200}):
                for answer in (ANSWER, {"task": {"taskId": TASK, "status": "working"}}):
                    with self.subTest(use_task=use_task, ttl=ttl, answer=answer):
                        result, session, _ = await self.invoke(
                            payload("startTask", question=question, toolName="sdk_tool",
                                    questionProperty="prompt", useTask=use_task, **ttl),
                            handshake(False) + [reply(answer)])
                        self.assertEqual((await decode(result))["result"], answer)
                        self.assertEqual(len(session.calls), 3)
                        params = session.calls[-1][1]["json"]["params"]
                        self.assertEqual(params["name"], "sdk_tool")
                        self.assertEqual(params["arguments"], {"prompt": question})
                        self.assertEqual("task" in params, use_task)
                        if use_task:
                            self.assertEqual(params["task"], ttl)
                        self.assertEqual(session.deletes, [])

    async def test_history_and_old_sdk_fields_fail_loudly_before_credentials(self):
        for history in (None, [], {}, "old", [{"role": "user", "content": "old"}]):
            result, session, client = await self.invoke(payload("startTask", history=history))
            self.assertEqual(result.status_code, 400)
            self.assertIn("upgrade", (await decode(result))["message"])
            self.assertEqual((session.calls, client.calls), ([], 0))
        for name in ("toolName", "questionProperty", "useTask"):
            data = payload("startTask")
            del data["input"][name]
            result, session, client = await self.invoke(data)
            self.assertEqual(result.status_code, 400)
            self.assertIn("upgrade", (await decode(result))["message"])
            self.assertEqual((session.calls, client.calls), ([], 0))

    async def test_extra_fields_are_ignored_not_used_for_routing_or_authentication(self):
        result, session, _ = await self.invoke(payload(
            mcpEndpoint="https://untrusted.test", accessToken="caller-token",
            headers={"Authorization": "caller-token"}, refresh="unused"))
        self.assertEqual(result.status_code, 200)
        self.assertTrue(session.calls[0][0].startswith(TEST_ORIGIN + "/v1/mcp/"))
        self.assertEqual(session.calls[0][1]["headers"]["Authorization"], "Bearer test-token")

    async def test_task_operations_retain_handshake_and_preserve_raw_results(self):
        for operation, method in (("getTask", "tasks/get"), ("getTaskResult", "tasks/result"),
                                  ("cancelTask", "tasks/cancel")):
            for value in (ANSWER, {"taskId": TASK, "status": "cancelled"}, {"resultType": "complete"}):
                with self.subTest(operation=operation, value=value):
                    result, session, _ = await self.invoke(
                        payload(operation, taskId=TASK), handshake(False) + [reply(value)])
                    self.assertEqual((await decode(result))["result"], value)
                    self.assertEqual([call[1]["json"]["method"] for call in session.calls],
                                     ["initialize", "notifications/initialized", method])
                    self.assertEqual(session.calls[-1][1]["json"]["params"], {"taskId": TASK})

    async def test_json_rpc_errors_at_each_stage_remain_unclassified(self):
        for step in range(3):
            for status in (200, 202, 400):
                error = {"code": -32602, "message": "upstream", "data": {"unknown": "kept"}}
                responses = handshake(False) + [reply(ANSWER)]
                responses[step] = Response(
                    lambda msg: {"jsonrpc": "2.0", "id": msg.get("id"), "error": error}, status=status)
                result, session, _ = await self.invoke(payload("startTask"), responses)
                self.assertEqual(result.status_code, 400 if status == 400 else 200)
                self.assertEqual((await decode(result))["error"], error)
                self.assertEqual(len(session.calls), step + 1)
        result, _, _ = await self.invoke(payload("startTask"),
                                         handshake(False) + [reply({**ANSWER, "isError": True})])
        self.assertTrue((await decode(result))["result"]["isError"])

    async def test_input_and_guid_validation_precedes_credentials(self):
        invalid = [[], {}, {"operation": "getInfo", "input": []},
                   payload("ask"), payload("getTask", taskId=""), payload("getTask", taskId=3),
                   payload("startTask", useTask=1), payload("startTask", question=" "),
                   payload(clientRequestId="x\r\ninjected: value")]
        for name in ("workspaceId", "itemId"):
            invalid += [payload(**{name: value}) for value in (None, 123, "", WORKSPACE + "/../", "{" + WORKSPACE + "}")]
        for data in invalid:
            with self.subTest(data=data):
                result, session, client = await self.invoke(data)
                self.assertEqual(result.status_code, 400)
                self.assertEqual((session.calls, client.calls), ([], 0))

    async def test_environment_uses_exact_trusted_origins_before_credentials(self):
        for origin in APP._DATA_AGENT_ORIGINS:
            with self.subTest(origin=origin), patch.object(APP, "_POWERBI_BASE", origin + "/v1.0/myorg"):
                result, session, _ = await self.invoke()
                self.assertEqual(result.status_code, 200)
                self.assertTrue(session.calls[0][0].startswith(origin + "/v1/mcp/"))
        invalid = ["", "http://api.fabric.microsoft.com", TEST_ORIGIN + ":443",
                   "https://user@powerbiapi.analysis-df.windows.net", TEST_ORIGIN + "?x=y",
                   TEST_ORIGIN + "#x", TEST_ORIGIN + ".evil.test", TEST_ORIGIN + "\\@evil.test",
                   " " + TEST_ORIGIN, TEST_ORIGIN + "\n", "https://[::1]", "https://["]
        for origin in invalid:
            with self.subTest(origin=origin), patch.object(APP, "_POWERBI_BASE", origin):
                result, session, client = await self.invoke()
                self.assertEqual(result.status_code, 503)
                self.assertEqual((session.calls, client.calls), ([], 0))

    async def test_get_info_does_not_validate_discovery_policy(self):
        for tools in ({}, {"tools": []}, {"tools": [TOOLS["tools"][0]] * 2},
                      {**TOOLS, "nextCursor": "more"}):
            result, _, _ = await self.invoke(responses=handshake(False) + [reply(tools)])
            self.assertEqual((await decode(result))["tools"], tools)

    async def test_json_sse_multiline_split_utf8_and_202_responses(self):
        for status in (200, 202):
            for content_type in ("application/json", "text/event-stream; charset=utf-8"):
                response = reply(ANSWER, status=status, content_type=content_type)
                if content_type.startswith("text/event-stream"):
                    def prepare(msg):
                        answer = copy.deepcopy(ANSWER)
                        answer["content"][0]["text"] = "caf\u00e9"
                        raw = json.dumps(rpc(answer, msg["id"]), ensure_ascii=False)
                        stream = (": heartbeat\r\n\r\ndata: " + json.dumps({"jsonrpc": "2.0", "method": "notifications/progress"})
                                  + "\r\n\r\ndata: " + json.dumps(rpc({}, "other-id"))
                                  + "\r\n\r\ndata: " + raw + "\r\n\r\n").encode()
                        response.chunks = [stream[i:i + 1] for i in range(len(stream))]
                    response.document = prepare
                result, _, _ = await self.invoke(payload("startTask"), handshake(False) + [response])
                expected = "caf\u00e9" if content_type.startswith("text/event-stream") else "answer"
                self.assertEqual((await decode(result))["result"]["content"][0]["text"], expected)
        for ending in ("\n\n", "\r\r"):
            response = Response(content_type="text/event-stream")
            def prepare(msg):
                response.chunks = [f'data: {{"jsonrpc":"2.0",\ndata: "id":"{msg["id"]}","result":{{}}}}{ending}'.encode()]
            response.document = prepare
            result, _, _ = await self.invoke(payload("getTask", taskId=TASK), handshake(False) + [response])
            self.assertEqual((await decode(result))["result"], {})

    async def test_malformed_responses_and_acks_return_http_failure(self):
        for raw in (b"not JSON", b"\xff", b'{"n":NaN}', b'{"n":1e999}', b'{"n":' + b"9" * 5000 + b"}",
                    b"[]", b'{"jsonrpc":"2.0","id":"wrong","result":{}}'):
            result, _, _ = await self.invoke(payload("getTask", taskId=TASK),
                                             handshake(False) + [Response(chunks=[raw])])
            self.assertEqual(result.status_code, 502)
        for raw in ([], {"jsonrpc": "2.0", "id": "other", "result": {}},
                    {"jsonrpc": "2.0", "id": None, "error": {}}):
            result, _, _ = await self.invoke(responses=[reply(INITIALIZE), Response(raw, status=202)])
            self.assertEqual(result.status_code, 502)

    async def test_http_failures_preserve_status_body_and_end_to_end_headers(self):
        for status in (400, 401, 403, 404, 408, 413, 422, 429, 500, 504):
            raw = b"verbatim upstream \xff"
            response = Response(status=status, content_type="text/plain", chunks=[raw],
                                headers={"x-ms-request-id": "upstream", "Retry-After": "12",
                                         "Connection": "keep-alive, X-Hop", "X-Hop": "private",
                                         "Content-Length": "100", "Set-Cookie": "session=private"})
            result, session, _ = await self.invoke(payload("startTask"),
                                                   handshake(False) + [response])
            self.assertEqual(result.status_code, status)
            self.assertEqual(await body_bytes(result), raw)
            self.assertEqual(result.headers["Retry-After"], "12")
            self.assertEqual(result.headers["x-ms-request-id"], "upstream")
            self.assertEqual(result.headers["Content-Type"], "text/plain")
            for name in ("Content-Length", "Connection", "Set-Cookie", "X-Hop"):
                self.assertNotIn(name, result.headers)
            self.assertEqual(len(session.calls), 3)

    async def test_timeout_disconnect_cancellation_and_cleanup_preserve_primary_outcome(self):
        for failure, status in ((asyncio.TimeoutError(), 504), (aiohttp.ClientConnectionError("private"), 503),
                                (aiohttp.ClientPayloadError("private"), 502)):
            for in_body in (False, True):
                response = Response(chunks=[], failure=failure) if in_body else failure
                result, session, _ = await self.invoke(payload("startTask"),
                    handshake(False, headers={"Mcp-Session-Id": "id"}) + [response])
                self.assertEqual(result.status_code, status)
                self.assertNotIn("private", (await body_bytes(result)).decode())
                self.assertEqual(len(session.deletes), 1)
        for cleanup in (asyncio.TimeoutError(), aiohttp.ClientError(), Response(status=500, chunks=[]),
                        Response(status=405, chunks=[])):
            result, session, _ = await self.invoke(
                responses=handshake(headers={"Mcp-Session-Id": "id"}), cleanup=cleanup)
            self.assertEqual(result.status_code, 200)
            self.assertEqual((await decode(result))["tools"], TOOLS)
            self.assertEqual(len(session.deletes), 1)
        with self.assertRaises(asyncio.CancelledError):
            await self.invoke(payload("startTask"), handshake(False) + [asyncio.CancelledError()])
        with self.assertRaises(RuntimeError):
            await self.invoke(responses=[RuntimeError("programming error")])

    async def test_protocol_and_session_header_validation(self):
        for initialize in ({}, {**INITIALIZE, "protocolVersion": "unknown"}, {**INITIALIZE, "capabilities": []}):
            result, session, _ = await self.invoke(responses=handshake(initialize=initialize,
                                                                     headers={"Mcp-Session-Id": "id"}))
            self.assertEqual(result.status_code, 502)
            self.assertEqual(len(session.deletes), 1)
        for session_id in ("", "injected\r\nheader", "non ascii \u00e9"):
            result, session, _ = await self.invoke(responses=handshake(headers={"Mcp-Session-Id": session_id}))
            self.assertEqual(result.status_code, 502)
            self.assertEqual(session.deletes, [])

    async def test_credentials_and_logs_do_not_echo_private_inputs(self):
        for token in ("", None, "token\r\ninjected: value"):
            result, session, _ = await self.invoke(client=Client(token))
            self.assertEqual(result.status_code, 401)
            self.assertEqual(session.calls, [])
        await self.invoke(payload("startTask", question="private-question"),
                          handshake(False) + [reply(ANSWER)], Client("private-token"))
        await self.invoke(payload("unknown-private-operation"))
        logs = str(self.info.call_args_list) + str(self.warning.call_args_list)
        for forbidden in ("private", WORKSPACE, ITEM, "artifactName", "answer"):
            self.assertNotIn(forbidden, logs)

    async def test_limits_include_notifications_and_http_error_bodies(self):
        for status, chunks in ((200, [b"12345", b"67890"]), (500, [b"12345", b"67890"])):
            with patch.object(APP, "_MCP_MAX_RESPONSE_BYTES", 8):
                result, _, _ = await self.invoke(responses=[Response(status=status, chunks=chunks)])
            self.assertEqual(result.status_code, 413)
        with patch.object(APP, "_MCP_MAX_RESPONSE_BYTES", 64):
            result, _, _ = await self.invoke(responses=[Response(content_type="text/event-stream",
                chunks=[b'data: {"jsonrpc":"2.0","method":"notifications/x"}\n\n'] * 3)])
        self.assertEqual(result.status_code, 413)


class ArchiveTests(unittest.TestCase):
    def baseline(self):
        directory = os.environ.get("DATA_AGENT_BASELINE_DIR")
        if not directory:
            self.skipTest("Set DATA_AGENT_BASELINE_DIR to an exported base template for peer regression checks.")
        return Path(directory)

    def test_existing_app_statements_are_unchanged(self):
        before = ast.parse((self.baseline() / "function_app.py").read_bytes())
        after = ast.parse((ROOT / "function_app.py").read_bytes())
        remaining = iter(ast.dump(node) for node in after.body)
        for node in before.body:
            self.assertTrue(any(candidate == ast.dump(node) for candidate in remaining),
                            "Changed existing app statement: " + ast.dump(node)[:120])

    def test_packed_source_metadata_and_import(self):
        metadata = (ROOT / "functions.metadata").read_bytes().replace(b"\r\n", b"\n")
        definitions = json.loads(metadata)
        self.assertEqual({entry["name"] for entry in definitions}, {
            "rayfin_semantic_model_v1", "rayfin_office365users_v1", "rayfin_telemetry_v1", "rayfin_data_agent_v1"})
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
                self.assertFalse(any("test_" in member or "__pycache__" in member for member in archive.namelist()))
            for origin in (TEST_ORIGIN, "https://dailyapi.fabric.microsoft.com"):
                loader = zipimport.zipimporter(str(path))
                spec = importlib.util.spec_from_loader("function_app", loader)
                module = importlib.util.module_from_spec(spec)
                with _without_api_environment(), patch.dict(os.environ, {"POWERBI_API_BASE": origin + "/v1.0/myorg"}):
                    loader.exec_module(module)
                self.assertTrue(inspect.iscoroutinefunction(module.rayfin_data_agent_v1))
                session = Session(handshake())
                async def invoke_packed():
                    with patch.object(module, "_get_session", AsyncMock(return_value=types.SimpleNamespace(connector=None))), \
                            patch.object(module.aiohttp, "ClientSession", return_value=session):
                        return await decode(await module.rayfin_data_agent_v1(payload(), Client()))
                self.assertEqual(asyncio.run(invoke_packed()), {"initialize": INITIALIZE, "tools": TOOLS})
                self.assertTrue(all(url.startswith(origin + "/v1/mcp/") for url, _ in session.calls))

    def test_archives_preserve_peer_metadata_members_and_permissions(self):
        baseline = self.baseline()
        definitions = json.loads((ROOT / "functions.metadata").read_bytes())
        self.assertEqual([entry for entry in definitions if entry["name"] != "rayfin_data_agent_v1"],
                         json.loads((baseline / "functions.metadata").read_bytes()))
        for name in ("SourceCode.zip", "Deploy.zip"):
            with zipfile.ZipFile(baseline / name) as before, zipfile.ZipFile(ROOT / name) as after:
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
        self.records, self.blocked, self.unblock = [], asyncio.Event(), asyncio.Event()

        async def handler(request):
            message = {"method": "DELETE"} if request.method == "DELETE" else await request.json()
            self.records.append((message, dict(request.headers)))
            method = message["method"]
            session_id = "session-" + request.headers["Authorization"].split()[-1]
            if method == "initialize":
                result = web.json_response(rpc(INITIALIZE, message["id"]), headers={"Mcp-Session-Id": session_id})
                result.set_cookie("mcp-session", session_id)
                return result
            self.assertEqual(request.headers["Mcp-Session-Id"], session_id)
            if method == "DELETE":
                return web.Response(status=204)
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
            response = web.StreamResponse(status=202, headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            for chunk in (b": heartbeat\r\n\r\n", b"data: ", json.dumps(rpc(ANSWER, message["id"])).encode(), b"\r\n\r\n"):
                await response.write(chunk)
            await response.write_eof()
            return response

        app = web.Application()
        app.router.add_route("*", "/agent", handler)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.url = f"http://127.0.0.1:{self.runner.addresses[0][1]}/agent"
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
        self.assertFalse(self.shared.connector._acquired)
        await self.shared.close()

    async def test_real_aiohttp_concurrent_cookie_header_isolation_and_cleanup(self):
        async def call(user):
            return await decode(await APP.rayfin_data_agent_v1(payload("startTask"), Client(user)))
        results = await asyncio.gather(call("A"), call("B"))
        self.assertEqual([result["result"] for result in results], [ANSWER, ANSWER])
        self.assertEqual(len(self.records), 8)
        self.assertEqual(sum(m["method"] == "DELETE" for m, _ in self.records), 2)
        ids = [message["id"] for message, _ in self.records if "id" in message]
        self.assertEqual(len(ids), len(set(ids)))
        for message, headers in self.records:
            self.assertNotIn("Cookie", headers)
            self.assertNotIn("shared", str(headers))
            if message["method"] == "initialize":
                self.assertNotIn("Mcp-Session-Id", headers)

    async def test_real_aiohttp_redirect_is_not_followed(self):
        result = await APP.rayfin_data_agent_v1(payload("cancelTask", taskId=TASK), Client())
        self.assertEqual(result.status_code, 502)
        self.assertEqual(len(self.records), 4)

    async def test_real_aiohttp_cancellation_cleans_session_and_connection(self):
        pending = asyncio.create_task(APP.rayfin_data_agent_v1(payload("getTaskResult", taskId=TASK), Client()))
        await asyncio.wait_for(self.blocked.wait(), timeout=5)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertEqual(self.records[-1][0]["method"], "DELETE")

    async def test_actual_invocation_deadline(self):
        with patch.object(APP, "_MCP_TIMEOUT_SECONDS", 1):
            result = await APP.rayfin_data_agent_v1(payload("getTaskResult", taskId=TASK), Client())
        self.assertEqual(result.status_code, 504)
        self.assertTrue(self.blocked.is_set())
        self.assertEqual(self.records[-1][0]["method"], "DELETE")


class PeerAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_semantic_model_routes_and_byte_pump(self):
        for artifact in (None, "test-artifact"):
            response = Response(chunks=[b"first", b"second"])
            response.release = AsyncMock()
            session = types.SimpleNamespace(post=AsyncMock(return_value=response))
            data = {"input": {"workspaceId": WORKSPACE, "itemId": ITEM, "query": "EVALUATE {1}"}}
            if artifact:
                data["input"]["baasItemId"] = artifact
            with patch.object(APP, "_get_session", AsyncMock(return_value=session)):
                stream = await APP.rayfin_semantic_model_v1(data, "peer-token")
                self.assertEqual(await body_bytes(stream), b"firstsecond")
            response.release.assert_awaited_once()
            url = session.post.call_args.args[0]
            self.assertTrue(url.endswith(f"/models/{ITEM}/executeDaxQueriesInternal" if artifact
                                         else f"/datasets/{ITEM}/executeDaxQueries"))

    async def test_office365users_operation_mapping(self):
        connectors = types.ModuleType("azure.connectors")
        connectors.ConnectorException = type("ConnectorException", (Exception,), {})
        inputs = {"managerAsync": {"userId": "test-user"}, "userProfileAsync": {"userId": "test-user"},
                  "directReportsAsync": {"userId": "test-user"}, "relevantPeopleAsync": {"userId": "test-user"},
                  "myProfileAsync": {}, "searchUserAsync": {"searchTerm": "test"}}
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
