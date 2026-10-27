"""Production promotion tests adapted from the reviewed Daily Data Agent adapter.

DATA_AGENT_BASELINE_DIR optionally supplies an exported base template for peer
and archive-member preservation checks; tests never consult git refs.
"""

import ast
import asyncio
from contextlib import contextmanager
import importlib.util
import inspect
import json
import logging
import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch
import zipfile
import zipimport

import aiohttp
import fabric.functions as fn
from aiohttp import web

ROOT = Path(__file__).resolve().parent
TEST_ORIGIN = "https://powerbiapi.analysis-df.windows.net"
DEFAULT_ORIGIN = "https://api.fabric.microsoft.com"
RING_ROUTES = (
    ("TEST/EDOG", TEST_ORIGIN, TEST_ORIGIN),
    ("Daily", "https://dailyapi.powerbi.com", "https://dailyapi.fabric.microsoft.com"),
    ("DXT/BCDR", "https://dxtapi.powerbi.com", "https://dxtapi.fabric.microsoft.com"),
    ("MSIT", "https://api.powerbi.com", "https://api.fabric.microsoft.com"),
    ("Production", "https://api.powerbi.com", "https://api.fabric.microsoft.com"),
)
WORKSPACE = "11111111-1111-4111-8111-111111111111"
ITEM = "22222222-2222-4222-8222-222222222222"
INITIALIZE = {
    "protocolVersion": "2025-06-18", "serverInfo": {"name": "published-agent"},
    "capabilities": {"tasks": {"requests": {"tools": {"call": {}}}, "cancel": {}}},
}
TOOLS = {"tools": [{"name": "published_tool", "inputSchema": {
    "type": "object", "properties": {"userQuestion": {"type": "string"}}, "required": ["userQuestion"],
}, "execution": {"taskSupport": "optional"}}]}
ANSWER = {"content": [{"type": "text", "text": "answer"}], "_meta": {"unknown": True}}


@contextmanager
def _without_api_environment():
    previous = os.environ.pop("POWERBI_API_BASE", None)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("POWERBI_API_BASE", None)
        else:
            os.environ["POWERBI_API_BASE"] = previous


def _load_function_app():
    class UserDataFunctions:
        def generic_connection(self, **_kwargs):
            return lambda function: function

        def streaming_function(self):
            return lambda function: function

    # Bypass host invocation only; keep the real SDK response type and isolate the fixture.
    with patch.object(fn, "UserDataFunctions", UserDataFunctions):
        spec = importlib.util.spec_from_file_location("data_agent_test_app", ROOT / "function_app.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


with _without_api_environment():
    APP = _load_function_app()


def message(method, params=None, request_id="request-1"):
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": {} if params is None else params}


def payload(operation="getInfo", **values):
    methods = {"getInfo": "tools/list", "startTask": "tools/call", "getTask": "tasks/get",
               "getTaskResult": "tasks/result", "cancelTask": "tasks/cancel"}
    return {"operation": operation, "input": {
        "workspaceId": WORKSPACE, "itemId": ITEM,
        "message": message(methods.get(operation, "unknown")), **values,
    }}


def rpc(result, request_id):
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


class Client:
    def __init__(self, token="test-token"):
        self.token, self.calls = token, 0

    def get_access_token(self):
        self.calls += 1
        return self

    def get_token(self):
        return self


async def body_bytes(response):
    if hasattr(response.content, "__aiter__"):
        return b"".join([chunk async for chunk in response.content])
    return b"".join(response.content)


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.records, self.overrides, self.sessions = [], {}, []
        self.resolved = []
        self.unblock = asyncio.Event()
        self.blocked = asyncio.Event()
        self.info = self.enterContext(patch.object(logging, "info"))
        self.warning = self.enterContext(patch.object(logging, "warning"))

        async def handler(request):
            document = {"method": "DELETE"} if request.method == "DELETE" else await request.json()
            self.records.append((document, dict(request.headers)))
            method = document["method"]
            if method in self.overrides:
                result = self.overrides[method](request, document)
                return await result if inspect.isawaitable(result) else result
            session_id = "session-" + request.headers["Authorization"].split()[-1]
            if method == "initialize":
                result = web.json_response(rpc(INITIALIZE, document["id"]), headers={"Mcp-Session-Id": session_id})
                result.set_cookie("private-session", "must-not-forward")
                return result
            self.assertEqual(request.headers["Mcp-Session-Id"], session_id)
            if method == "DELETE":
                return web.Response(status=204)
            if method == "notifications/initialized":
                return web.Response(status=202, text="Accepted")
            value = TOOLS if method == "tools/list" else ANSWER
            return web.json_response(rpc(value, document["id"]))

        app = web.Application()
        app.router.add_route("*", "/agent", handler)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.url = f"http://127.0.0.1:{self.runner.addresses[0][1]}/agent"
        self.shared = aiohttp.ClientSession(headers={"Authorization": "shared-token"},
                                            cookies={"shared": "must-not-forward"})
        self.enterContext(patch.object(APP, "_get_session", AsyncMock(return_value=self.shared)))
        endpoint = APP._data_agent_endpoint

        def local_endpoint(data):
            self.resolved.append(endpoint(data))
            return self.url

        self.enterContext(patch.object(APP, "_data_agent_endpoint", side_effect=local_endpoint))
        session_class = aiohttp.ClientSession

        def session(**kwargs):
            result = session_class(**kwargs)
            self.sessions.append(result)
            return result

        self.enterContext(patch.object(APP.aiohttp, "ClientSession", side_effect=session))

    async def asyncTearDown(self):
        self.unblock.set()
        try:
            await self.runner.cleanup()
            self.assertTrue(all(session.closed for session in self.sessions))
            self.assertFalse(self.shared.connector._acquired)
        finally:
            for session in self.sessions:
                await session.close()
            await self.shared.close()

    async def invoke(self, data=None, client=None):
        response = await APP.rayfin_data_agent_v1(payload() if data is None else data, client or Client())
        return response, await body_bytes(response)

    async def test_handshake_relays_requested_json_rpc_without_an_envelope(self):
        response, body = await self.invoke()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(body), rpc(TOOLS, "request-1"))
        self.assertEqual([m["method"] for m, _ in self.records],
                         ["initialize", "notifications/initialized", "tools/list", "DELETE"])
        self.assertEqual(self.records[0][0]["id"], "initialize:request-1")
        self.assertEqual(self.records[0][0]["params"]["clientInfo"]["name"], "rayfin-dataagent")
        self.assertEqual(self.records[2][0], payload()["input"]["message"])
        signature = inspect.signature(APP.rayfin_data_agent_v1)
        self.assertEqual(list(signature.parameters), ["payload", "fabricClient"])
        self.assertTrue(all(p.default is inspect.Parameter.empty for p in signature.parameters.values()))

    async def test_initialize_metadata_is_a_separate_raw_get_info_response(self):
        request = message("initialize", {"protocolVersion": "2025-06-18", "clientInfo": {"name": "sdk"}, "capabilities": {}})
        response, body = await self.invoke(payload(message=request))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(body), rpc(INITIALIZE, request["id"]))
        self.assertEqual([m["method"] for m, _ in self.records],
                         ["initialize", "notifications/initialized", "DELETE"])
        self.assertEqual(self.records[0][0], request)

    async def test_sdk_message_params_and_unknown_fields_reach_mcp_unchanged(self):
        request = message("tools/call", {"name": "sdk_tool", "arguments": {"prompt": "composed\nquestion"},
                                        "task": {"ttl": 123}, "_meta": {"future": 1}})
        response, body = await self.invoke(payload("startTask", message=request, mcpEndpoint="https://bad.test"))
        self.assertEqual(json.loads(body), rpc(ANSWER, request["id"]))
        self.assertEqual(self.records[2][0], request)
        self.assertFalse(any(m["method"] == "tools/list" for m, _ in self.records))
        self.assertEqual(self.resolved[0], f"{DEFAULT_ORIGIN}/v1/mcp/workspaces/{WORKSPACE}/dataagents/{ITEM}/agent")

    async def test_rpc_method_cannot_bypass_the_authorized_connector_operation(self):
        for operation in ("getInfo", "getTask", "getTaskResult", "cancelTask"):
            client = Client()
            response, _ = await self.invoke(payload(operation, message=message("tools/call")), client)
            self.assertEqual(response.status_code, 400)
            self.assertEqual(client.calls, 0)
        self.assertEqual(self.records, [])

    async def test_legacy_wire_and_bad_paths_fail_before_credentials(self):
        bad = [[], {}, {"operation": "getInfo", "input": []},
               payload("startTask", history=[]), payload(history=None), payload(message=None),
               payload("startTask", message={"method": "tools/call"}), payload(workspaceId=WORKSPACE + "/../"),
               payload(itemId=None), payload(clientRequestId="bad\r\nheader")]
        legacy = payload("startTask", question="q", toolName="old", useTask=True)
        del legacy["input"]["message"]
        bad.append(legacy)
        for data in bad:
            client = Client()
            response, body = await self.invoke(data, client)
            self.assertEqual(response.status_code, 400)
            self.assertIn("message", json.loads(body))
            self.assertEqual(client.calls, 0)
        self.assertEqual(self.records, [])

    async def test_configured_powerbi_origins_route_to_correct_fabric_hosts(self):
        for ring, powerbi, fabric in RING_ROUTES:
            with self.subTest(ring=ring), patch.object(APP, "_POWERBI_BASE", powerbi + "/v1.0/myorg"):
                response, _ = await self.invoke()
                self.assertEqual(response.status_code, 200)
                self.assertEqual(self.resolved[-1], f"{fabric}/v1/mcp/workspaces/{WORKSPACE}/dataagents/{ITEM}/agent")
        for origin in ("", "http://api.powerbi.com", "https://api.powerbi.com:443", "https://api.powerbi.com.evil.test",
                       "https://user@api.powerbi.com", TEST_ORIGIN + "?x=y", TEST_ORIGIN + "#x",
                       TEST_ORIGIN + "\\@evil.test", TEST_ORIGIN + "\n", " " + TEST_ORIGIN, "https://[",
                       "https://api.fabric.microsoft.com", "https://msitapi.fabric.microsoft.com"):
            with self.subTest(origin=origin), patch.object(APP, "_POWERBI_BASE", origin):
                client = Client()
                response, _ = await self.invoke(client=client)
                self.assertEqual(response.status_code, 503)
                self.assertEqual(client.calls, 0)

    async def test_upstream_status_content_type_and_bytes_are_relayed_verbatim(self):
        for status in (200, 202, 400, 401, 403, 404, 429, 500, 504):
            for content_type, raw in (("application/json", b'{"unfamiliar":true}'),
                                      ("text/event-stream", b": note\r\ndata: untouched\xff\r\n\r\n"),
                                      ("text/plain", b"upstream bytes")):
                with self.subTest(status=status, content_type=content_type):
                    self.overrides["tools/call"] = lambda _r, _m: web.Response(status=status, body=raw,
                                                        headers={"Content-Type": content_type})
                    response, body = await self.invoke(payload("startTask"))
                    self.assertEqual(response.status_code, status)
                    self.assertEqual(response.media_type, content_type)
                    self.assertEqual(body, raw)
                    self.assertEqual(self.records[-1][0]["method"], "DELETE")

    async def test_handshake_errors_are_relayed_without_sending_the_requested_method(self):
        for method in ("initialize", "notifications/initialized"):
            self.records.clear()
            self.overrides.clear()
            error = {"jsonrpc": "2.0", "id": None, "error": {"code": -32602, "message": "failed"}}
            self.overrides[method] = lambda _r, _m: web.json_response(error)
            response, body = await self.invoke(payload("startTask"))
            self.assertEqual(response.status_code, 200)
            self.assertEqual(json.loads(body), error)
            self.assertFalse(any(m["method"] == "tools/call" for m, _ in self.records))

    async def test_redirects_are_not_followed(self):
        self.overrides["tools/call"] = lambda _r, _m: web.Response(status=307, headers={"Location": self.url + "/elsewhere"})
        response, _ = await self.invoke(payload("startTask"))
        self.assertEqual(response.status_code, 502)
        self.assertEqual(len(self.records), 4)

    async def test_initialize_validation_is_owned_by_the_sdk(self):
        for field, value in (("id", "unrelated"), ("jsonrpc", "invalid"),
                             ("result", {**INITIALIZE, "protocolVersion": "future-version"})):
            self.records.clear()
            self.overrides["initialize"] = lambda _r, m: web.json_response({
                **rpc(INITIALIZE, m["id"]), field: value}, headers={"Mcp-Session-Id": "session-test-token"})
            response, body = await self.invoke(payload(message=message("initialize")))
            self.assertEqual(response.status_code, 200)
            self.assertEqual(json.loads(body)[field], value)
            self.assertEqual([m["method"] for m, _ in self.records],
                             ["initialize", "notifications/initialized", "DELETE"])

    async def test_non_json_initialize_is_a_clear_502_and_never_sends_the_requested_message(self):
        for content_type, raw in (("text/event-stream", b"data: {}\n\n"),
                                  ("text/plain", b"upstream"),
                                  ("application/json", b"{broken"),
                                  ("application/json", b"\xff")):
            self.records.clear()
            self.overrides["initialize"] = lambda _r, _m: web.Response(
                body=raw, headers={"Content-Type": content_type, "Mcp-Session-Id": "session-test-token"})
            response, body = await self.invoke(payload("startTask"))
            self.assertEqual(response.status_code, 502)
            self.assertEqual(json.loads(body)["message"], "MCP initialize did not return JSON.")
            self.assertEqual([m["method"] for m, _ in self.records], ["initialize", "DELETE"])

    async def test_non_json_initialize_http_failure_preserves_the_upstream_response(self):
        self.overrides["initialize"] = lambda _r, _m: web.Response(
            status=503, text="upstream unavailable",
            headers={"Mcp-Session-Id": "session-test-token"})
        response, body = await self.invoke(payload("startTask"))
        self.assertEqual(response.status_code, 503)
        self.assertEqual(body, b"upstream unavailable")
        self.assertTrue(response.media_type.startswith("text/plain"))
        self.assertEqual([m["method"] for m, _ in self.records], ["initialize", "DELETE"])

    async def test_stream_is_unbuffered_and_cancellation_cleans_the_session(self):
        async def stalled(request, _message):
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            await response.write(b": first\n\n")
            self.blocked.set()
            await self.unblock.wait()
            return response
        self.overrides["tasks/result"] = stalled
        response = await APP.rayfin_data_agent_v1(payload("getTaskResult"), Client())
        self.assertEqual(await anext(response.content), b": first\n\n")
        self.assertFalse(self.sessions[-1].closed)
        await response.content.aclose()
        self.assertTrue(self.sessions[-1].closed)
        self.assertEqual(self.records[-1][0]["method"], "DELETE")

    async def test_deadline_bounds_response_consumption_and_releases_connection(self):
        async def stalled(request, _message):
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            await response.write(b": waiting\n\n")
            self.blocked.set()
            await self.unblock.wait()
            return response
        self.overrides["tasks/result"] = stalled
        with patch.object(APP, "_MCP_TIMEOUT_SECONDS", 1):
            with self.assertRaises(asyncio.TimeoutError):
                await self.invoke(payload("getTaskResult"))
        self.assertTrue(self.blocked.is_set())
        self.assertEqual(self.records[-1][0]["method"], "DELETE")

    async def test_cleanup_failure_does_not_replace_the_response(self):
        for status in (405, 500):
            self.overrides["DELETE"] = lambda _r, _m: web.Response(status=status)
            response, body = await self.invoke()
            self.assertEqual(response.status_code, 200)
            self.assertEqual(json.loads(body)["result"], TOOLS)
        self.warning.assert_called()

    async def test_invocations_isolate_headers_and_cookies_and_do_not_log_inputs(self):
        await asyncio.gather(self.invoke(payload("startTask"), Client("private-A")),
                             self.invoke(payload("startTask"), Client("private-B")))
        for msg, headers in self.records:
            self.assertNotIn("Cookie", headers)
            self.assertNotIn("shared", str(headers))
            if msg["method"] == "initialize":
                self.assertNotIn("Mcp-Session-Id", headers)
        logs = str(self.info.call_args_list) + str(self.warning.call_args_list)
        self.assertNotIn("private", logs)


class ArchiveTests(unittest.TestCase):
    def baseline(self):
        directory = os.environ.get("DATA_AGENT_BASELINE_DIR")
        if not directory:
            self.skipTest("Set DATA_AGENT_BASELINE_DIR for exported-base regression checks.")
        return Path(directory)

    def test_existing_app_statements_are_unchanged(self):
        before = ast.parse((self.baseline() / "function_app.py").read_bytes())
        after = ast.parse((ROOT / "function_app.py").read_bytes())
        remaining = iter(ast.dump(node) for node in after.body)
        for node in before.body:
            self.assertTrue(any(value == ast.dump(node) for value in remaining), ast.dump(node)[:100])

    def test_packed_source_metadata_and_import(self):
        metadata = (ROOT / "functions.metadata").read_bytes().replace(b"\r\n", b"\n")
        definitions = json.loads(metadata)
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
            loader = zipimport.zipimporter(str(path))
            spec = importlib.util.spec_from_loader("function_app", loader)
            module = importlib.util.module_from_spec(spec)
            with _without_api_environment():
                loader.exec_module(module)
            self.assertEqual(module._POWERBI_BASE, "https://api.powerbi.com/v1.0/myorg")
            self.assertEqual(module._data_agent_endpoint(payload()["input"]),
                             f"{DEFAULT_ORIGIN}/v1/mcp/workspaces/{WORKSPACE}/dataagents/{ITEM}/agent")
            for ring, powerbi, fabric in RING_ROUTES:
                loader = zipimport.zipimporter(str(path))
                spec = importlib.util.spec_from_loader("function_app", loader)
                module = importlib.util.module_from_spec(spec)
                with _without_api_environment(), patch.dict(os.environ, {"POWERBI_API_BASE": powerbi + "/v1.0/myorg"}):
                    loader.exec_module(module)
                self.assertIn("rayfin_data_agent_v1", {f.get_function_name() for f in module.udf.get_functions()})
                self.assertEqual(module._data_agent_endpoint(payload()["input"]),
                                 f"{fabric}/v1/mcp/workspaces/{WORKSPACE}/dataagents/{ITEM}/agent", (name, ring))

    def test_archives_preserve_peer_metadata_members_and_permissions(self):
        baseline = self.baseline()
        definitions = json.loads((ROOT / "functions.metadata").read_bytes())
        baseline_definitions = json.loads((baseline / "functions.metadata").read_bytes())
        baseline_names = {d["name"] for d in baseline_definitions}
        self.assertEqual([d for d in definitions if d["name"] in baseline_names],
                         baseline_definitions)
        for name in ("SourceCode.zip", "Deploy.zip"):
            with zipfile.ZipFile(baseline / name) as before, zipfile.ZipFile(ROOT / name) as after:
                self.assertEqual(before.namelist(), after.namelist())
                self.assertEqual(before.comment, after.comment)
                for entry in before.infolist():
                    packed = after.getinfo(entry.filename)
                    for attr in ("external_attr", "internal_attr", "create_system", "create_version",
                                 "extract_version", "date_time", "compress_type", "comment", "extra"):
                        self.assertEqual(getattr(entry, attr), getattr(packed, attr))
                    if entry.filename not in ("function_app.py", "fabric_lib/functions.metadata"):
                        self.assertEqual(before.read(entry), after.read(entry.filename))


    def test_metadata_matches_real_runtime_binding(self):
        spec = importlib.util.spec_from_file_location("data_agent_binding_app", ROOT / "function_app.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        definitions = json.loads((ROOT / "functions.metadata").read_bytes())
        entry = next(d for d in definitions if d["name"] == "rayfin_data_agent_v1")
        functions = {f.get_function_name(): f for f in module.udf.get_functions()}
        self.assertEqual(set(functions), {d["name"] for d in definitions})
        bindings = functions[entry["name"]].get_dict_repr()["bindings"]
        self.assertEqual(next(b for b in bindings if b["type"] == "httpTrigger")["route"], entry["name"])
        binding = next(b for b in bindings if b["type"] == "FabricItem")
        self.assertEqual(binding["name"], "fabricClient")
        self.assertEqual(binding["audienceType"], "Fabric")


if __name__ == "__main__":
    unittest.main()
