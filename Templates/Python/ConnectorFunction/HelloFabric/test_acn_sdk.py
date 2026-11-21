"""Contract regressions using the released azure-connectors==1.0.0 clients.

Run with python -B -m unittest discover -s . -p "test_acn*.py".
Only Fabric bindings and the HTTP send boundary are replaced; generated SDK
methods, paging, response parsing, credentials and exceptions are real.
"""

import importlib.util
import inspect
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

from azure.connectors.sdk import ConnectorResponseSnapshot
from azure.core.exceptions import ServiceRequestError, ServiceResponseTimeoutError

ROOT = Path(__file__).resolve().parent
RUNTIME_URL = "https://example.azure-apim.net/apim/planner/connection"
INPUT = {
    "userId": "u", "groupId": "g", "taskId": "t", "planId": "p",
    "siteUrl": "https://example.sharepoint.com/sites/test", "listName": "Tasks",
    "id": "7", "folderId": "folder", "fileId": "file", "driveId": "drive",
    "source": "me", "tableName": "Table1", "keyColumn": "ID", "keyValue": "7",
    "calendarId": "calendar", "messageId": "message", "teamId": "team",
    "channelId": "channel", "chatId": "chat", "meetingId": "meeting",
    "transcriptId": "transcript", "eventId": "event", "formId": "form", "responseId": "1",
    "path": "/test", "query": "test", "findMode": "Pattern", "size": "small",
    "poster": "Flow bot", "location": "Channel", "folderPath": "/",
    "name": "test.txt", "content": "dGVzdA==", "destination": "/copy.txt",
    "type": "view", "groupIds": ["g"], "securityEnabledOnly": False,
    "startDateTimeUtc": "2026-11-20T00:00:00Z", "endDateTimeUtc": "2026-11-21T00:00:00Z",
    "item": {"Title": "test"}, "request": {"title": "test", "assignments": "u"},
    "top": 3, "skip": 0, "maxFileCount": 5, "overwrite": False,
    "fetchOnlyUnread": False, "isSearchTermRequired": False,
}


def _load_app():
    fabric = types.ModuleType("fabric")
    functions = types.ModuleType("fabric.functions")

    class UserDataFunctions:
        def generic_connection(self, **kwargs):
            return lambda function: function

        def streaming_function(self):
            return lambda function: function

    class StreamResponse:
        def __init__(self, body, media_type=None, status_code=200, headers=None):
            self.body, self.status_code, self.headers = body, status_code, headers or {}

    functions.UserDataFunctions = UserDataFunctions
    functions.StreamResponse = StreamResponse
    functions.FabricItem = type("FabricItem", (), {})
    fabric.functions = functions
    with patch.dict(sys.modules, {"fabric": fabric, "fabric.functions": functions}):
        spec = importlib.util.spec_from_file_location("acn_sdk_app", ROOT / "function_app.py")
        app = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(app)
    return app


class AcnSdkTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.app = _load_app()

    def response(self, status=200, body=None):
        text = json.dumps(body) if body is not None else ""
        return ConnectorResponseSnapshot(status, {}, text, text.encode())

    async def invoke(self, slug, operation, client, **data):
        payload = {"operation": operation, "input": {
            "connectionRuntimeUrl": RUNTIME_URL, **data,
        }}
        with patch.object(self.app, "_get_acn_client", AsyncMock(return_value=client)):
            response = await getattr(self.app, f"rayfin_{slug}_v1")(payload)
        return response, json.loads(b"".join(response.body))

    async def test_every_admitted_operation_matches_the_released_sdk(self):
        for slug in self.app._ACN_CONNECTORS:
            client = self.app._new_acn_client(
                slug, RUNTIME_URL, self.app._AcnAccessTokenCredential("test-token"))
            tables = [table for name, table in vars(self.app).items()
                      if name == f"_{slug.upper()}_OPERATIONS"
                      or (name.startswith(f"_{slug.upper()}_OWNER_") and name.endswith("_OPERATIONS"))]
            try:
                for table in tables:
                    for operation, (method_name, build) in table.items():
                        with self.subTest(slug=slug, operation=operation):
                            method = getattr(client, method_name)
                            kwargs = build(INPUT)
                            signature = inspect.signature(method)
                            signature.bind(**kwargs)
                            for name, value in kwargs.items():
                                annotation = str(signature.parameters[name].annotation)
                                if value is not None and annotation in ("int", "Optional[int]", "bool", "Optional[bool]"):
                                    self.assertIs(type(value), bool if "bool" in annotation else int)
                            send = AsyncMock(return_value=self.response(body={"value": []}))
                            with patch.object(client.http_client, "send", send):
                                response, body = await self.invoke(slug, operation, client, **INPUT)
                            self.assertEqual(response.headers, {})
                            self.assertEqual(response.status_code, 200)
                            self.assertEqual(body, None if signature.return_annotation == "None" else {"value": []})
                            self.assertEqual(send.await_count, 1)
            finally:
                await client.close()

    async def test_planner_empty_successful_writes_do_not_become_errors(self):
        client = self.app._new_acn_client(
            "planner", RUNTIME_URL, self.app._AcnAccessTokenCredential("test-token"))
        try:
            for operation in ("updateTaskAsync", "updateTaskDetailsAsync", "assignUsersAsync",
                              "unassignUsersAsync", "deleteTaskAsync"):
                with self.subTest(operation=operation):
                    send = AsyncMock(return_value=self.response(status=204))
                    with patch.object(client.http_client, "send", send):
                        response, body = await self.invoke("planner", operation, client, **INPUT)
                    self.assertEqual(response.headers, {})
                    self.assertIsNone(body)
                    self.assertEqual(send.await_count, 1)
                    self.assertEqual(send.call_args.args[0],
                                     "DELETE" if operation == "deleteTaskAsync" else "PATCH")
        finally:
            await client.close()

    async def test_list_retains_first_page_envelope_nulls_and_continuation(self):
        client = self.app._new_acn_client(
            "planner", RUNTIME_URL, self.app._AcnAccessTokenCredential("test-token"))
        document = {"value": [{"planId": "p", "dueDateTime": None}],
                    "@odata.nextLink": RUNTIME_URL + "/next", "custom": None}
        try:
            send = AsyncMock(return_value=self.response(body=document))
            with patch.object(client.http_client, "send", send):
                response, body = await self.invoke("planner", "listMyTasksAsync", client)
            self.assertEqual(body, document)
            self.assertEqual(response.headers, {})
            self.assertEqual(send.await_count, 1)
        finally:
            await client.close()

    async def test_real_sdk_errors_preserve_udf_error_contract(self):
        client = self.app._new_acn_client(
            "planner", RUNTIME_URL, self.app._AcnAccessTokenCredential("test-token"))
        try:
            for upstream, code, status in (
                (self.response(403, {"error": {"code": "Forbidden", "message": "denied"}}),
                 "Forbidden", "BadRequest"),
                (ServiceRequestError("secret connection URL"), "ConnectorUnreachable", "Failed"),
                (ServiceResponseTimeoutError("secret connection URL"), "Timeout", "Timeout"),
            ):
                send = AsyncMock(side_effect=upstream) if isinstance(upstream, Exception) else AsyncMock(return_value=upstream)
                with self.subTest(code=code), patch.object(client.http_client, "send", send):
                    with self.assertLogs(level="WARNING") as logs:
                        response, body = await self.invoke("planner", "listMyTasksAsync", client)
                    self.assertEqual(response.headers["x-fabric-udf-status"], status)
                    self.assertEqual(body["error"]["code"], code)
                    self.assertNotIn("secret", json.dumps(body) + str(logs.output))
        finally:
            await client.close()

    async def test_platform_token_is_a_bearer_credential_not_an_api_key(self):
        from azure.core.pipeline.policies import AsyncBearerTokenCredentialPolicy
        from azure.core.pipeline import PipelineRequest
        from azure.core.pipeline.transport import HttpRequest
        from azure.core.pipeline import PipelineContext

        credential = self.app._AcnAccessTokenCredential("platform-token")
        policy = AsyncBearerTokenCredentialPolicy(credential, "https://apihub.azure.com/.default")
        request = PipelineRequest(HttpRequest("GET", RUNTIME_URL), PipelineContext(None))
        await policy.on_request(request)
        self.assertEqual(request.http_request.headers["Authorization"], "Bearer platform-token")


if __name__ == "__main__":
    unittest.main()
