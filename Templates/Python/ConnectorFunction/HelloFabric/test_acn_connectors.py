"""Run with python -B test_acn_connectors.py.

Exercises the shared ACN adapter against fake azure.connectors clients, so the
real SDK is not required.
"""

import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
import zipfile

ROOT = Path(__file__).resolve().parent
RUNTIME_URL = "https://example.azure-apim.net/apim/x/11111111111111111111111111111111"
SLUGS = (
    "office365users", "sharepointonline", "office365", "excelonlinebusiness", "teams",
    "onedriveforbusiness", "office365groups", "microsoftforms", "planner", "azuread",
)


class ConnectorException(Exception):
    def __init__(self, status_code, response_body):
        super().__init__("leaks /apim/x/connection-id")
        self.status_code, self.response_body = status_code, response_body


class FakeClient:
    calls = []
    result = {"value": []}
    error = None

    def __init__(self, connection_runtime_url, token_provider=None, options=None):
        self.url = connection_runtime_url

    def __getattr__(self, name):
        async def call(**kwargs):
            FakeClient.calls.append((name, kwargs))
            if FakeClient.error is not None:
                raise FakeClient.error
            return FakeClient.result
        return call


def _install_stubs():
    fabric, functions = types.ModuleType("fabric"), types.ModuleType("fabric.functions")

    class UserDataFunctions:
        def generic_connection(self, **_kwargs):
            return lambda function: function

        def streaming_function(self):
            return lambda function: function

    class StreamResponse:
        def __init__(self, body, media_type=None, status_code=200, headers=None):
            self.body, self.media_type, self.status_code = body, media_type, status_code

    functions.UserDataFunctions, functions.StreamResponse = UserDataFunctions, StreamResponse
    functions.FabricItem = type("FabricItem", (), {})
    fabric.functions = functions
    sys.modules["fabric"], sys.modules["fabric.functions"] = fabric, functions

    azure = sys.modules.setdefault("azure", types.ModuleType("azure"))
    connectors = types.ModuleType("azure.connectors")
    connectors.ConnectorException = ConnectorException
    azure.connectors = connectors
    sys.modules["azure.connectors"] = connectors
    for slug in SLUGS:
        module = types.ModuleType(f"azure.connectors.{slug}")
        setattr(module, slug.capitalize() + "Client", FakeClient)
        sys.modules[module.__name__] = module


def _load_function_app():
    _install_stubs()
    spec = importlib.util.spec_from_file_location("function_app", ROOT / "function_app.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


app = _load_function_app()


def _invoke(slug, operation, **input_data):
    payload = {"operation": operation, "input": {"connectionRuntimeUrl": RUNTIME_URL, **input_data}}
    response = asyncio.run(getattr(app, f"rayfin_{slug}_v1")(payload))
    return response.status_code, b"".join(response.body).decode("utf-8")


class AcnAdapterTests(unittest.TestCase):
    def setUp(self):
        FakeClient.calls, FakeClient.result, FakeClient.error = [], {"value": []}, None
        app._acn_clients.clear()

    def test_every_connector_is_registered_and_exposed(self):
        self.assertEqual(set(app._ACN_CONNECTORS), set(SLUGS))
        for slug in SLUGS:
            self.assertTrue(callable(getattr(app, f"rayfin_{slug}_v1")))
            self.assertTrue(getattr(app, f"_{slug.upper()}_OPERATIONS"))

    def test_dispatch_passes_upstream_json_through(self):
        FakeClient.result = {"value": [{"Title": "a"}]}
        status, body = _invoke("sharepointonline", "getItemsAsync",
                               siteUrl="https://contoso.sharepoint.com/sites/x", listName="Tasks", top=5)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"value": [{"Title": "a"}]})
        method, kwargs = FakeClient.calls[0]
        self.assertEqual(method, "get_items_async")
        self.assertEqual(kwargs["dataset"], "https://contoso.sharepoint.com/sites/x")
        self.assertEqual(kwargs["table"], "Tasks")
        self.assertEqual(kwargs["top"], "5")

    def test_null_result_returns_json_null(self):
        FakeClient.result = None
        self.assertEqual(_invoke("teams", "getAllChannelsForTeamAsync", teamId="t1"), (200, "null"))

    def test_query_flags_are_stringified(self):
        _invoke("office365", "getEventsCalendarViewAsync", calendarId="c1",
                startDateTimeUtc="2026-01-01T00:00:00Z", endDateTimeUtc="2026-01-02T00:00:00Z", top=5)
        _, kwargs = FakeClient.calls[0]
        self.assertEqual(kwargs["top"], "5")
        self.assertEqual(kwargs["calendar_id"], "c1")

    OWNER_WIDE = {
        "office365": ("getEmailsAsync", "getEmailAsync", "calendarGetTablesAsync"),
        "teams": ("getAllTeamsAsync", "getMessagesFromChatAsync", "listMeetingTranscriptsAsync",
                  "getMeetingTranscriptContentAsync"),
        "onedriveforbusiness": ("listRootFolderAsync", "getFileMetadataByPathAsync", "findFilesByPathAsync"),
    }

    def test_owner_wide_reads_are_disabled_by_default(self):
        self.assertFalse(app.ENABLE_OWNER_WIDE_READS)
        for slug, operations in self.OWNER_WIDE.items():
            for operation in operations:
                with self.subTest(slug=slug, operation=operation):
                    self.assertBadRequest(_invoke(slug, operation), f"'{operation}'.*ENABLE_OWNER_WIDE_READS")
        self.assertEqual(FakeClient.calls, [])
        self.assertEqual(app._acn_clients, {})

    def test_owner_wide_reads_dispatch_when_enabled(self):
        app.ENABLE_OWNER_WIDE_READS = True
        try:
            _invoke("office365", "calendarGetTablesAsync")
            _invoke("office365", "getEmailsAsync", top=3)
            _invoke("teams", "getMessagesFromChatAsync", chatId="19:abc")
            _invoke("onedriveforbusiness", "getFileMetadataByPathAsync", path="/Shared/a.xlsx")
        finally:
            app.ENABLE_OWNER_WIDE_READS = False
        self.assertEqual(FakeClient.calls[0], ("calendar_get_tables_async", {}))
        self.assertEqual(FakeClient.calls[1][0], "get_emails_async")
        self.assertEqual(FakeClient.calls[1][1]["include_attachments"], "false")
        self.assertEqual(FakeClient.calls[1][1]["top"], "3")
        self.assertEqual(FakeClient.calls[2][1]["chat_id"], "19:abc")
        self.assertEqual(FakeClient.calls[3], ("get_file_metadata_by_path_async", {"path": "/Shared/a.xlsx"}))

    def test_post_bodies_use_wire_names(self):
        _invoke("azuread", "checkMemberGroupsAsync", userId="u1", groupIds=["g1"])
        self.assertEqual(FakeClient.calls[0][1], {"id": "u1", "input": {"groupIds": ["g1"]}})

    def assertBadRequest(self, response, pattern):
        status, body = response
        self.assertEqual(status, 400)
        self.assertRegex(json.loads(body)["error"]["message"], pattern)

    def test_missing_required_field_returns_400(self):
        self.assertBadRequest(
            _invoke("sharepointonline", "getItemsAsync", siteUrl="https://contoso.sharepoint.com"),
            "listName")
        self.assertEqual(FakeClient.calls, [])

    def test_unknown_operation_lists_supported(self):
        self.assertBadRequest(_invoke("planner", "deleteBucketAsync"), "listMyTasksAsync")

    OWNER_WRITES = {
        "office365": ("sendEmailAsync", "replyToAsync", "calendarPostItemAsync",
                      "sharedMailboxSendEmailAsync"),
        "teams": ("postMessageToConversationAsync", "postCardToConversationAsync",
                  "replyWithMessageToConversationAsync", "updateCardInConversationAsync",
                  "createTeamsMeetingAsync"),
        "onedriveforbusiness": ("createFileAsync", "createShareLinkAsync",
                                "copyDriveFileAsync", "moveFileAsync"),
    }

    def test_owner_writes_are_disabled_by_default(self):
        self.assertFalse(app.ENABLE_OWNER_WRITES)
        for slug, operations in self.OWNER_WRITES.items():
            for operation in operations:
                with self.subTest(slug=slug, operation=operation):
                    self.assertBadRequest(_invoke(slug, operation), f"'{operation}'.*ENABLE_OWNER_WRITES")
        self.assertEqual(FakeClient.calls, [])
        self.assertEqual(app._acn_clients, {})

    def test_owner_writes_dispatch_when_enabled(self):
        app.ENABLE_OWNER_WRITES = True
        try:
            _invoke("office365", "sendEmailAsync", request={"To": "a@b.c", "Subject": "s"})
            _invoke("office365", "replyToAsync", messageId="m1", request={"Body": "hi"})
            _invoke("teams", "postMessageToConversationAsync", poster="Flow bot",
                    location="Channel", request={"recipient": {"groupId": "g"}})
            _invoke("onedriveforbusiness", "createFileAsync", folderPath="/Docs",
                    name="a.txt", content="aGVsbG8=")
            _invoke("onedriveforbusiness", "copyDriveFileAsync", fileId="f1",
                    destination="/Backup/a.txt", overwrite=True)
        finally:
            app.ENABLE_OWNER_WRITES = False
        self.assertEqual(FakeClient.calls[0],
                         ("send_email_async", {"input": {"To": "a@b.c", "Subject": "s"}}))
        self.assertEqual(FakeClient.calls[1], ("reply_to_async", {
            "message_id": "m1", "input": {"Body": "hi"}, "mailbox_address": None}))
        self.assertEqual(FakeClient.calls[2], ("post_message_to_conversation_async", {
            "poster": "Flow bot", "location": "Channel", "input": {"recipient": {"groupId": "g"}}}))
        self.assertEqual(FakeClient.calls[3], ("create_file_async", {
            "folder_path": "/Docs", "name": "a.txt", "input": b"hello"}))
        self.assertEqual(FakeClient.calls[4][1]["overwrite"], "true")

    def test_create_file_rejects_non_base64_content(self):
        app.ENABLE_OWNER_WRITES = True
        try:
            response = _invoke("onedriveforbusiness", "createFileAsync",
                               folderPath="/Docs", name="a.txt", content="not base64!")
        finally:
            app.ENABLE_OWNER_WRITES = False
        self.assertBadRequest(response, "base64")
        self.assertEqual(FakeClient.calls, [])

    def test_delegated_connector_writes_need_no_flag(self):
        site = "https://contoso.sharepoint.com/sites/x"
        _invoke("sharepointonline", "postItemAsync", siteUrl=site, listName="Tasks", item={"Title": "a"})
        _invoke("sharepointonline", "patchItemAsync", siteUrl=site, listName="Tasks", id=7, item={"Title": "b"})
        _invoke("sharepointonline", "getItemAttachmentsAsync", siteUrl=site, listName="Tasks", id=7)
        _invoke("excelonlinebusiness", "addRowAsync", source="me", driveId="d", fileId="f",
                tableName="T", item={"A": 1})
        _invoke("office365groups", "updateCalendarEventAsync", groupId="g", eventId="e", request={"subject": "x"})
        _invoke("planner", "assignUsersAsync", taskId="t", request={"userIds": ["u"]})
        self.assertEqual(FakeClient.calls[0], ("post_item_async", {
            "dataset": site, "table": "Tasks", "input": {"Title": "a"}}))
        self.assertEqual(FakeClient.calls[1], ("patch_item_async", {
            "dataset": site, "table": "Tasks", "id": "7", "input": {"Title": "b"}}))
        self.assertEqual(FakeClient.calls[2], ("get_item_attachments_async", {
            "dataset": site, "table": "Tasks", "item_id": "7"}))
        self.assertEqual(FakeClient.calls[3], ("add_row_async", {
            "source": "me", "drive": "d", "file": "f", "table": "T", "input": {"A": 1}}))
        self.assertEqual(FakeClient.calls[4], ("update_calendar_event_async", {
            "group_id": "g", "event": "e", "input": {"subject": "x"}}))
        self.assertEqual(FakeClient.calls[5], ("assign_users_async", {"id": "t", "input": {"userIds": ["u"]}}))

    def test_office365users_uses_shared_adapter(self):
        _invoke("office365users", "searchUserAsync", searchTerm="ada", top=5, isSearchTermRequired=False)
        self.assertEqual(FakeClient.calls[0], ("search_user_async", {
            "search_term": "ada", "top": "5", "is_search_term_required": "false", "skip_token": None}))
        self.assertIn(("office365users", RUNTIME_URL), app._acn_clients)
        self.assertBadRequest(_invoke("office365users", "managerAsync"), "userId")

    def test_missing_operation_and_runtime_url_return_400(self):
        def run(payload):
            response = asyncio.run(app.rayfin_planner_v1(payload))
            return response.status_code, b"".join(response.body).decode("utf-8")
        self.assertBadRequest(run({"input": {"connectionRuntimeUrl": RUNTIME_URL}}), "operation")
        self.assertBadRequest(run({"operation": "listMyTasksAsync", "input": {}}), "connectionRuntimeUrl")

    def test_sharepoint_get_item_reads_id_not_reserved_item_id(self):
        site = "https://contoso.sharepoint.com/sites/x"
        _invoke("sharepointonline", "getItemAsync", siteUrl=site, listName="Tasks", id=0)
        self.assertEqual(FakeClient.calls[0], ("get_item_async", {"dataset": site, "table": "Tasks", "id": 0}))
        self.assertBadRequest(
            _invoke("sharepointonline", "getItemAsync", siteUrl=site, listName="Tasks", itemId=7), "'id'")

    def test_transport_and_unexpected_errors_map_without_leaking(self):
        cases = (
            (asyncio.TimeoutError(), 504),
            (app.aiohttp.ClientConnectionError("leaks /apim/x/connection-id"), 502),
            (RuntimeError("leaks /apim/x/connection-id"), 500),
        )
        for error, expected in cases:
            with self.subTest(error=type(error).__name__):
                FakeClient.error = error
                with self.assertLogs(level="WARNING") as logs:
                    status, body = _invoke("planner", "listMyTasksAsync")
                self.assertEqual(status, expected)
                self.assertIn("listMyTasksAsync", json.loads(body)["error"]["message"])
                self.assertNotIn("connection-id", body + "\n".join(logs.output))

    def test_connector_error_maps_status_without_leaking_connection(self):
        FakeClient.error = ConnectorException(403, '{"error":"forbidden"}')
        with self.assertLogs(level="WARNING") as logs:
            status, body = _invoke("onedriveforbusiness", "listFolderAsync", folderId="f1")
        self.assertEqual((status, body), (403, '{"error":"forbidden"}'))
        self.assertNotIn("connection-id", "\n".join(logs.output))
        self.assertIn("rayfin_onedriveforbusiness_v1", logs.output[0])

    def test_connector_error_without_status_maps_to_502(self):
        FakeClient.error = ConnectorException(None, None)
        self.assertEqual(_invoke("microsoftforms", "listFormsAsync"), (502, ""))

    def test_clients_are_cached_per_connector_and_url(self):
        _invoke("planner", "listMyTasksAsync")
        _invoke("planner", "listMyTasksAsync")
        _invoke("office365groups", "listOwnedGroupsAsync")
        self.assertEqual(len(app._acn_clients), 2)

    def test_metadata_and_archives_declare_every_function(self):
        expected = {f"rayfin_{slug}_v1" for slug in SLUGS}
        metadata = json.loads((ROOT / "functions.metadata").read_text(encoding="utf-8"))
        names = {entry["name"] for entry in metadata}
        self.assertLessEqual(expected, names)
        source = (ROOT / "function_app.py").read_bytes().replace(b"\r\n", b"\n")
        for archive in ("Deploy.zip", "SourceCode.zip"):
            with zipfile.ZipFile(ROOT / archive) as zf:
                self.assertEqual(zf.read("function_app.py"), source)
                packed = json.loads(zf.read("fabric_lib/functions.metadata"))
                self.assertEqual(packed, metadata)


if __name__ == "__main__":
    unittest.main()
