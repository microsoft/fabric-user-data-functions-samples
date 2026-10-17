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
    "sharepointonline", "office365", "excelonlinebusiness", "teams",
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
        self.assertEqual(_invoke("teams", "getAllTeamsAsync"), (200, "null"))

    def test_query_flags_are_stringified_and_attachments_forced_off(self):
        _invoke("office365", "getEmailsAsync", fetchOnlyUnread=True, includeAttachments=True)
        _, kwargs = FakeClient.calls[0]
        self.assertEqual(kwargs["fetch_only_unread"], "true")
        self.assertEqual(kwargs["include_attachments"], "false")
        self.assertNotIn("mailbox_address", kwargs)

    def test_post_bodies_use_wire_names(self):
        _invoke("azuread", "checkMemberGroupsAsync", userId="u1", groupIds=["g1"])
        self.assertEqual(FakeClient.calls[0][1], {"id": "u1", "input": {"groupIds": ["g1"]}})

    def test_missing_required_field_raises(self):
        with self.assertRaisesRegex(ValueError, "listName"):
            _invoke("sharepointonline", "getItemsAsync", siteUrl="https://contoso.sharepoint.com")

    def test_unknown_operation_lists_supported(self):
        with self.assertRaisesRegex(ValueError, "listMyTasksAsync"):
            _invoke("planner", "createTaskAsync")

    def test_missing_operation_and_runtime_url_raise(self):
        with self.assertRaisesRegex(ValueError, "operation"):
            asyncio.run(app.rayfin_planner_v1({"input": {"connectionRuntimeUrl": RUNTIME_URL}}))
        with self.assertRaisesRegex(ValueError, "connectionRuntimeUrl"):
            asyncio.run(app.rayfin_planner_v1({"operation": "listMyTasksAsync", "input": {}}))

    def test_connector_error_maps_status_without_leaking_connection(self):
        FakeClient.error = ConnectorException(403, '{"error":"forbidden"}')
        with self.assertLogs(level="WARNING") as logs:
            status, body = _invoke("onedriveforbusiness", "listRootFolderAsync")
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
