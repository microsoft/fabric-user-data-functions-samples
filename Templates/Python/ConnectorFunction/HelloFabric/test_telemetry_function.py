"""Standalone tests for the rayfin_telemetry_v1 streaming function."""

import asyncio
import importlib
import json
import os
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch


_RESOURCE_ID = (
    "/subscriptions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    "/resourceGroups/telemetry-rg/providers/Microsoft.Insights/components/app-insights"
)


def _load_function_app():
    if "fabric.functions" not in sys.modules:
        fabric = types.ModuleType("fabric")
        functions = types.ModuleType("fabric.functions")

        class UserDataFunctions:
            def generic_connection(self, *_args, **_kwargs):
                return lambda function: function

            def streaming_function(self, *_args, **_kwargs):
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

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    return importlib.import_module("function_app")


class _FakeContent:
    def __init__(self, chunks):
        self.chunks = chunks

    async def iter_any(self):
        for chunk in self.chunks:
            yield chunk


class _FakeResponse:
    def __init__(self, chunks=(), status=200, json_body=None):
        self.status = status
        self.headers = {"Content-Type": "application/json; charset=utf-8"}
        self.content = _FakeContent(chunks)
        self.json_body = json_body
        self.text_called = False
        self.released = False

    async def text(self):
        self.text_called = True
        return b"".join(self.content.chunks).decode("utf-8")

    async def read(self):
        return b"".join(self.content.chunks)

    async def json(self):
        return self.json_body

    def release(self):
        self.released = True


def _payload(**overrides):
    data = {
        "kql": "AppTraces | project TimeGenerated, Message",
        "resourceId": _RESOURCE_ID,
        "timespan": "P1D",
        "maxRows": 1000,
    }
    data.update(overrides)
    return {"operation": "query", "input": data}


class TelemetryFunctionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.app = _load_function_app()
        self.query_response = _FakeResponse(
            [b'{"tables":[', b'{"name":"PrimaryResult"}', b"]}"]
        )
        self.token_response = _FakeResponse(
            json_body={"access_token": "fake-monitor-token", "expires_on": "4102444800"}
        )
        self.session = types.SimpleNamespace(
            get=AsyncMock(return_value=self.token_response),
            post=AsyncMock(return_value=self.query_response),
        )
        for patcher in (
            patch.object(self.app, "_get_session", AsyncMock(return_value=self.session)),
            patch.object(self.app, "_telemetry_token", None),
            patch.object(self.app, "_telemetry_token_expires_at", 0.0),
            patch.object(self.app, "_telemetry_token_lock", asyncio.Lock()),
            patch.dict(os.environ, {
                "IDENTITY_ENDPOINT": "http://127.0.0.1:41741/MSI/token",
                "IDENTITY_HEADER": "rotating-identity-header",
            }),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_streams_with_managed_identity(self):
        result = await self.app.rayfin_telemetry_v1(
            _payload(workspaceId="ignored-legacy-workspace")
        )
        self.assertEqual(
            [chunk async for chunk in result.body],
            self.query_response.content.chunks,
        )
        self.assertFalse(self.query_response.text_called)
        self.assertTrue(self.query_response.released)
        self.assertTrue(self.token_response.released)
        self.assertEqual(result.media_type, "application/json")
        self.session.get.assert_awaited_once_with(
            "http://127.0.0.1:41741/MSI/token",
            params={
                "resource": "https://api.loganalytics.io",
                "api-version": "2019-08-01",
            },
            headers={"X-IDENTITY-HEADER": "rotating-identity-header"},
        )
        self.session.post.assert_awaited_once_with(
            f"https://api.loganalytics.azure.com/v1{_RESOURCE_ID}/query",
            json={
                "query": "AppTraces | project TimeGenerated, Message\n| take 1000",
                "timespan": "P1D",
            },
            headers={
                "Authorization": "Bearer fake-monitor-token",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )

    async def test_accepts_mixed_case_resource_id(self):
        resource_id = _RESOURCE_ID.upper().replace(
            "APP-INSIGHTS", "App_Insights.v1(blue)"
        )
        await self.app.rayfin_telemetry_v1(_payload(resourceId=resource_id))
        self.assertEqual(
            self.session.post.call_args.args[0],
            f"https://api.loganalytics.azure.com/v1{resource_id}/query",
        )

    async def test_removes_trailing_query_semicolon(self):
        await self.app.rayfin_telemetry_v1(_payload(kql="AppTraces;  \r\n"))
        self.assertEqual(
            self.session.post.call_args.kwargs["json"]["query"],
            "AppTraces\n| take 1000",
        )

    async def test_optional_defaults_and_valid_boundaries(self):
        payload = _payload(kql="x" * 8192)
        del payload["input"]["timespan"]
        del payload["input"]["maxRows"]
        await self.app.rayfin_telemetry_v1(payload)
        body = self.session.post.call_args.kwargs["json"]
        self.assertNotIn("timespan", body)
        self.assertTrue(body["query"].endswith("\n| take 10000"))
        await self.app.rayfin_telemetry_v1(_payload(timespan="P30D", maxRows=1))
        self.assertEqual(
            self.session.post.call_args.kwargs["json"],
            {"query": "AppTraces | project TimeGenerated, Message\n| take 1",
             "timespan": "P30D"},
        )

    async def test_forwards_upstream_error(self):
        error = json.dumps({"error": {"code": "Forbidden"}}).encode("utf-8")
        self.query_response.status = 403
        self.query_response.content = _FakeContent([error])
        result = await self.app.rayfin_telemetry_v1(_payload())
        self.assertEqual(result.status_code, 403)
        self.assertEqual(b"".join(result.body), error)
        self.assertTrue(self.query_response.released)

    async def test_rejects_invalid_input_before_network(self):
        invalid = [
            None, [], {}, {"operation": "emit", "input": {}},
            {"operation": "query"}, {"operation": "query", "input": []},
            _payload(kql=""), _payload(kql=" "), _payload(kql=123),
            _payload(kql="x" * 8193),
            *[_payload(timespan=value) for value in
              ("P31D", "P0D", "P", "PT", "P1DT", "2026-10-16", 123)],
            *[_payload(maxRows=value) for value in (0, -1, 10001, True, "1", 1.5)],
        ]
        for payload in invalid:
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    await self.app.rayfin_telemetry_v1(payload)
        self.session.get.assert_not_awaited()
        self.session.post.assert_not_awaited()

    async def test_rejects_invalid_resource_ids_before_network(self):
        invalid_ids = [
            None, 123, {}, "", "not-an-arm-id",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            _RESOURCE_ID.replace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "not-a-guid"),
            _RESOURCE_ID.replace("Microsoft.Insights/components", "Microsoft.OperationalInsights/workspaces"),
            _RESOURCE_ID.replace("Microsoft.Insights/components", "Microsoft.Insights/webtests"),
            _RESOURCE_ID.replace("Microsoft.Insights", "Other.Insights"),
            _RESOURCE_ID.replace("/telemetry-rg/", "//"),
            _RESOURCE_ID.replace("/telemetry-rg/", "/../"),
            _RESOURCE_ID.replace("/telemetry-rg/", "/./"),
            _RESOURCE_ID.replace("/telemetry-rg/", "/bad rg/"),
            _RESOURCE_ID.replace("/telemetry-rg/", "/bad./"),
            _RESOURCE_ID.rsplit("/", 1)[0] + "/..",
            _RESOURCE_ID.rsplit("/", 1)[0] + "/.",
            _RESOURCE_ID.rsplit("/", 1)[0] + "/",
            _RESOURCE_ID + "/", _RESOURCE_ID + "/child",
            _RESOURCE_ID + "/../other", _RESOURCE_ID + "/./query",
            _RESOURCE_ID + "?query=other", _RESOURCE_ID + "#fragment",
            _RESOURCE_ID + "%2f..%2fother", _RESOURCE_ID + "%252fother",
            _RESOURCE_ID + "\\other", _RESOURCE_ID + " space",
            _RESOURCE_ID + "\n", _RESOURCE_ID + "\r", _RESOURCE_ID + "\t",
            _RESOURCE_ID + "\x00", " " + _RESOURCE_ID,
            _RESOURCE_ID.lstrip("/"), "/" + _RESOURCE_ID,
            "https://api.loganalytics.azure.com/v1" + _RESOURCE_ID,
            "//evil.example" + _RESOURCE_ID,
            _RESOURCE_ID.replace("Insights", "In\u017fights"),
        ]
        for resource_id in invalid_ids:
            with self.subTest(resource_id=resource_id):
                with self.assertRaisesRegex(ValueError, "resourceId"):
                    await self.app.rayfin_telemetry_v1(_payload(resourceId=resource_id))
        legacy = _payload(workspaceId="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        del legacy["input"]["resourceId"]
        with self.assertRaisesRegex(ValueError, "resourceId"):
            await self.app.rayfin_telemetry_v1(legacy)
        self.session.get.assert_not_awaited()
        self.session.post.assert_not_awaited()

    async def test_reuses_cached_token(self):
        self.assertEqual(
            await self.app._get_telemetry_access_token(), "fake-monitor-token"
        )
        self.assertEqual(
            await self.app._get_telemetry_access_token(), "fake-monitor-token"
        )
        self.session.get.assert_awaited_once()

    async def test_refreshes_token_before_expiry(self):
        self.app._telemetry_token = "old-token"
        self.app._telemetry_token_expires_at = 1059
        with patch.object(self.app.time, "time", return_value=1000):
            token = await self.app._get_telemetry_access_token()
        self.assertEqual(token, "fake-monitor-token")
        self.session.get.assert_awaited_once()

    async def test_missing_identity_fails_explicitly(self):
        with patch.dict(os.environ, {"IDENTITY_ENDPOINT": "", "IDENTITY_HEADER": ""}):
            with self.assertRaisesRegex(RuntimeError, "Managed identity is unavailable"):
                await self.app._get_telemetry_access_token()
        self.session.get.assert_not_awaited()

    async def test_identity_error_releases_response(self):
        self.token_response.status = 500
        with self.assertRaisesRegex(RuntimeError, "token acquisition failed"):
            await self.app._get_telemetry_access_token()
        self.assertTrue(self.token_response.released)
        self.assertIsNone(self.app._telemetry_token)

    async def test_invalid_identity_token_is_rejected(self):
        for body in ([], {}, {"access_token": "token", "expires_on": "bad"},
                     {"access_token": "token", "expires_on": "1"}):
            with self.subTest(body=body):
                self.token_response.json_body = body
                with self.assertRaisesRegex(RuntimeError, "invalid token"):
                    await self.app._get_telemetry_access_token()
                self.assertTrue(self.token_response.released)
                self.assertIsNone(self.app._telemetry_token)


if __name__ == "__main__":
    unittest.main()
