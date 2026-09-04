"""Opt-in raw relay probe; credentials stay outside source, payloads, and logs."""

import asyncio
import os
import subprocess

import aiohttp
import pytest

from test_fabric_mcp import _Response, _load_function_app, _request


_DAILY_ENDPOINT = "https://dailyapi.fabric.microsoft.com/v1/mcp/fabriciq"


def _token():
    value = os.environ.get("FABRIC_MCP_ACCESS_TOKEN")
    if value:
        return value
    try:
        completed = subprocess.run(
            [
                "az",
                "account",
                "get-access-token",
                "--resource",
                "https://analysis.windows.net/powerbi/api",
                "--query",
                "accessToken",
                "-o",
                "tsv",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return completed.stdout.strip() or None


def test_daily_harness_uses_raw_sse_and_private_token(monkeypatch):
    app = _load_function_app()
    monkeypatch.setattr(app, "_FABRIC_MCP_ENDPOINT", _DAILY_ENDPOINT)
    response = _Response(
        raw=(
            b'data: {"jsonrpc":"2.0","method":"notifications/progress","params":{}}\n\n'
            b'data: {"jsonrpc":"2.0","id":"probe","result":{"tools":[]}}\n\n'
        ),
        content_type="text/event-stream",
    )
    output, session = __import__("test_fabric_mcp")._invoke(
        app,
        _request("tools/list", "probe"),
        [response],
        "private-token",
    )
    assert output["message"]["result"] == {"tools": []}
    assert session.requests[0]["url"] == _DAILY_ENDPOINT
    assert "private-token" not in repr(output)


@pytest.mark.skipif(
    os.environ.get("FABRIC_MCP_DAILY_TEST") != "1",
    reason="set FABRIC_MCP_DAILY_TEST=1 to run the Daily probe",
)
def test_daily_raw_discovery_and_tools(monkeypatch):
    token = _token()
    if token is None:
        pytest.skip("Daily token is unavailable")
    app = _load_function_app()
    monkeypatch.setattr(app, "_FABRIC_MCP_ENDPOINT", _DAILY_ENDPOINT)

    async def check():
        async with aiohttp.ClientSession() as session:
            for method in ("server/discover", "tools/list"):
                request_id = f"daily-{method}"
                output = await app._invoke_fabric_mcp(
                    _request(
                        method,
                        request_id,
                        {
                            "_meta": {
                                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                                "io.modelcontextprotocol/clientInfo": {
                                    "name": "Fabric-UDF-Daily-Smoke",
                                    "version": "1.0.0",
                                },
                            }
                        },
                    ),
                    lambda: token,
                    session_provider=lambda: session,
                )
                assert output["message"]["jsonrpc"] == "2.0"
                assert output["message"]["id"] == request_id
                assert type(output["message"].get("result")) is dict

    asyncio.run(check())
