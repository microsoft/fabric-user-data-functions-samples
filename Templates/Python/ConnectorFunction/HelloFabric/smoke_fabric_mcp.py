#!/usr/bin/env python3
"""Opt-in Daily smoke check for the managed Fabric MCP endpoint."""

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request


ENDPOINT = "https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq"
PROTOCOL_VERSION = "2026-07-28"
VARIANTS = "Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect"


def access_token():
    token = os.environ.get("FABRIC_MCP_ACCESS_TOKEN")
    if token:
        return token
    completed = subprocess.run(
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            "https://analysis.windows.net/powerbi/api",
            "--query",
            "accessToken",
            "--output",
            "tsv",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    token = completed.stdout.strip()
    if not token:
        raise RuntimeError("Azure CLI returned an empty access token.")
    return token


def main():
    if os.environ.get("FABRIC_MCP_DAILY_SMOKE") != "1":
        print("Skipped: set FABRIC_MCP_DAILY_SMOKE=1 to run.")
        return 0

    message = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "server/discover",
        "params": {
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                "io.modelcontextprotocol/clientInfo": {
                    "name": "Fabric-UDF-Daily-Smoke",
                    "version": "1.0.0",
                },
                "io.modelcontextprotocol/clientCapabilities": {
                    "extensions": {"io.modelcontextprotocol/tasks": {}}
                },
            }
        },
    }
    try:
        token = access_token()
    except (FileNotFoundError, subprocess.CalledProcessError, RuntimeError) as error:
        print(f"FAIL: access token unavailable ({type(error).__name__})", file=sys.stderr)
        return 1

    request = urllib.request.Request(
        ENDPOINT,
        data=json.dumps(message, separators=(",", ":")).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
            "Mcp-Method": "server/discover",
            "X-Variants": VARIANTS,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as error:
        print(f"FAIL: HTTP {error.code}", file=sys.stderr)
        return 1
    if (
        type(payload) is not dict
        or payload.get("jsonrpc") != "2.0"
        or payload.get("id") != 1
        or ("result" not in payload and "error" not in payload)
    ):
        print("FAIL: invalid JSON-RPC response", file=sys.stderr)
        return 1
    print("PASS: managed Fabric MCP server/discover")
    return 0


if __name__ == "__main__":
    sys.exit(main())
