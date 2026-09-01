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
MAX_RESPONSE_BYTES = 64 * 1024
MAX_SSE_EVENTS = 256
MAX_SSE_LINES_PER_EVENT = 8


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _media_type(content_type):
    parts = [part.strip().lower() for part in content_type.split(";")]
    if (
        not parts
        or parts[0] not in ("application/json", "text/event-stream")
        or len(parts) > 2
        or (len(parts) == 2 and parts[1] != "charset=utf-8")
    ):
        raise ValueError("invalid upstream response")
    return parts[0]


def _sse_response(payload, request_id):
    if "\r" in payload.replace("\r\n", ""):
        raise ValueError("invalid upstream response")
    payload = payload.replace("\r\n", "\n")
    if not payload.endswith("\n\n"):
        raise ValueError("invalid upstream response")
    events = payload[:-2].split("\n\n")
    if not events or len(events) > MAX_SSE_EVENTS or any(not event for event in events):
        raise ValueError("invalid upstream response")
    matching = None
    for event in events:
        lines = event.split("\n")
        if len(lines) > MAX_SSE_LINES_PER_EVENT:
            raise ValueError("invalid upstream response")
        event_name = None
        data_lines = []
        for line in lines:
            if line.startswith("event:"):
                if event_name is not None:
                    raise ValueError("invalid upstream response")
                event_name = line[6:].lstrip(" ")
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip(" "))
            else:
                raise ValueError("invalid upstream response")
        if event_name != "message" or not data_lines:
            raise ValueError("invalid upstream response")
        candidate = json.loads("\n".join(data_lines))
        if type(candidate) is not dict or candidate.get("jsonrpc") != "2.0":
            raise ValueError("invalid upstream response")
        if "id" in candidate:
            if (
                type(candidate["id"]) is not type(request_id)
                or candidate["id"] != request_id
                or matching is not None
            ):
                raise ValueError("invalid upstream response")
            matching = candidate
        elif (
            set(candidate)
            not in (
                {"jsonrpc", "method"},
                {"jsonrpc", "method", "params"},
            )
            or type(candidate.get("method")) is not str
            or not candidate["method"].startswith("notifications/")
            or ("params" in candidate and type(candidate["params"]) is not dict)
        ):
            raise ValueError("invalid upstream response")
    if matching is None:
        raise ValueError("invalid upstream response")
    return matching


def parse_response(content, content_type, request_id):
    if not content or len(content) > MAX_RESPONSE_BYTES:
        raise ValueError("invalid upstream response")
    decoded = content.decode("utf-8", errors="strict")
    if _media_type(content_type) == "application/json":
        return json.loads(decoded)
    return _sse_response(decoded, request_id)


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
            "Accept": "application/json,text/event-stream",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
            "Mcp-Method": "server/discover",
            "X-Variants": VARIANTS,
        },
        method="POST",
    )
    try:
        opener = urllib.request.build_opener(_NoRedirectHandler())
        with opener.open(request, timeout=300) as response:
            if not 200 <= response.status <= 299:
                raise ValueError("invalid upstream response")
            content = response.read(MAX_RESPONSE_BYTES + 1)
            payload = parse_response(
                content,
                response.headers.get("Content-Type", ""),
                1,
            )
    except urllib.error.HTTPError as error:
        print(f"FAIL: HTTP {error.code}", file=sys.stderr)
        return 1
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
        print("FAIL: invalid upstream response", file=sys.stderr)
        return 1
    if type(payload) is dict and "error" in payload:
        print("FAIL: upstream JSON-RPC error", file=sys.stderr)
        return 1
    if (
        type(payload) is not dict
        or payload.get("jsonrpc") != "2.0"
        or payload.get("id") != 1
        or type(payload.get("result")) is not dict
        or PROTOCOL_VERSION not in payload["result"].get("supportedVersions", ())
        or type(payload["result"].get("capabilities")) is not dict
        or type(
            payload["result"]["capabilities"].get("extensions")
        ) is not dict
        or "io.modelcontextprotocol/tasks"
        not in payload["result"]["capabilities"]["extensions"]
    ):
        print("FAIL: invalid JSON-RPC response", file=sys.stderr)
        return 1
    print("PASS: managed Fabric MCP server/discover")
    return 0


if __name__ == "__main__":
    sys.exit(main())
