import fabric.functions as fn
import aiohttp
import asyncio
import base64
import binascii
import inspect
import json
import logging
import math
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse

udf = fn.UserDataFunctions()

_POWERBI_BASE = os.environ.get("POWERBI_API_BASE", "https://dailyapi.powerbi.com/v1.0/myorg")
_ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"
_JSON_MEDIA_TYPE = "application/json"
_LOG_ANALYTICS_BASE = "https://api.loganalytics.azure.com/v1"
_LOG_ANALYTICS_RESOURCE = "https://api.loganalytics.io"
_MAXIMUM_TELEMETRY_QUERY_LENGTH = 8 * 1024
_MAXIMUM_TELEMETRY_TIMESPAN_SECONDS = 30 * 24 * 60 * 60
_MAXIMUM_TELEMETRY_ROWS = 10000
_TELEMETRY_TIMESPAN_PATTERN = re.compile(
    r"^P(?!$)(?:(\d+)D)?(?:T(?!$)(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$"
)
_APPLICATION_INSIGHTS_RESOURCE_ID_PATTERN = re.compile(
    r"/subscriptions/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}"
    r"/resourceGroups/(?!\.{1,2}/)[a-z0-9_().-]{1,90}(?<!\.)"
    r"/providers/Microsoft\.Insights/components/"
    r"(?!\.{1,2}$)[a-z0-9_().-]{1,260}",
    re.IGNORECASE | re.ASCII,
)

# Relaxed-Build internal DAX route. Lives at the host root (origin), not under
# /v1.0/myorg, and is model-only. When the caller supplies a BaaS artifact
# object id we route here and pass it in the X-Rayfin-ArtifactObjectId header,
# which lets Read-only (View) users execute DAX. Absent -> public endpoint.
_INTERNAL_ROUTE_PREFIX = "metadata/datasets/v202607"
_RAYFIN_ARTIFACT_OBJECT_ID_HEADER = "X-Rayfin-ArtifactObjectId"

# Shared, lazily-created session reused across invocations for connection pooling
# (keep-alive to Power BI, no per-call TLS handshake). Created inside the event
# loop on first use; never closed per-invoke.
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()
_telemetry_token: Optional[str] = None
_telemetry_token_expires_at = 0.0
_telemetry_token_lock = asyncio.Lock()


async def _get_session() -> aiohttp.ClientSession:
    global _session
    # Fast path: already have a live session.
    if _session is not None and not _session.closed:
        return _session
    # Slow path: create once, guarded so concurrent first-invokes don't race.
    async with _session_lock:
        if _session is None or _session.closed:
            # No total/read timeout: a streamed DAX response can take a while to
            # drain, and we forward bytes as they arrive rather than time out.
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
            connector = aiohttp.TCPConnector(
                limit=100,            # max pooled connections
                keepalive_timeout=60, # keep idle conns warm for reuse
                ttl_dns_cache=300,    # cache DNS so we don't re-resolve each call
            )
            _session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return _session


def _validate_telemetry_timespan(value: object) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("timespan must be an ISO-8601 duration")

    match = _TELEMETRY_TIMESPAN_PATTERN.fullmatch(value)
    if not match:
        raise ValueError("timespan must be an ISO-8601 duration")

    days, hours, minutes, seconds = (
        int(component or 0) for component in match.groups()
    )
    duration_seconds = (
        days * 24 * 60 * 60 + hours * 60 * 60 + minutes * 60 + seconds
    )
    if duration_seconds <= 0 or duration_seconds > _MAXIMUM_TELEMETRY_TIMESPAN_SECONDS:
        raise ValueError("timespan must be greater than zero and at most 30 days")
    return value


async def _get_telemetry_access_token() -> str:
    global _telemetry_token, _telemetry_token_expires_at

    now = time.time()
    if _telemetry_token and now < _telemetry_token_expires_at - 60:
        return _telemetry_token

    async with _telemetry_token_lock:
        now = time.time()
        if _telemetry_token and now < _telemetry_token_expires_at - 60:
            return _telemetry_token

        identity_endpoint = os.environ.get("IDENTITY_ENDPOINT")
        identity_header = os.environ.get("IDENTITY_HEADER")
        if not identity_endpoint or not identity_header:
            raise RuntimeError("Managed identity is unavailable")

        session = await _get_session()
        response = await session.get(
            identity_endpoint,
            params={
                "resource": _LOG_ANALYTICS_RESOURCE,
                "api-version": "2019-08-01",
            },
            headers={"X-IDENTITY-HEADER": identity_header},
        )
        try:
            if response.status != 200:
                await response.read()
                raise RuntimeError("Managed identity token acquisition failed")
            token_response = await response.json()
        finally:
            response.release()

        if not isinstance(token_response, dict):
            raise RuntimeError("Managed identity returned an invalid token")
        access_token = token_response.get("access_token")
        try:
            expires_on = float(token_response.get("expires_on", 0))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Managed identity returned an invalid token") from exc
        if not access_token or expires_on <= now:
            raise RuntimeError("Managed identity returned an invalid token")

        _telemetry_token = access_token
        _telemetry_token_expires_at = expires_on
        return access_token


@udf.streaming_function()
async def rayfin_telemetry_v1(payload: dict) -> fn.StreamResponse:
    if not isinstance(payload, dict) or payload.get("operation") != "query":
        raise ValueError("operation must be query")

    input_data = payload.get("input")
    if not isinstance(input_data, dict):
        raise ValueError("input is required")

    query = input_data.get("kql")
    if not isinstance(query, str) or not query.strip():
        raise ValueError("kql is required")
    if len(query) > _MAXIMUM_TELEMETRY_QUERY_LENGTH:
        raise ValueError(
            f"kql cannot exceed {_MAXIMUM_TELEMETRY_QUERY_LENGTH} characters"
        )

    resource_id = input_data.get("resourceId")
    if (
        not isinstance(resource_id, str)
        or not _APPLICATION_INSIGHTS_RESOURCE_ID_PATTERN.fullmatch(resource_id)
    ):
        raise ValueError("resourceId must be a full Application Insights ARM resource ID")

    timespan = _validate_telemetry_timespan(input_data.get("timespan"))

    maximum_rows = input_data.get("maxRows", _MAXIMUM_TELEMETRY_ROWS)
    if (
        not isinstance(maximum_rows, int)
        or isinstance(maximum_rows, bool)
        or maximum_rows < 1
        or maximum_rows > _MAXIMUM_TELEMETRY_ROWS
    ):
        raise ValueError(
            f"maxRows must be between 1 and {_MAXIMUM_TELEMETRY_ROWS}"
        )

    access_token = await _get_telemetry_access_token()
    url = f"{_LOG_ANALYTICS_BASE}{resource_id}/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": _JSON_MEDIA_TYPE,
        "Accept": _JSON_MEDIA_TYPE,
    }
    query_text = query.rstrip()
    if query_text.endswith(";"):
        query_text = query_text[:-1].rstrip()

    bounded_query = f"{query_text}\n| take {maximum_rows}"
    body = {"query": bounded_query}
    if timespan:
        body["timespan"] = timespan

    session = await _get_session()
    response = await session.post(url, json=body, headers=headers)
    if response.status != 200:
        detail = await response.text()
        response.release()
        return fn.StreamResponse(
            iter([detail.encode("utf-8")]),
            media_type=response.headers.get("Content-Type", _JSON_MEDIA_TYPE),
            status_code=response.status,
        )

    async def relay():
        try:
            async for chunk in response.content.iter_any():
                yield chunk
        finally:
            response.release()

    return fn.StreamResponse(relay(), media_type=_JSON_MEDIA_TYPE)


class FabricMcpRequestError(ValueError):
    pass


async def _invoke_fabric_mcp(payload, token_provider, session_provider=_get_session):
    try:
        parsed = urlparse(_POWERBI_BASE)
    except ValueError as exc:
        raise FabricMcpRequestError("POWERBI_API_BASE must use a supported Power BI HTTPS origin.") from exc
    origin = _FABRIC_ORIGINS.get(f"{parsed.scheme}://{parsed.netloc}")
    if (origin is None or not _mcp_header(_POWERBI_BASE, 2048)
            or "\\" in _POWERBI_BASE or parsed.query or parsed.fragment):
        raise FabricMcpRequestError("POWERBI_API_BASE must use a supported Power BI HTTPS origin.")

    token = token_provider()
    if inspect.isawaitable(token):
        token = await token
    if type(token) is not str or not token or "\r" in token or "\n" in token:
        raise FabricMcpRequestError("Invalid Fabric MCP access token.")

    session = session_provider()
    if inspect.isawaitable(session):
        session = await session

    message = payload["message"]
    headers = {
        name: value
        for name, value in payload["headers"].items()
        if name.lower() not in ("authorization", "host", "content-length", "connection")
    }
    header_names = {name.lower() for name in headers}
    for name, value in (
        ("Content-Type", _JSON_MEDIA_TYPE),
        ("Accept", "application/json, text/event-stream"),
        ("MCP-Protocol-Version", payload["protocolVersion"]),
    ):
        if name.lower() not in header_names:
            headers[name] = value
    headers["Authorization"] = f"Bearer {token}"

    try:
        response = await session.post(
            f"{origin}/v1/mcp/fabriciq",
            data=json.dumps(message, separators=(",", ":")).encode("utf-8"),
            headers=headers,
            allow_redirects=False,
        )
        try:
            raw = await response.text(encoding="utf-8")
            response_headers = {}
            seen_names = set()
            for name, value in response.headers.items():
                if name.lower() not in seen_names:
                    response_headers[name] = value
                    seen_names.add(name.lower())
            return {
                "status": response.status,
                "headers": response_headers,
                "message": raw,
            }
        finally:
            response.release()
    except (aiohttp.ClientError, asyncio.TimeoutError, UnicodeError):
        raise RuntimeError("Fabric MCP upstream transport failed.") from None


@udf.generic_connection(argName="fabricIqClient", audienceType="Fabric")
@udf.streaming_function()
async def rayfin_fabriciq_mcp_v1(
    payload: dict, fabricIqClient: fn.FabricItem
) -> fn.StreamResponse:
    if (
        payload.get("operation") != "executeQuery"
        or not isinstance(payload.get("input"), dict)
        or any(key in payload for key in ("protocolVersion", "headers", "message"))
    ):
        raise FabricMcpRequestError("Invalid connector invocation wrapper.")
    payload = payload["input"]

    def token_provider():
        return fabricIqClient.get_access_token().get_token().token

    result = await _invoke_fabric_mcp(payload, token_provider)
    return fn.StreamResponse(
        iter([json.dumps(result, separators=(",", ":")).encode("utf-8")]),
        media_type=_JSON_MEDIA_TYPE,
    )


@udf.streaming_function()
async def rayfin_semantic_model_v1(payload: dict, accesstoken: str) -> fn.StreamResponse:
    input_data = payload.get("input", {})

    dataset_id = input_data.get("itemId")
    workspace_id = input_data.get("workspaceId")
    dax_query = input_data.get("query")
    baas_item_id = input_data.get("baasItemId")

    if not workspace_id or not dataset_id or not dax_query:
        raise ValueError("workspaceId, datasetId and query are required")

    # Route based on the presence of a BaaS artifact object id. When provided,
    # target the internal (relaxed-Build) endpoint at the host root and pass the
    # id in the X-Rayfin-ArtifactObjectId header. Otherwise use the public
    # executeDaxQueries endpoint (byte-identical to prior behavior).
    headers = {
        "Authorization": f"Bearer {accesstoken}",
        "Content-Type": "application/json",
    }
    if baas_item_id:
        origin = "{0.scheme}://{0.netloc}".format(urlparse(_POWERBI_BASE))
        url = f"{origin}/{_INTERNAL_ROUTE_PREFIX}/models/{dataset_id}/executeDaxQueriesInternal"
        headers[_RAYFIN_ARTIFACT_OBJECT_ID_HEADER] = baas_item_id
    else:
        url = f"{_POWERBI_BASE}/datasets/{dataset_id}/executeDaxQueries"
    body = {"query": dax_query}

    session = await _get_session()

    # `await session.post(...)` returns once the response *headers* are received
    # (aiohttp reads the body lazily via `resp.content`), so we learn the real
    # upstream status before deciding how to respond — without buffering the body.
    resp = await session.post(url, json=body, headers=headers)

    if resp.status != 200:
        # Surface the upstream error verbatim and don't open a stream.
        detail = await resp.text()   # fully drains the body -> connection returns to pool
        await resp.release()         # release the RESPONSE, never the shared session
        return fn.StreamResponse(
            iter([detail.encode("utf-8")]),
            media_type=resp.headers.get("Content-Type", "application/json"),
            status_code=resp.status,
        )

    async def relay():
        try:
            # iter_any() yields each TCP read as soon as it lands -> lowest latency.
            async for chunk in resp.content.iter_any():
                yield chunk
        finally:
            # Release the response so its connection returns to the pool (or is
            # closed if the client disconnected mid-stream). Do NOT close the
            # shared session here.
            await resp.release()

    return fn.StreamResponse(relay(), media_type=_ARROW_MEDIA_TYPE)


# Connector-neutral MCP streamable-HTTP transport. Keep in this file until
# ConnectorFunction deployment supports multiple Python source files.
_MCP_PROTOCOL = "2025-06-18"
# Leave 5 seconds for session cleanup, below UDF's 240s and BaaS's 300s deadline.
_MCP_TIMEOUT_SECONDS = 230
_MCP_MAX_INITIALIZE_BYTES = 16 * 1024 * 1024


class _McpError(Exception):
    def __init__(self, status, body, headers):
        super().__init__(f"MCP transport failed ({status}).")
        self.status, self.body, self.headers = status, body, headers


def _mcp_failure(status, message):
    return _McpError(status, json.dumps({"message": message}).encode("utf-8"),
                     {"Content-Type": _JSON_MEDIA_TYPE})


def _mcp_header(value, limit):
    return isinstance(value, str) and 0 < len(value) <= limit and all(0x21 <= ord(c) <= 0x7E for c in value)


async def _mcp_relay(session, endpoint, headers, message):
    deadline = asyncio.get_running_loop().time() + _MCP_TIMEOUT_SECONDS
    response, buffered = None, None

    async def post(document):
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise asyncio.TimeoutError()
        return await session.post(
            endpoint, json=document, headers=headers, allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=remaining, sock_connect=30, sock_read=210),
        )

    async def close():
        if response is not None:
            response.release()
        if "Mcp-Session-Id" in headers:
            try:
                closed = await session.delete(
                    endpoint, headers=headers, allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=5),
                )
                try:
                    if closed.status not in (200, 202, 204, 404, 405):
                        logging.warning("MCP session cleanup failed with HTTP %s.", closed.status)
                finally:
                    closed.release()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                logging.warning("MCP session cleanup failed; preserving the invocation outcome.")
        await session.close()

    try:
        initialize = message if message["method"] == "initialize" else {
            "jsonrpc": "2.0", "id": "initialize:" + message["id"], "method": "initialize",
            "params": {"protocolVersion": _MCP_PROTOCOL,
                       "capabilities": {"tasks": {"requests": {"tools": {"call": {}}}}},
                       "clientInfo": {"name": "rayfin-dataagent", "version": "1"}},
        }
        response = await post(initialize)
        session_id = response.headers.get("Mcp-Session-Id")
        if session_id is not None:
            if not _mcp_header(session_id, 4096):
                raise _mcp_failure(502, "MCP returned an invalid session header.")
            headers["Mcp-Session-Id"] = session_id
        if 200 <= response.status < 300:
            # Fabric's initialize endpoint returns JSON. Only the handshake is
            # consumed here; all requested response validation belongs to the SDK.
            media_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
            if media_type != "application/json" and not media_type.endswith("+json"):
                raise _mcp_failure(502, "MCP initialize did not return JSON.")
            body = bytearray()
            async for chunk in response.content.iter_any():
                body.extend(chunk)
                if len(body) > _MCP_MAX_INITIALIZE_BYTES:
                    raise _mcp_failure(413, "MCP initialization exceeded the 16 MiB limit.")
            buffered = bytes(body)
            try:
                initialized = json.loads(buffered)
            except (ValueError, UnicodeError, RecursionError) as exc:
                raise _mcp_failure(502, "MCP initialize did not return JSON.") from exc
            if isinstance(initialized, dict) and "error" not in initialized and isinstance(initialized.get("result"), dict):
                headers["MCP-Protocol-Version"] = _MCP_PROTOCOL
                acknowledged = await post({"jsonrpc": "2.0", "method": "notifications/initialized"})
                if acknowledged.status not in (202, 204):
                    response.release()
                    response, buffered = acknowledged, None
                else:
                    acknowledged.release()
                    if message["method"] != "initialize":
                        response.release()
                        response, buffered = await post(message), None
        if 300 <= response.status < 400:
            raise _mcp_failure(502, "MCP redirects are not supported.")
    except BaseException:
        await close()
        raise

    async def relay():
        try:
            async with asyncio.timeout(max(0, deadline - asyncio.get_running_loop().time())):
                if buffered is not None:
                    yield buffered
                else:
                    async for chunk in response.content.iter_any():
                        yield chunk
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logging.warning("MCP response stream failed.")
            raise
        finally:
            await close()

    return fn.StreamResponse(relay(), status_code=response.status,
                             media_type=response.headers.get("Content-Type", _JSON_MEDIA_TYPE))


_FABRIC_ORIGINS = {
    "https://powerbiapi.analysis-df.windows.net": "https://powerbiapi.analysis-df.windows.net",
    "https://dailyapi.powerbi.com": "https://dailyapi.fabric.microsoft.com",
    "https://dxtapi.powerbi.com": "https://dxtapi.fabric.microsoft.com",
    # MSIT and production share this Power BI base; it cannot distinguish them.
    "https://api.powerbi.com": "https://api.fabric.microsoft.com",
}
# Bind the requested RPC to the connector operation authorized by BaaS.
_DATA_AGENT_METHODS = {
    "getInfo": ("initialize", "tools/list"), "startTask": ("tools/call",),
    "getTask": ("tasks/get",), "getTaskResult": ("tasks/result",), "cancelTask": ("tasks/cancel",),
}


def _data_agent_endpoint(data):
    for name in ("workspaceId", "itemId"):
        if not isinstance(data.get(name), str) or not re.fullmatch(
                r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", data[name]):
            raise _mcp_failure(400, "workspaceId and itemId must be GUIDs.")
    try:
        parsed = urlparse(_POWERBI_BASE)
    except ValueError as exc:
        raise _mcp_failure(503, "POWERBI_API_BASE must use a supported Power BI HTTPS origin.") from exc
    origin = _FABRIC_ORIGINS.get(f"{parsed.scheme}://{parsed.netloc}")
    if (origin is None or not _mcp_header(_POWERBI_BASE, 2048)
            or "\\" in _POWERBI_BASE or parsed.query or parsed.fragment):
        raise _mcp_failure(503, "POWERBI_API_BASE must use a supported Power BI HTTPS origin.")
    return f"{origin}/v1/mcp/workspaces/{data['workspaceId']}/dataagents/{data['itemId']}/agent"


@udf.generic_connection(argName="fabricClient", audienceType="Fabric")
@udf.streaming_function()
async def rayfin_data_agent_v1(payload: dict, fabricClient: fn.FabricItem) -> fn.StreamResponse:
    logging.info("rayfin_data_agent_v1: invocation started")
    try:
        if not isinstance(payload, dict) or not isinstance(payload.get("input"), dict):
            raise _mcp_failure(400, "Expected an input object.")
        operation, data = payload.get("operation"), payload["input"]
        if "history" in data:
            raise _mcp_failure(400, "history is no longer accepted on the wire; upgrade the Data Agent SDK.")
        endpoint = _data_agent_endpoint(data)
        message = data.get("message")
        if (not isinstance(operation, str) or not isinstance(message, dict)
                or message.get("jsonrpc") != "2.0" or not isinstance(message.get("id"), str)
                or not message["id"] or not isinstance(message.get("params"), dict)
                or message.get("method") not in _DATA_AGENT_METHODS.get(operation, ())):
            raise _mcp_failure(400, "Expected an SDK JSON-RPC message matching the connector operation; upgrade the SDK.")
        correlation_id = data.get("clientRequestId", str(uuid.uuid4()))
        if not _mcp_header(correlation_id, 256):
            raise _mcp_failure(400, "clientRequestId must contain 1-256 visible ASCII characters.")
        access_token = fabricClient.get_access_token().get_token().token
        if not _mcp_header(access_token, 65536):
            raise _mcp_failure(401, "Fabric connection did not provide an access token.")
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": _JSON_MEDIA_TYPE,
            "Accept": "application/json, text/event-stream",
            "x-ms-client-request-id": correlation_id,
        }
        shared = await _get_session()
        # The relay owns this session until response consumption or cancellation.
        session = aiohttp.ClientSession(connector=shared.connector, connector_owner=False,
                                        cookie_jar=aiohttp.DummyCookieJar())
        return await _mcp_relay(session, endpoint, headers, message)
    except _McpError as exc:
        error = exc
    except asyncio.TimeoutError:
        error = _mcp_failure(504, "MCP request timed out.")
    except (aiohttp.ClientPayloadError, UnicodeError):
        error = _mcp_failure(502, "MCP response could not be decoded.")
    except aiohttp.ClientError:
        error = _mcp_failure(503, "MCP transport failed.")
    except asyncio.CancelledError:
        logging.info("rayfin_data_agent_v1: invocation cancelled")
        raise
    logging.warning("rayfin_data_agent_v1: invocation failed with HTTP %s", error.status)
    return fn.StreamResponse(iter([error.body]), status_code=error.status,
                             media_type=_JSON_MEDIA_TYPE, headers=error.headers)


# ---------------------------------------------------------------------------
# Eventhouse -- saved, parameterized queries
# ---------------------------------------------------------------------------
# Next to operation and input, the host adds payload fields that the caller
# cannot write:
#   additionalPayload: the connector settings saved by the app builder
#       {"queryServiceUri": ..., "database": <KQL database item ID>,
#        "queries": {queryId: {"operation", "text", "parameters", "roles"}}}
#   binding (optional): the invocation the host authorized, e.g. {"function", "operation", ...}
#   callerContext: the authenticated caller {"isAuthenticated": bool, "roles": [...]}
# The caller selects a saved template by queryId and supplies typed values. The
# template text is sent unchanged and the values travel as Kusto query
# parameters, so a value can never change the query.
_EVENTHOUSE_TARGET_FIELD = "queryServiceUri"
_EVENTHOUSE_DATABASE_FIELD = "database"
_EVENTHOUSE_QUERIES_FIELD = "queries"
_EVENTHOUSE_SETTINGS_PAYLOAD_FIELD = "additionalPayload"
_EVENTHOUSE_BINDING_PAYLOAD_FIELD = "binding"
_EVENTHOUSE_CALLER_PAYLOAD_FIELD = "callerContext"
_EVENTHOUSE_FUNCTION_NAME = "rayfin_kusto_v1"
_EVENTHOUSE_AUTHENTICATED_FIELD = "isAuthenticated"
_EVENTHOUSE_ROLES_FIELD = "roles"
_EVENTHOUSE_SETTINGS_FIELDS = frozenset({
    _EVENTHOUSE_TARGET_FIELD, _EVENTHOUSE_DATABASE_FIELD, _EVENTHOUSE_QUERIES_FIELD,
})
_EVENTHOUSE_TEMPLATE_FIELDS = frozenset({"operation", "text", "parameters", "roles"})
_EVENTHOUSE_PAYLOAD_FIELDS = frozenset({
    "operation", "input", _EVENTHOUSE_SETTINGS_PAYLOAD_FIELD,
    _EVENTHOUSE_BINDING_PAYLOAD_FIELD, _EVENTHOUSE_CALLER_PAYLOAD_FIELD,
})
# workspaceId, itemId and baasItemId are injected by the platform from the
# saved connector configuration; every other routing field is refused.
_EVENTHOUSE_INPUT_FIELDS = frozenset({
    "queryId", "parameters", "clientRequestId", "workspaceId", "itemId", "baasItemId",
})
_EVENTHOUSE_ROUTES = {"executeQuery": "query", "executeCommand": "mgmt"}
_EVENTHOUSE_MAXIMUM_TEMPLATES = 256
_EVENTHOUSE_MAXIMUM_TEXT_LENGTH = 65536
_EVENTHOUSE_MAXIMUM_PARAMETERS = 128
_EVENTHOUSE_MAXIMUM_ROLES = 128
_EVENTHOUSE_MAXIMUM_ROLE_LENGTH = 128
_EVENTHOUSE_MAXIMUM_REQUEST_ID_LENGTH = 256
_EVENTHOUSE_AUTHENTICATED_ROLE = "authenticated"
_EVENTHOUSE_ANONYMOUS_ROLE = "anonymous"
_EVENTHOUSE_ENDPOINT_PATTERN = re.compile(
    r"https://(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"kusto\.fabric\.microsoft\.com(?::443)?/?",
    re.IGNORECASE | re.ASCII,
)
_EVENTHOUSE_GUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE | re.ASCII,
)
_EVENTHOUSE_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}", re.ASCII)
_EVENTHOUSE_PARAMETER_TYPES = frozenset({
    "string", "long", "real", "bool", "datetime", "guid", "timespan", "dynamic",
})
_EVENTHOUSE_COMMAND_SPACE = r"[ \t\r\n]+"
_EVENTHOUSE_ENTITY_NAME = (
    r"(?:[A-Za-z_][A-Za-z0-9_]{0,127}|\['[A-Za-z_][A-Za-z0-9_ .-]{0,127}'\])"
)
# Database-scoped metadata commands only: no details forms, external tables,
# comments, pipes or further statements.
_EVENTHOUSE_METADATA_COMMAND_PATTERN = re.compile(
    r"[ \t\r\n]*\.show" + _EVENTHOUSE_COMMAND_SPACE
    + r"(?:tables|table" + _EVENTHOUSE_COMMAND_SPACE + _EVENTHOUSE_ENTITY_NAME
    + r"(?:" + _EVENTHOUSE_COMMAND_SPACE + r"(?:schema(?:" + _EVENTHOUSE_COMMAND_SPACE
    + r"as" + _EVENTHOUSE_COMMAND_SPACE + r"json)?|cslschema))?|database"
    + _EVENTHOUSE_COMMAND_SPACE + r"(?:schema(?:" + _EVENTHOUSE_COMMAND_SPACE + r"as"
    + _EVENTHOUSE_COMMAND_SPACE + r"json)?|cslschema)|functions|function"
    + _EVENTHOUSE_COMMAND_SPACE + _EVENTHOUSE_ENTITY_NAME
    + r"|materialized-views)[ \t\r\n]*",
    re.IGNORECASE | re.ASCII,
)
# The characters that .NET treats as white space; the query policy is shared
# with the platform, so blank and trim checks use the same set.
_EVENTHOUSE_WHITESPACE = (
    "\t\n\v\f\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006"
    "\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"
)
_EVENTHOUSE_LEADING_SPACE = frozenset(_EVENTHOUSE_WHITESPACE + "\ufeff")
_EVENTHOUSE_DATETIME_PATTERN = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,7}))?)?"
    r"(?:Z|([+-])(\d{2}):(\d{2}))",
    re.ASCII,
)
# 7d, -1.5h, 100ms or [-][d.]hh:mm:ss[.fffffff]
_EVENTHOUSE_TIMESPAN_PATTERN = re.compile(
    r"-?[0-9]{1,7}(?:\.[0-9]{1,7})?(?:d|h|m|s|ms)"
    r"|-?(?:[0-9]{1,7}\.)?(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]{1,7})?",
    re.ASCII,
)
_EVENTHOUSE_MAXIMUM_DYNAMIC_ITEMS = 1000
_EVENTHOUSE_MAXIMUM_DYNAMIC_LITERAL_LENGTH = 100000
_EVENTHOUSE_LITERAL_ESCAPES = {'"': '\\"', "\\": "\\\\", "\n": "\\n", "\r": "\\r", "\t": "\\t"}


class _EventhouseError(Exception):
    def __init__(self, code: str, message: str, details: str = "400",
                 category: str = "query"):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details
        self.category = category


def _eventhouse_invalid_input(message: str) -> _EventhouseError:
    return _EventhouseError("INVALID_QUERY_INPUT", message)


def _eventhouse_invalid_policy(message: str) -> _EventhouseError:
    return _EventhouseError("INVALID_QUERY_POLICY", message, category="configuration")


def _eventhouse_invalid_target(message: str) -> _EventhouseError:
    return _EventhouseError("INVALID_CONNECTOR_TARGET", message, category="configuration")


def _eventhouse_length(value: str) -> int:
    # Lengths are counted in UTF-16 code units, as the platform counts them.
    return len(value.encode("utf-16-le", "surrogatepass")) // 2


def _eventhouse_is_blank(value: str) -> bool:
    return all(character in _EVENTHOUSE_WHITESPACE for character in value)


def _eventhouse_has_control(value: str) -> bool:
    return any(
        character < "\x20" or "\x7f" <= character <= "\x9f" for character in value
    )


def _eventhouse_has_lone_surrogate(value: str) -> bool:
    return any("\ud800" <= character <= "\udfff" for character in value)


def _eventhouse_is_identifier(value: object) -> bool:
    return isinstance(value, str) and _EVENTHOUSE_IDENTIFIER_PATTERN.fullmatch(value) is not None


def _eventhouse_endpoint(value: object) -> Optional[str]:
    if (
        isinstance(value, str)
        and value.isascii()
        and _EVENTHOUSE_ENDPOINT_PATTERN.fullmatch(value)
    ):
        hostname = urlparse(value).hostname
        if hostname is not None and len(hostname) <= 253:
            return f"https://{hostname}"
    return None


def _eventhouse_is_query_text(text: str) -> bool:
    # Skip white space and nested comments; the first remaining character must
    # not start a management command.
    index = 0
    while index < len(text):
        if text[index] in _EVENTHOUSE_LEADING_SPACE:
            index += 1
        elif text.startswith("//", index):
            index += 2
            while index < len(text) and text[index] not in "\r\n":
                index += 1
        elif text.startswith("/*", index):
            index += 2
            depth = 1
            while index < len(text) and depth > 0:
                if text.startswith("/*", index):
                    depth += 1
                    index += 2
                elif text.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth > 0:
                return False
        else:
            return text[index] != "."
    return False


def _eventhouse_validate_template(template: object) -> None:
    if not isinstance(template, dict) or set(template) != _EVENTHOUSE_TEMPLATE_FIELDS:
        raise _eventhouse_invalid_policy(
            "Each query requires exactly operation, text, parameters, and roles."
        )

    operation = template["operation"]
    if not isinstance(operation, str) or operation not in _EVENTHOUSE_ROUTES:
        raise _eventhouse_invalid_policy("Query operation must be executeQuery or executeCommand.")

    text = template["text"]
    if (
        not isinstance(text, str)
        or _eventhouse_is_blank(text)
        or _eventhouse_length(text) > _EVENTHOUSE_MAXIMUM_TEXT_LENGTH
        or "\0" in text
    ):
        raise _eventhouse_invalid_policy(
            "Query text must be nonblank, contain no NUL, and not exceed 65536 characters."
        )
    if operation == "executeCommand" and (
        not text.isascii() or _EVENTHOUSE_METADATA_COMMAND_PATTERN.fullmatch(text) is None
    ):
        raise _eventhouse_invalid_policy(
            "Command templates require one allowed database-scoped .show metadata command "
            "for the bound database. Use .show tables or .show database schema; do not use "
            "comments, pipes, multiple statements, or explicit database names."
        )
    if operation == "executeQuery" and not _eventhouse_is_query_text(text):
        raise _eventhouse_invalid_policy(
            "Query templates require query text, not a management command or an "
            "empty/comment-only query. Register allowed metadata commands with operation "
            "executeCommand."
        )

    parameters = template["parameters"]
    if not isinstance(parameters, dict) or len(parameters) > _EVENTHOUSE_MAXIMUM_PARAMETERS:
        raise _eventhouse_invalid_policy(
            "Each query requires an explicit parameters dictionary with at most 128 entries; "
            "use {} for none."
        )
    if any(
        not _eventhouse_is_identifier(name)
        or not isinstance(kind, str)
        or kind not in _EVENTHOUSE_PARAMETER_TYPES
        for name, kind in parameters.items()
    ):
        raise _eventhouse_invalid_policy(
            "Parameters require simple identifier names and one of string, long, real, bool, "
            "datetime, guid, timespan, dynamic."
        )

    roles = template["roles"]
    if (
        not isinstance(roles, list)
        or not 0 < len(roles) <= _EVENTHOUSE_MAXIMUM_ROLES
        or any(
            not isinstance(role, str)
            or _eventhouse_is_blank(role)
            or _eventhouse_length(role) > _EVENTHOUSE_MAXIMUM_ROLE_LENGTH
            or role != role.strip(_EVENTHOUSE_WHITESPACE)
            or _eventhouse_has_control(role)
            for role in roles
        )
        or len(set(roles)) != len(roles)
    ):
        raise _eventhouse_invalid_policy(
            "Each query requires 1 to 128 distinct, nonblank role names of at most 128 "
            "characters without surrounding whitespace or control characters."
        )


def _eventhouse_settings(settings: object) -> tuple:
    if settings is None or settings == {}:
        raise _EventhouseError(
            "QUERY_POLICY_REQUIRED",
            "Eventhouse requires a nonempty queries policy. Declare named templates and "
            "redeploy; raw KQL invocation is not supported.",
            category="configuration",
        )
    if not isinstance(settings, dict) or not set(settings) <= _EVENTHOUSE_SETTINGS_FIELDS:
        raise _eventhouse_invalid_policy(
            "Eventhouse connector settings accept only queryServiceUri, database, and queries."
        )

    endpoint = _eventhouse_endpoint(settings.get(_EVENTHOUSE_TARGET_FIELD))
    if endpoint is None:
        raise _eventhouse_invalid_target(
            "queryServiceUri must be a root HTTPS Fabric Kusto endpoint."
        )
    database = settings.get(_EVENTHOUSE_DATABASE_FIELD)
    if not isinstance(database, str) or _EVENTHOUSE_GUID_PATTERN.fullmatch(database) is None:
        raise _eventhouse_invalid_target("database must be the KQL database item ID.")

    queries = settings.get(_EVENTHOUSE_QUERIES_FIELD)
    if not isinstance(queries, dict) or not queries:
        raise _EventhouseError(
            "QUERY_POLICY_REQUIRED",
            "Eventhouse requires a nonempty queries policy. Declare named templates and "
            "redeploy; raw KQL invocation is not supported.",
            category="configuration",
        )
    if len(queries) > _EVENTHOUSE_MAXIMUM_TEMPLATES:
        raise _eventhouse_invalid_policy("A query policy supports at most 256 templates.")
    for query_id, template in queries.items():
        if not _eventhouse_is_identifier(query_id):
            raise _eventhouse_invalid_policy(
                "Query IDs must be simple identifiers of 1 to 128 characters with a template object."
            )
        _eventhouse_validate_template(template)
    return endpoint, database, queries


def _eventhouse_caller(context: object) -> tuple:
    if (
        not isinstance(context, dict)
        or type(context.get(_EVENTHOUSE_AUTHENTICATED_FIELD)) is not bool
        or not isinstance(context.get(_EVENTHOUSE_ROLES_FIELD), list)
        or any(not isinstance(role, str) for role in context[_EVENTHOUSE_ROLES_FIELD])
    ):
        raise _EventhouseError(
            "INVALID_CALLER_CONTEXT",
            "The host did not supply a valid caller context.",
            details="500",
            category="configuration",
        )
    return context[_EVENTHOUSE_AUTHENTICATED_FIELD], context[_EVENTHOUSE_ROLES_FIELD]


def _eventhouse_role(role: str) -> str:
    # Built-in roles match case-insensitively; custom roles are case-sensitive.
    if _eventhouse_is_blank(role) or (
        role.isascii() and role.lower() == _EVENTHOUSE_AUTHENTICATED_ROLE
    ):
        return _EVENTHOUSE_AUTHENTICATED_ROLE
    if role.isascii() and role.lower() == _EVENTHOUSE_ANONYMOUS_ROLE:
        return _EVENTHOUSE_ANONYMOUS_ROLE
    return role


def _eventhouse_string_literal(value: str) -> str:
    characters = ['"']
    for character in value:
        escape = _EVENTHOUSE_LITERAL_ESCAPES.get(character)
        if escape is not None:
            characters.append(escape)
        elif " " <= character <= "~":
            characters.append(character)
        else:
            code = ord(character)
            if code > 0xFFFF:
                code -= 0x10000
                characters.append(
                    "\\u%04X\\u%04X" % (0xD800 + (code >> 10), 0xDC00 + (code & 0x3FF))
                )
            else:
                characters.append("\\u%04X" % code)
    characters.append('"')
    return "".join(characters)


def _eventhouse_datetime_literal(value: str) -> Optional[str]:
    match = _EVENTHOUSE_DATETIME_PATTERN.fullmatch(value)
    if match is None:
        return None
    year, month, day, hour, minute, second, fraction, sign, offset_hours, offset_minutes = (
        match.groups()
    )
    ticks = int(fraction.ljust(7, "0")) if fraction else 0
    offset = timedelta(0)
    if sign:
        if int(offset_hours) > 14 or int(offset_minutes) > 59 or (
            int(offset_hours) == 14 and int(offset_minutes) != 0
        ):
            return None
        offset = timedelta(hours=int(offset_hours), minutes=int(offset_minutes))
        if sign == "-":
            offset = -offset
    try:
        moment = datetime(
            int(year), int(month), int(day), int(hour), int(minute), int(second or 0),
            ticks // 10, tzinfo=timezone(offset),
        ).astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None
    return (
        f"datetime({moment.year:04d}-{moment.month:02d}-{moment.day:02d}"
        f"T{moment.hour:02d}:{moment.minute:02d}:{moment.second:02d}"
        f".{moment.microsecond:06d}{ticks % 10}Z)"
    )


def _eventhouse_dynamic_literal(value: object) -> Optional[str]:
    # Only a flat array of scalars; json.dumps emits exactly one literal.
    if type(value) is not list or len(value) > _EVENTHOUSE_MAXIMUM_DYNAMIC_ITEMS:
        return None
    for item in value:
        if item is None or type(item) is bool:
            continue
        if type(item) is int:
            if not -(2 ** 63) <= item < 2 ** 63:
                return None
        elif type(item) is float:
            if not math.isfinite(item):
                return None
        elif type(item) is str:
            if _eventhouse_has_lone_surrogate(item):
                return None
        else:
            return None
    literal = "dynamic(" + json.dumps(
        value, ensure_ascii=True, separators=(",", ":"), allow_nan=False
    ) + ")"
    if len(literal) > _EVENTHOUSE_MAXIMUM_DYNAMIC_LITERAL_LENGTH:
        return None
    return literal


def _eventhouse_literal(value: object, kind: str) -> Optional[str]:
    if kind == "string":
        if isinstance(value, str) and not _eventhouse_has_lone_surrogate(value):
            return _eventhouse_string_literal(value)
    elif kind == "long":
        if type(value) is int and -(2 ** 63) <= value < 2 ** 63:
            return f"long({value})"
    elif kind == "real":
        if type(value) in (int, float):
            try:
                number = float(value)
            except OverflowError:
                return None
            if math.isfinite(number):
                return f"real({number!r})"
    elif kind == "bool":
        if type(value) is bool:
            return "true" if value else "false"
    elif kind == "datetime":
        if isinstance(value, str):
            return _eventhouse_datetime_literal(value)
    elif kind == "guid":
        if isinstance(value, str) and _EVENTHOUSE_GUID_PATTERN.fullmatch(value):
            return f"guid({value.lower()})"
    elif kind == "timespan":
        if isinstance(value, str) and _EVENTHOUSE_TIMESPAN_PATTERN.fullmatch(value):
            return f"timespan({value})"
    elif kind == "dynamic":
        return _eventhouse_dynamic_literal(value)
    return None


def _eventhouse_binding(binding: object, operation: object) -> None:
    if (
        not isinstance(binding, dict)
        or ("operation" in binding and binding["operation"] != operation)
        or ("function" in binding and binding["function"] != _EVENTHOUSE_FUNCTION_NAME)
    ):
        raise _eventhouse_invalid_policy(
            "The host binding does not match this function and operation."
        )


def _eventhouse_request(payload: object) -> tuple:
    if not isinstance(payload, dict):
        raise _eventhouse_invalid_input("The request payload must be an object.")
    endpoint, database, queries = _eventhouse_settings(
        payload.get(_EVENTHOUSE_SETTINGS_PAYLOAD_FIELD)
    )
    if _EVENTHOUSE_BINDING_PAYLOAD_FIELD in payload:
        _eventhouse_binding(payload[_EVENTHOUSE_BINDING_PAYLOAD_FIELD], payload.get("operation"))
    is_authenticated, caller_roles = _eventhouse_caller(
        payload.get(_EVENTHOUSE_CALLER_PAYLOAD_FIELD)
    )

    if (
        not set(payload) <= _EVENTHOUSE_PAYLOAD_FIELDS
        or not isinstance(payload.get("operation"), str)
        or not isinstance(payload.get("input"), dict)
        or not set(payload["input"]) <= _EVENTHOUSE_INPUT_FIELDS
    ):
        raise _eventhouse_invalid_input(
            "Supply only operation and input containing queryId, parameters, and "
            "clientRequestId; raw text, routing, options, and roles are not accepted."
        )
    operation, input_data = payload["operation"], payload["input"]

    if "itemId" in input_data and (
        not isinstance(input_data["itemId"], str)
        or input_data["itemId"].lower() != database.lower()
    ):
        raise _eventhouse_invalid_target(
            "The configured KQL database does not match the saved connector settings."
        )

    query_id = input_data.get("queryId")
    if not _eventhouse_is_identifier(query_id):
        raise _eventhouse_invalid_input("A simple identifier queryId is required.")

    template = queries.get(query_id)
    if template is None or template["operation"] != operation:
        raise _EventhouseError(
            "QUERY_NOT_ALLOWED",
            "The requested template is not available for this connector and operation.",
            details="403",
        )

    if not is_authenticated or any(
        _eventhouse_role(role) == _EVENTHOUSE_ANONYMOUS_ROLE for role in caller_roles
    ):
        raise _EventhouseError(
            "QUERY_ROLE_NOT_ALLOWED",
            "An authenticated caller with a permitted template role is required.",
            details="403",
        )
    roles = {_eventhouse_role(role) for role in caller_roles} | {_EVENTHOUSE_AUTHENTICATED_ROLE}
    if not any(_eventhouse_role(role) in roles for role in template["roles"]):
        raise _EventhouseError(
            "QUERY_ROLE_NOT_ALLOWED",
            "The caller role is not permitted for this template.",
            details="403",
        )

    if "parameters" in input_data and not isinstance(input_data["parameters"], dict):
        raise _eventhouse_invalid_input("Parameters must be an object of non-null JSON scalars.")
    supplied = input_data.get("parameters", {})
    declared = template["parameters"]
    if len(supplied) != len(declared) or any(name not in declared for name in supplied):
        raise _eventhouse_invalid_input(
            "Supply exactly the required template parameters, without additional names."
        )
    literals = {}
    for name, kind in declared.items():
        if supplied.get(name) is None:
            raise _eventhouse_invalid_input(
                "All template parameters are required and must be non-null."
            )
        literal = _eventhouse_literal(supplied[name], kind)
        if literal is None:
            raise _eventhouse_invalid_input(
                "A parameter does not match its declared JSON type; datetime requires ISO 8601 "
                "with an explicit time zone, guid requires the hyphenated format, timespan "
                "requires a Kusto timespan such as 7d or 01:30:00, and dynamic requires a flat "
                "array of scalars."
            )
        literals[name] = literal

    client_request_id = f"KPC.rayfin_kusto_v1;{uuid.uuid4()}"
    if "clientRequestId" in input_data:
        client_request_id = input_data["clientRequestId"]
        if (
            not isinstance(client_request_id, str)
            or _eventhouse_is_blank(client_request_id)
            or len(client_request_id) > _EVENTHOUSE_MAXIMUM_REQUEST_ID_LENGTH
            or not client_request_id.isascii()
            or _eventhouse_has_control(client_request_id)
        ):
            raise _eventhouse_invalid_input(
                "clientRequestId must be a nonblank ASCII string of at most 256 characters "
                "without control characters."
            )

    url = f"{endpoint}/v1/rest/{_EVENTHOUSE_ROUTES[operation]}"
    body = {
        "db": database,
        "csl": template["text"],
        "properties": json.dumps({
            "Options": {
                "request_readonly": True,
                "request_readonly_hardline": True,
                "request_remote_entities_disabled": True,
            },
            "Parameters": literals,
        }),
    }
    return url, body, client_request_id


@udf.generic_connection(argName="kustoClient", audienceType="Kusto")
@udf.streaming_function()
async def rayfin_kusto_v1(payload: dict, kustoClient: fn.FabricItem) -> fn.StreamResponse:
    try:
        url, body, client_request_id = _eventhouse_request(payload)
    except _EventhouseError as error:
        logging.warning("rayfin_kusto_v1: rejected the request with %s", error.code)
        return _error_response(error.message, error.code, error.category, error.details)

    access_token = kustoClient.get_access_token().get_token().token
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": _JSON_MEDIA_TYPE,
        "Accept": _JSON_MEDIA_TYPE,
        "x-ms-client-request-id": client_request_id,
    }

    session = await _get_session()
    resp = await session.post(url, json=body, headers=headers, allow_redirects=False)

    if resp.status != 200:
        detail = await resp.text()
        await resp.release()
        return fn.StreamResponse(
            iter([detail.encode("utf-8")]),
            media_type=resp.headers.get("Content-Type", _JSON_MEDIA_TYPE),
            status_code=resp.status,
        )

    async def relay():
        try:
            async for chunk in resp.content.iter_any():
                yield chunk
        finally:
            await resp.release()

    return fn.StreamResponse(relay(), media_type=_JSON_MEDIA_TYPE)


# ---------------------------------------------------------------------------
# Azure Connector Namespace (ACN) -- shared adapter for the M365 connectors
# ---------------------------------------------------------------------------
# No bearer token is passed in: the connection's access policy is bound to this
# function's managed identity, so the `azure-connectors` SDK mints its own. In
# exchange for its retries and typed requests it buffers rather than streams.
# Operation names are flat camelCase and match rayfin.yml and the generated
# TypeScript SDK 1:1; upstream JSON passes through as-is. Binary content,
# triggers and http_request are excluded.

# connector slug -> (SDK module, client class)
_ACN_CONNECTORS = {
    "office365users": ("azure.connectors.office365users", "Office365usersClient"),
    "sharepointonline": ("azure.connectors.sharepointonline", "SharepointonlineClient"),
    "office365": ("azure.connectors.office365", "Office365Client"),
    "excelonlinebusiness": ("azure.connectors.excelonlinebusiness", "ExcelonlinebusinessClient"),
    "teams": ("azure.connectors.teams", "TeamsClient"),
    "onedriveforbusiness": ("azure.connectors.onedriveforbusiness", "OnedriveforbusinessClient"),
    "office365groups": ("azure.connectors.office365groups", "Office365groupsClient"),
    "microsoftforms": ("azure.connectors.microsoftforms", "MicrosoftformsClient"),
    "planner": ("azure.connectors.planner", "PlannerClient"),
    "azuread": ("azure.connectors.azuread", "AzureadClient"),
}
# Keyed by (slug, connectionRuntimeUrl). Deliberately never closed: each client
# owns an aiohttp pool and a credential, and rebuilding both per invocation
# would cost a handshake and an IMDS probe.
_acn_clients: dict = {}
_acn_clients_lock = asyncio.Lock()


def _new_acn_client(slug: str, connectionRuntimeUrl: str, **kwargs):
    # Deferred so a missing module fails this connector alone, not every
    # function in the app at import time (requirements.txt is generated by Fabric).
    import importlib

    moduleName, className = _ACN_CONNECTORS[slug]
    clientClass = getattr(importlib.import_module(moduleName), className)
    return clientClass(connectionRuntimeUrl, **kwargs)


async def _get_acn_client(slug: str, connectionRuntimeUrl: str):
    key = (slug, connectionRuntimeUrl)
    client = _acn_clients.get(key)
    if client is not None:
        return client

    async with _acn_clients_lock:
        client = _acn_clients.get(key)
        if client is None:
            # No credential argument -> the SDK's ManagedIdentityTokenProvider.
            client = _new_acn_client(slug, connectionRuntimeUrl)
            _acn_clients[key] = client
    return client


def _required(input_data: dict, name: str):
    value = input_data.get(name)
    if not value:
        raise ValueError(f"'{name}' is required for this operation")
    return value


def _bool(value) -> bool:
    # bool("false") is True, so string flags are parsed explicitly.
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return bool(value)


def _required_key(input_data: dict, name: str):
    # Like _required, but admits falsy values such as 0 (a valid numeric id).
    value = input_data.get(name)
    if value is None or value == "":
        raise ValueError(f"'{name}' is required for this operation")
    return value


# operation -> (SDK coroutine name, kwargs builder). kwargs differ per operation,
# so no generic splat.

# Office 365 Users (App + Delegated). Excluded: userPhoto (raw bytes), the
# profile writes, httpRequest (untyped).
_OFFICE365USERS_OPERATIONS = {
    "managerAsync": ("manager_async", lambda d: {
        "id": _required(d, "userId"),
        "select": d.get("select"),
    }),
    "userProfileAsync": ("user_profile_async", lambda d: {
        "id": _required(d, "userId"),
        "select": d.get("select"),
    }),
    "directReportsAsync": ("direct_reports_async", lambda d: {
        "id": _required(d, "userId"),
        "select": d.get("select"),
        "top": _query(d, "top"),
    }),
    "relevantPeopleAsync": ("relevant_people_async", lambda d: {
        "user_id": _required(d, "userId"),
    }),
    "myProfileAsync": ("my_profile_async", lambda d: {
        "select": d.get("select"),
    }),
    "searchUserAsync": ("search_user_async", lambda d: {
        "search_term": d.get("searchTerm"),
        "top": _query(d, "top"),
        "is_search_term_required": _query(d, "isSearchTermRequired"),
        "skip_token": d.get("skipToken"),
    }),
}


@udf.streaming_function()
async def rayfin_office365users_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("office365users", _OFFICE365USERS_OPERATIONS, payload)


def _query(input_data: dict, name: str):
    # The SDK types every query parameter as Optional[str] and sends it verbatim.
    value = input_data.get(name)
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if name == "top":
        # Graph rejects $top=0 with an opaque error, so validate it up front.
        try:
            top = int(value)
        except (TypeError, ValueError):
            top = 0
        if top < 1:
            raise ValueError("'top' must be a positive integer")
    return str(value)


# Errors go back as HTTP 200 with a non-Succeeded x-fabric-udf-status header
# and a UDFInvokeResponse body. The UDF host replaces any non-2xx response with
# a generic failure and drops the body, but for a 2xx with a non-Succeeded
# status it reads the body, keeps the error details, records the invocation as
# failed and maps the status to an HTTP error code for the caller.
_MAX_ERROR_MESSAGE_LENGTH = 1000
_EMPTY_INVOCATION_ID = "00000000-0000-0000-0000-000000000000"


def _udf_status(code: str, category: str, details: Optional[str]) -> str:
    if code == "Timeout":
        return "Timeout"
    if category == "query":
        return "BadRequest"
    if details and details.isdigit() and 400 <= int(details) < 500:
        return "BadRequest"
    return "Failed"


def _error_response(message: str, code: str, category: str,
                    details: Optional[str] = None) -> fn.StreamResponse:
    error = {"message": message, "code": code, "category": category}
    if details:
        error["details"] = details
    status = _udf_status(code, category, details)
    body = json.dumps({
        "functionName": "connector",
        "invocationId": _EMPTY_INVOCATION_ID,
        "status": status,
        "errors": [{
            # "UserThrown" (the value of the host's UserThrownError code) marks
            # a caller/upstream error, which the host returns as 422 rather than
            # 400.
            "errorCode": "UserThrown" if status == "BadRequest" else code,
            "subErrorCode": code,
            "message": message,
            "properties": error,
        }],
        "error": error,
    })
    return fn.StreamResponse(
        iter([body.encode("utf-8")]),
        media_type=_JSON_MEDIA_TYPE,
        status_code=200,
        headers={"x-fabric-udf-status": status},
    )


def _upstream_error(response_body) -> tuple:
    # Pull only the code and message out of the connector's error body; other
    # fields (source, path) can carry the connection URL.
    if not response_body:
        return None, None
    try:
        parsed = json.loads(response_body)
    except (TypeError, ValueError):
        return None, None
    if not isinstance(parsed, dict):
        return None, None

    code, message = None, None
    error = parsed.get("error")
    if isinstance(error, dict):
        code = error.get("code")
        message = error.get("message")
    elif isinstance(error, str):
        message = error
    if not message and isinstance(parsed.get("message"), str):
        message = parsed["message"]
    if not code and isinstance(parsed.get("code"), str):
        code = parsed["code"]

    if not isinstance(code, str):
        code = None
    if not isinstance(message, str) or not message.strip():
        message = None
    elif len(message) > _MAX_ERROR_MESSAGE_LENGTH:
        message = message[:_MAX_ERROR_MESSAGE_LENGTH] + "..."
    return code, message


async def _invoke_acn(slug: str, operations: dict, payload: dict) -> fn.StreamResponse:
    functionName = f"rayfin_{slug}_v1"
    operation = (payload.get("operation") or "").strip()
    input_data = payload.get("input", {})
    connectionRuntimeUrl = input_data.get("connectionRuntimeUrl")

    # Input errors return a BadRequest error result instead of an opaque 500.
    try:
        if not operation:
            raise ValueError("'operation' is required")
        if not connectionRuntimeUrl:
            raise ValueError("input.connectionRuntimeUrl is required")

        handler = operations.get(operation)
        if handler is None:
            supported = ", ".join(sorted(operations))
            raise ValueError(
                f"unsupported operation '{operation}'; expected one of: {supported}"
            )
        methodName, buildKwargs = handler
        kwargs = buildKwargs(input_data)
    except (ValueError, KeyError, TypeError) as exc:
        logging.warning("%s: rejected operation '%s': %s", functionName, operation, exc)
        return _error_response(str(exc), "BadRequest", "query", "400")

    # Every connector honours an accessToken passed internally by the platform
    # (used as-is) in place of managed identity; apps don't send it. It gets a
    # fresh, uncached client that is closed after the call; otherwise the cached
    # managed-identity client is reused.
    accessToken = input_data.get("accessToken")
    if accessToken:
        from azure.connectors.sdk.authentication import ConnectionStringTokenProvider

        client = _new_acn_client(
            slug, connectionRuntimeUrl, token_provider=ConnectionStringTokenProvider(accessToken)
        )
    else:
        client = await _get_acn_client(slug, connectionRuntimeUrl)

    try:
        return await _call_acn(client, methodName, kwargs, functionName, operation)
    finally:
        if accessToken:
            await client.close()


async def _call_acn(client, methodName: str, kwargs: dict, functionName: str,
                    operation: str) -> fn.StreamResponse:
    from azure.connectors import ConnectorException

    try:
        result = await getattr(client, methodName)(**kwargs)
        document = json.dumps(result) if result is not None else "null"
    except asyncio.TimeoutError:
        logging.warning("%s: operation '%s' timed out", functionName, operation)
        return _error_response(
            f"operation '{operation}' timed out", "Timeout", "network", "504"
        )
    except aiohttp.ClientError as exc:
        # Type name only: the message can embed the connection URL.
        logging.warning(
            "%s: operation '%s' transport error %s",
            functionName,
            operation,
            type(exc).__name__,
        )
        return _error_response(
            f"operation '{operation}' failed to reach the connector",
            "ConnectorUnreachable",
            "network",
            "502",
        )
    except ConnectorException as exc:
        # Never log or return str(exc), exc.path or exc.operation: they embed
        # the connection id.
        logging.warning(
            "%s: operation '%s' failed with status %s",
            functionName,
            operation,
            exc.status_code,
        )
        upstreamCode, upstreamMessage = _upstream_error(exc.response_body)
        status = exc.status_code or 502
        return _error_response(
            upstreamMessage or f"operation '{operation}' failed with status {status}",
            upstreamCode or f"Http{status}",
            "api",
            str(status),
        )
    except Exception as exc:
        # Type name only, for the same reason as above.
        logging.error(
            "%s: operation '%s' failed unexpectedly: %s",
            functionName,
            operation,
            type(exc).__name__,
        )
        return _error_response(
            f"operation '{operation}' failed unexpectedly",
            "InternalError",
            "unknown",
            type(exc).__name__,
        )

    return fn.StreamResponse(
        iter([document.encode("utf-8")]),
        media_type=_JSON_MEDIA_TYPE,
        status_code=200,
    )


# SharePoint (App + Delegated). Excluded: file content (binary), sharing and
# permission changes, hub-site ops, triggers.
def _sharepoint_list(d: dict) -> dict:
    return {"dataset": _required(d, "siteUrl"), "table": _required(d, "listName")}


_SHAREPOINTONLINE_OPERATIONS = {
    "getDataSetsAsync": ("get_data_sets_async", lambda d: {}),
    "getTablesAsync": ("get_tables_async", lambda d: {
        "dataset": _required(d, "siteUrl"),
    }),
    "getAllTablesAsync": ("get_all_tables_async", lambda d: {
        "dataset": _required(d, "siteUrl"),
    }),
    "getTableAsync": ("get_table_async", lambda d: {
        **_sharepoint_list(d),
        "view": d.get("view"),
    }),
    "getItemsAsync": ("get_items_async", lambda d: {
        "dataset": _required(d, "siteUrl"),
        "table": _required(d, "listName"),
        "filter": d.get("filter"),
        "orderby": d.get("orderby"),
        "top": _query(d, "top"),
        "folder_path": d.get("folderPath"),
    }),
    "getItemAsync": ("get_item_async", lambda d: {
        "dataset": _required(d, "siteUrl"),
        "table": _required(d, "listName"),
        # Not `itemId`: BaaS reserves that name for the Fabric item and strips it
        # from the caller's input before invoking, so it never reaches the adapter.
        "id": _required_key(d, "id"),
    }),
    "postItemAsync": ("post_item_async", lambda d: {
        **_sharepoint_list(d),
        "input": _required(d, "item"),
    }),
    "patchItemAsync": ("patch_item_async", lambda d: {
        **_sharepoint_list(d),
        "id": str(_required_key(d, "id")),
        "input": _required(d, "item"),
    }),
    "deleteItemAsync": ("delete_item_async", lambda d: {
        **_sharepoint_list(d),
        "id": str(_required_key(d, "id")),
    }),
    "getItemAttachmentsAsync": ("get_item_attachments_async", lambda d: {
        **_sharepoint_list(d),
        "item_id": str(_required_key(d, "id")),
    }),
    "getFileItemsAsync": ("get_file_items_async", lambda d: {
        **_sharepoint_list(d),
        "filter": d.get("filter"),
        "orderby": d.get("orderby"),
        "top": _query(d, "top"),
        "folder_path": d.get("folderPath"),
    }),
    "listRootFolderAsync": ("list_root_folder_async", lambda d: {
        "dataset": _required(d, "siteUrl"),
    }),
    "listFolderAsync": ("list_folder_async", lambda d: {
        "dataset": _required(d, "siteUrl"),
        "id": _required(d, "folderId"),
    }),
    "getFileMetadataByPathAsync": ("get_file_metadata_by_path_async", lambda d: {
        "dataset": _required(d, "siteUrl"),
        "path": _required(d, "path"),
    }),
}


# Outlook, Teams and OneDrive are App only: every op runs as the connection
# owner. This adapter exposes every supported op; BaaS rejects any op the app
# did not declare under `operations:` in rayfin.yml, so builders opt in per app
# to owner-wide reads (mailbox, chats, drive root) and owner writes (send mail,
# post to Teams, create or share files).


# Outlook (App only). calendarId is meant to be pinned per connection; until
# BaaS injects it, the caller supplies it.
def _include_attachments(d: dict) -> str:
    # Off by default: attachment content comes back base64-encoded and can be large.
    return "true" if _bool(d.get("includeAttachments")) else "false"


_OFFICE365_OWNER_WIDE_OPERATIONS = {
    "getEmailsAsync": ("get_emails_async", lambda d: {
        "folder_path": d.get("folderPath"),
        "from_": d.get("from"),
        "to": d.get("to"),
        "subject_filter": d.get("subjectFilter"),
        "fetch_only_unread": _query(d, "fetchOnlyUnread"),
        "search_query": d.get("searchQuery"),
        "top": _query(d, "top"),
        "include_attachments": _include_attachments(d),
    }),
    "getEmailAsync": ("get_email_async", lambda d: {
        "message_id": _required(d, "messageId"),
        "include_attachments": _include_attachments(d),
    }),
    "calendarGetTablesAsync": ("calendar_get_tables_async", lambda d: {}),
}

_OFFICE365_OPERATIONS = {
    "getEventsCalendarViewAsync": ("get_events_calendar_view_async", lambda d: {
        "calendar_id": _required(d, "calendarId"),
        "start_date_time_utc": _required(d, "startDateTimeUtc"),
        "end_date_time_utc": _required(d, "endDateTimeUtc"),
        "filter": d.get("filter"),
        "top": _query(d, "top"),
    }),
    # POST with read semantics; the SDK serializes a wire-named dict body as-is.
    "findMeetingTimesAsync": ("find_meeting_times_async", lambda d: {
        "input": _required(d, "request"),
    }),
    "getMailTipsAsync": ("get_mail_tips_async", lambda d: {
        "input": _required(d, "request"),
    }),
    "getRoomListsAsync": ("get_room_lists_async", lambda d: {}),
    "getRoomsAsync": ("get_rooms_async", lambda d: {}),
}

_OFFICE365_OWNER_WRITE_OPERATIONS = {
    "sendEmailAsync": ("send_email_async", lambda d: {
        "input": _required(d, "request"),
    }),
    "sharedMailboxSendEmailAsync": ("shared_mailbox_send_email_async", lambda d: {
        "input": _required(d, "request"),
    }),
    "replyToAsync": ("reply_to_async", lambda d: {
        "message_id": _required(d, "messageId"),
        "input": _required(d, "request"),
        "mailbox_address": d.get("mailboxAddress"),
    }),
    "calendarPostItemAsync": ("calendar_post_item_async", lambda d: {
        "table": _required(d, "calendarId"),
        "input": _required(d, "request"),
    }),
}


# Excel (App + Delegated). `source` is "me" (OneDrive) or a SharePoint site URL;
# under App auth "me" is the connection owner.
def _excel_file(d: dict) -> dict:
    return {
        "source": _required(d, "source"),
        "drive": _required(d, "driveId"),
        "file": _required(d, "fileId"),
    }


_EXCELONLINEBUSINESS_OPERATIONS = {
    "getTablesAsync": ("get_tables_async", lambda d: _excel_file(d)),
    "getItemsAsync": ("get_items_async", lambda d: {
        **_excel_file(d),
        "table": _required(d, "tableName"),
        "filter": d.get("filter"),
        "orderby": d.get("orderby"),
        "top": _query(d, "top"),
        "skip": _query(d, "skip"),
        "select": d.get("select"),
    }),
    "getItemAsync": ("get_item_async", lambda d: {
        **_excel_file(d),
        "table": _required(d, "tableName"),
        "id_column": _required(d, "keyColumn"),
        "id": _required(d, "keyValue"),
    }),
    "addRowAsync": ("add_row_async", lambda d: {
        **_excel_file(d),
        "table": _required(d, "tableName"),
        "input": _required(d, "item"),
    }),
    "patchItemAsync": ("patch_item_async", lambda d: {
        **_excel_file(d),
        "table": _required(d, "tableName"),
        "id_column": _required(d, "keyColumn"),
        "id": str(_required_key(d, "keyValue")),
        "input": _required(d, "item"),
    }),
}


# Teams (App only). teamId/channelId are meant to be pinned per connection;
# until BaaS injects them, the caller supplies them. Excluded: team/channel/
# member management, card-and-wait, feed notifications, triggers.
_TEAMS_OWNER_WIDE_OPERATIONS = {
    "getAllTeamsAsync": ("get_all_teams_async", lambda d: {}),
    "getMessagesFromChatAsync": ("get_messages_from_chat_async", lambda d: {
        "chat_id": _required(d, "chatId"),
        "filter": d.get("filter"),
        "orderby": d.get("orderby"),
        "top": _query(d, "top"),
    }),
    "listMeetingTranscriptsAsync": ("list_meeting_transcripts_async", lambda d: {
        "meeting_id": _required(d, "meetingId"),
    }),
    "getMeetingTranscriptContentAsync": ("get_meeting_transcript_content_async", lambda d: {
        "meeting_id": _required(d, "meetingId"),
        "transcript_id": _required(d, "transcriptId"),
    }),
}

_TEAMS_OPERATIONS = {
    "getTeamAsync": ("get_team_async", lambda d: {
        "team_id": _required(d, "teamId"),
    }),
    "getAllChannelsForTeamAsync": ("get_all_channels_for_team_async", lambda d: {
        "group_id": _required(d, "teamId"),
        "filter": d.get("filter"),
        "orderby": d.get("orderby"),
    }),
    "getChannelsForGroupAsync": ("get_channels_for_group_async", lambda d: {
        "group_id": _required(d, "teamId"),
        "filter": d.get("filter"),
        "orderby": d.get("orderby"),
    }),
    "listTeamMembersAsync": ("list_team_members_async", lambda d: {
        "team_id": _required(d, "teamId"),
        "filter": d.get("filter"),
        "top": _query(d, "top"),
    }),
    "getMessagesFromChannelAsync": ("get_messages_from_channel_async", lambda d: {
        "group_id": _required(d, "teamId"),
        "channel_id": _required(d, "channelId"),
    }),
    "listRepliesToMessageAsync": ("list_replies_to_message_async", lambda d: {
        "group_id": _required(d, "teamId"),
        "channel_id": _required(d, "channelId"),
        "message_id": _required(d, "messageId"),
        "top": _query(d, "top"),
    }),
}


# poster ("Flow bot" / "User") and location ("Channel" / "Group chat" / ...)
# pick the conversation; the request body carries recipient and content.
def _teams_post(d: dict) -> dict:
    return {
        "poster": _required(d, "poster"),
        "location": _required(d, "location"),
        "input": _required(d, "request"),
    }


_TEAMS_OWNER_WRITE_OPERATIONS = {
    "postMessageToConversationAsync": ("post_message_to_conversation_async", _teams_post),
    "postCardToConversationAsync": ("post_card_to_conversation_async", _teams_post),
    "replyWithMessageToConversationAsync": ("reply_with_message_to_conversation_async", _teams_post),
    "updateCardInConversationAsync": ("update_card_in_conversation_async", _teams_post),
    "createTeamsMeetingAsync": ("create_teams_meeting_async", lambda d: {
        "calendarid": _required(d, "calendarId"),
        "input": _required(d, "request"),
    }),
}


# OneDrive (App only). folderId is meant to be pinned per connection; until
# BaaS injects it, the caller supplies it. Excluded: file content reads
# (binary), delete, triggers.
_ONEDRIVEFORBUSINESS_OWNER_WIDE_OPERATIONS = {
    "listRootFolderAsync": ("list_root_folder_async", lambda d: {}),
    "getFileMetadataByPathAsync": ("get_file_metadata_by_path_async", lambda d: {
        "path": _required(d, "path"),
    }),
    "findFilesByPathAsync": ("find_files_by_path_async", lambda d: {
        "query": _required(d, "query"),
        "path": _required(d, "path"),
        "find_mode": _required(d, "findMode"),
        "max_file_count": _query(d, "maxFileCount"),
    }),
}

_ONEDRIVEFORBUSINESS_OPERATIONS = {
    "listFolderAsync": ("list_folder_async", lambda d: {
        "id": _required(d, "folderId"),
    }),
    "getFileMetadataAsync": ("get_file_metadata_async", lambda d: {
        "id": _required(d, "fileId"),
    }),
    "getFileThumbnailAsync": ("get_file_thumbnail_async", lambda d: {
        "id": _required(d, "fileId"),
        "size": _required(d, "size"),
    }),
}


def _file_content(d: dict) -> bytes:
    # JSON cannot carry raw bytes, so the caller sends base64; the SDK uploads
    # a bytes body verbatim. Empty content ("") is a valid empty file.
    content = d.get("content")
    if content is None:
        raise ValueError("'content' is required for this operation")
    try:
        return base64.b64decode(content, validate=True)
    except (binascii.Error, TypeError) as exc:
        raise ValueError("'content' must be base64-encoded") from exc


_ONEDRIVEFORBUSINESS_OWNER_WRITE_OPERATIONS = {
    "createFileAsync": ("create_file_async", lambda d: {
        "folder_path": _required(d, "folderPath"),
        "name": _required(d, "name"),
        "input": _file_content(d),
    }),
    "createShareLinkAsync": ("create_share_link_async", lambda d: {
        "id": _required(d, "fileId"),
        "type_": _required(d, "type"),
        "scope": d.get("scope"),
    }),
    "copyDriveFileAsync": ("copy_drive_file_async", lambda d: {
        "id": _required(d, "fileId"),
        "destination": _required(d, "destination"),
        "overwrite": _query(d, "overwrite"),
    }),
    "moveFileAsync": ("move_file_async", lambda d: {
        "id": _required(d, "fileId"),
        "destination": _required(d, "destination"),
        "overwrite": _query(d, "overwrite"),
    }),
}


# Office 365 Groups (App + Delegated). Membership changes and deleted-group
# restore are never admitted.
_OFFICE365GROUPS_OPERATIONS = {
    "listGroupsAsync": ("list_groups_async", lambda d: {
        "filter": d.get("filter"),
        "top": _query(d, "top"),
        "skiptoken": d.get("skipToken"),
    }),
    "listOwnedGroupsAsync": ("list_owned_groups_async", lambda d: {}),
    "listGroupMembersAsync": ("list_group_members_async", lambda d: {
        "group_id": _required(d, "groupId"),
        "top": _query(d, "top"),
    }),
    "createCalendarEventAsync": ("create_calendar_event_async", lambda d: {
        "group_id": _required(d, "groupId"),
        "input": _required(d, "request"),
    }),
    "updateCalendarEventAsync": ("update_calendar_event_async", lambda d: {
        "group_id": _required(d, "groupId"),
        "event": _required(d, "eventId"),
        "input": _required(d, "request"),
    }),
    "calendarDeleteItemAsync": ("calendar_delete_item_async", lambda d: {
        "group_id": _required(d, "groupId"),
        "event": _required(d, "eventId"),
    }),
}


# Microsoft Forms (App + Delegated). Read-only connector; the SDK has no "list
# responses" operation, so response ids must come from elsewhere.
_MICROSOFTFORMS_OPERATIONS = {
    "listFormsAsync": ("list_forms_async", lambda d: {}),
    "getFormDetailsByIdAsync": ("get_form_details_by_id_async", lambda d: {
        "form_id": _required(d, "formId"),
    }),
    "getQuestionsAsync": ("get_questions_async", lambda d: {
        "form_id": _required(d, "formId"),
    }),
    "getFormResponseByIdAsync": ("get_form_response_by_id_async", lambda d: {
        "form_id": _required(d, "formId"),
        "response_id": _required(d, "responseId"),
    }),
}


# Planner (App + Delegated). Under App auth listMyTasksAsync returns the
# connection owner's tasks. Excluded: bucket management.
_PLANNER_OPERATIONS = {
    "listMyTasksAsync": ("list_my_tasks_async", lambda d: {}),
    "listGroupsAsync": ("list_groups_async", lambda d: {}),
    "listGroupPlansAsync": ("list_group_plans_async", lambda d: {
        "group_id": _required(d, "groupId"),
    }),
    "listBucketsAsync": ("list_buckets_async", lambda d: {
        "group_id": _required(d, "groupId"),
        "id": _required(d, "planId"),
    }),
    "listTasksAsync": ("list_tasks_async", lambda d: {
        "group_id": _required(d, "groupId"),
        "id": _required(d, "planId"),
    }),
    "getTaskAsync": ("get_task_async", lambda d: {
        "id": _required(d, "taskId"),
    }),
    "getTaskDetailsAsync": ("get_task_details_async", lambda d: {
        "id": _required(d, "taskId"),
    }),
    "createTaskAsync": ("create_task_async", lambda d: {
        "input": _required(d, "request"),
    }),
    "updateTaskAsync": ("update_task_async", lambda d: {
        "id": _required(d, "taskId"),
        "input": _required(d, "request"),
    }),
    "updateTaskDetailsAsync": ("update_task_details_async", lambda d: {
        "id": _required(d, "taskId"),
        "input": _required(d, "request"),
    }),
    "assignUsersAsync": ("assign_users_async", lambda d: {
        "id": _required(d, "taskId"),
        "input": _required(d, "request"),
    }),
    "unassignUsersAsync": ("unassign_users_async", lambda d: {
        "id": _required(d, "taskId"),
        "input": _required(d, "request"),
    }),
    "deleteTaskAsync": ("delete_task_async", lambda d: {
        "id": _required(d, "taskId"),
    }),
}


# Entra ID (App + Delegated). Tenant directory writes are never admitted. The
# membership checks are POSTs with read semantics.
_AZUREAD_OPERATIONS = {
    "getUserAsync": ("get_user_async", lambda d: {
        "id": _required(d, "userId"),
    }),
    "getGroupAsync": ("get_group_async", lambda d: {
        "id": _required(d, "groupId"),
    }),
    "getGroupMembersAsync": ("get_group_members_async", lambda d: {
        "id": _required(d, "groupId"),
        "top": _query(d, "top"),
    }),
    "checkMemberGroupsAsync": ("check_member_groups_async", lambda d: {
        "id": _required(d, "userId"),
        "input": {"groupIds": _required(d, "groupIds")},
    }),
    "getMemberGroupsAsync": ("get_member_groups_async", lambda d: {
        "id": _required(d, "userId"),
        "input": {"securityEnabledOnly": _bool(d.get("securityEnabledOnly"))},
    }),
}


@udf.streaming_function()
async def rayfin_sharepointonline_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("sharepointonline", _SHAREPOINTONLINE_OPERATIONS, payload)


@udf.streaming_function()
async def rayfin_office365_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("office365", {
        **_OFFICE365_OPERATIONS, **_OFFICE365_OWNER_WIDE_OPERATIONS,
        **_OFFICE365_OWNER_WRITE_OPERATIONS}, payload)


@udf.streaming_function()
async def rayfin_excelonlinebusiness_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("excelonlinebusiness", _EXCELONLINEBUSINESS_OPERATIONS, payload)


@udf.streaming_function()
async def rayfin_teams_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("teams", {
        **_TEAMS_OPERATIONS, **_TEAMS_OWNER_WIDE_OPERATIONS,
        **_TEAMS_OWNER_WRITE_OPERATIONS}, payload)


@udf.streaming_function()
async def rayfin_onedriveforbusiness_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("onedriveforbusiness", {
        **_ONEDRIVEFORBUSINESS_OPERATIONS, **_ONEDRIVEFORBUSINESS_OWNER_WIDE_OPERATIONS,
        **_ONEDRIVEFORBUSINESS_OWNER_WRITE_OPERATIONS}, payload)


@udf.streaming_function()
async def rayfin_office365groups_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("office365groups", _OFFICE365GROUPS_OPERATIONS, payload)


@udf.streaming_function()
async def rayfin_microsoftforms_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("microsoftforms", _MICROSOFTFORMS_OPERATIONS, payload)


@udf.streaming_function()
async def rayfin_planner_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("planner", _PLANNER_OPERATIONS, payload)


@udf.streaming_function()
async def rayfin_azuread_v1(payload: dict) -> fn.StreamResponse:
    return await _invoke_acn("azuread", _AZUREAD_OPERATIONS, payload)
