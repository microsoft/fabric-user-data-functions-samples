import fabric.functions as fn
import aiohttp
import asyncio
import base64
import binascii
import inspect
import json
import logging
import os
import re
import time
import uuid
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


@udf.generic_connection(argName="kustoClient", audienceType="Kusto")
@udf.streaming_function()
async def rayfin_kusto_v1(payload: dict, kustoClient: fn.FabricItem) -> fn.StreamResponse:
    # The SDK carries the operation name alongside the input. `executeQuery` runs a
    # KQL query against /v1/rest/query; `executeCommand` runs a Kusto management
    # (control) command — text starting with a leading dot, e.g. `.show databases` —
    # against /v1/rest/mgmt. Both share the same cluster, database context, token and
    # v1 {Tables} response shape; only the caller's input field and the REST verb
    # differ. Default to executeQuery so callers that omit the operation still work.
    operation = (payload.get("operation") or "executeQuery").strip()
    input_data = payload.get("input", {})

    # queryServiceUri + databaseName are resolved at `rayfin connector add` time by
    # the Rayfin CLI and flow in via connector config. executeQuery callers supply
    # `query`; executeCommand callers supply `command`.
    query_service_uri = input_data.get("queryServiceUri")
    database_name = input_data.get("databaseName")
    is_command = operation == "executeCommand"
    # Prefer the field that matches the operation; fall back to the other so a caller
    # that set only one of query/command still works.
    if is_command:
        csl = input_data.get("command") or input_data.get("query")
    else:
        csl = input_data.get("query") or input_data.get("command")

    if not query_service_uri or not database_name or not csl:
        raise ValueError("queryServiceUri, databaseName and query/command are required")

    client_request_id = (
        input_data.get("clientRequestId")
        or f"KPC.rayfin_kusto_v1;{uuid.uuid4()}"
    )

    # BaaS no longer forwards a raw accesstoken. FuncSet resolves the Kusto generic
    # connection (audienceType="Kusto") and injects a FabricItem whose
    # get_access_token() returns a token-credential object; get_token().token is the
    # pre-minted Kusto-audience bearer string.
    access_token = kustoClient.get_access_token().get_token().token

    # executeCommand -> /v1/rest/mgmt ; executeQuery -> /v1/rest/query. Kusto keeps
    # the endpoints apart at the protocol level; the caller's role decides authZ.
    rest_verb = "mgmt" if is_command else "query"
    url = f"{query_service_uri.rstrip('/')}/v1/rest/{rest_verb}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "x-ms-client-request-id": client_request_id,
    }
    body = {"db": database_name, "csl": csl, "properties": {}}

    session = await _get_session()

    resp = await session.post(url, json=body, headers=headers)

    if resp.status != 200:
        # Surface upstream status + body; the Fabric app backend sanitizes non-2xx.
        detail = await resp.text()
        await resp.release()
        return fn.StreamResponse(
            iter([detail.encode("utf-8")]),
            media_type=resp.headers.get("Content-Type", _JSON_MEDIA_TYPE),
            status_code=resp.status,
        )

    # True streaming: relay the Kusto v1 response body chunk-by-chunk without
    # buffering, parsing, or re-serializing it — a pure byte pump, mirroring
    # rayfin_semantic_model_v1. The v1 {Tables} document is transformed to the
    # Rayfin connector output shape client-side in the SDK
    # (packages/typescript-sdk/connector-kusto/src), so the UDF keeps constant
    # memory and TTFB stays ~= Kusto's TTFB. The `x-ms-client-request-id` header
    # was set from the SDK-supplied clientRequestId above, so the SDK can
    # correlate without reading the body; `x-ms-activity-id` is not relayed
    # (accepted loss, same as the semantic-model path).
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
    # Unlike the other connectors, office365users also honours an optional
    # caller-supplied accessToken (used as-is) in place of managed identity.
    return await _invoke_acn(
        "office365users", _OFFICE365USERS_OPERATIONS, payload, allowAccessToken=True
    )


def _query(input_data: dict, name: str):
    # The SDK types every query parameter as Optional[str] and sends it verbatim.
    value = input_data.get(name)
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


# Errors go back as HTTP 200 with a Rayfin ConnectorErrorResult body
# ({"status": "error", "error": {...}}). The UDF host replaces any non-2xx
# response with a generic failure and drops the body, so a non-2xx status
# would hide the reason from the app. Callers check isConnectorError().
_UDF_SUCCEEDED_HEADERS = {"x-fabric-udf-status": "Succeeded"}
_MAX_ERROR_MESSAGE_LENGTH = 1000


def _error_response(message: str, code: str, category: str,
                    details: Optional[str] = None) -> fn.StreamResponse:
    error = {"message": message, "code": code, "category": category}
    if details:
        error["details"] = details
    body = json.dumps({"status": "error", "error": error})
    return fn.StreamResponse(
        iter([body.encode("utf-8")]),
        media_type=_JSON_MEDIA_TYPE,
        status_code=200,
        headers=_UDF_SUCCEEDED_HEADERS,
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


async def _invoke_acn(
    slug: str, operations: dict, payload: dict, allowAccessToken: bool = False
) -> fn.StreamResponse:
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

    # A caller-supplied token gets a fresh, uncached client that is closed after
    # the call; otherwise the cached managed-identity client is reused.
    accessToken = input_data.get("accessToken") if allowAccessToken else None
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
_OFFICE365_OWNER_WIDE_OPERATIONS = {
    "getEmailsAsync": ("get_emails_async", lambda d: {
        "folder_path": d.get("folderPath"),
        "from_": d.get("from"),
        "to": d.get("to"),
        "subject_filter": d.get("subjectFilter"),
        "fetch_only_unread": _query(d, "fetchOnlyUnread"),
        "search_query": d.get("searchQuery"),
        "top": _query(d, "top"),
        "include_attachments": "false",
    }),
    "getEmailAsync": ("get_email_async", lambda d: {
        "message_id": _required(d, "messageId"),
        "include_attachments": "false",
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
