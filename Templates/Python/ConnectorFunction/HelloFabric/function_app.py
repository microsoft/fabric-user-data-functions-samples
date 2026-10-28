import fabric.functions as fn
import aiohttp
import asyncio
import inspect
import json
import os
import logging
import re
import uuid
import time
from typing import Optional
from urllib.parse import urlparse

udf = fn.UserDataFunctions()

_POWERBI_BASE = os.environ.get("POWERBI_API_BASE", "https://api.powerbi.com/v1.0/myorg")
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
_FABRIC_ORIGINS = {
    "https://powerbiapi.analysis-df.windows.net": "https://powerbiapi.analysis-df.windows.net",
    "https://dailyapi.powerbi.com": "https://dailyapi.fabric.microsoft.com",
    "https://dxtapi.powerbi.com": "https://dxtapi.fabric.microsoft.com",
    # MSIT and production share this Power BI base; it cannot distinguish them.
    "https://api.powerbi.com": "https://api.fabric.microsoft.com",
}

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
    """Query Application Insights using the host's managed identity.

    Payload: {"operation": "query", "input": {"kql": "AppTraces",
    "resourceId": "<Application Insights ARM resource ID>", "timespan": "P1D",
    "maxRows": 1000}}. Timespan is optional (positive, at most 30 days);
    maxRows defaults to 10,000 and must be 1-10,000. KQL is at most 8,192
    characters. The host must provide IDENTITY_ENDPOINT and IDENTITY_HEADER
    for an identity authorized to query the resource. Successful responses
    stream upstream JSON; upstream errors retain their status and body.
    """
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


def _mcp_header(value, limit):
    return isinstance(value, str) and 0 < len(value) <= limit and all(0x21 <= ord(c) <= 0x7E for c in value)


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
    body = {
        "db": database_name,
        "csl": csl,
        "properties": json.dumps({
            "Options": {
                "request_readonly": True,
                "request_readonly_hardline": True,
            },
        }),
    }

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
