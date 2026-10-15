import fabric.functions as fn
import aiohttp
import asyncio
import inspect
import json
import logging
import os
import re
import uuid
from typing import Optional
from urllib.parse import urlparse

udf = fn.UserDataFunctions()

_POWERBI_BASE = os.environ.get("POWERBI_API_BASE", "https://dailyapi.powerbi.com/v1.0/myorg")
_ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"
_JSON_MEDIA_TYPE = "application/json"

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
# Azure Connector Namespace (ACN) -- office365users
# ---------------------------------------------------------------------------
# No bearer token is passed in: the connection's access policy is bound to this
# function's managed identity, so the `azure-connectors` SDK mints its own. In
# exchange for its retries and typed requests it buffers rather than streams.
_clients: dict = {}
_clients_lock = asyncio.Lock()


async def _get_office365users_client(connectionRuntimeUrl: str):
    """Return a cached Office365usersClient for a given connection runtime URL.

    Deliberately never closed: the client owns an aiohttp pool and a credential,
    and rebuilding both per invocation would cost a handshake and an IMDS probe.
    """
    # Deferred so a missing wheel fails this function alone, not every function
    # in the app at import time (requirements.txt is generated by Fabric).
    from azure.connectors.office365users import Office365usersClient

    client = _clients.get(connectionRuntimeUrl)
    if client is not None:
        return client

    async with _clients_lock:
        client = _clients.get(connectionRuntimeUrl)
        if client is None:
            # No credential argument -> the SDK's ManagedIdentityTokenProvider.
            client = Office365usersClient(connectionRuntimeUrl)
            _clients[connectionRuntimeUrl] = client
    return client


def _required(input_data: dict, name: str):
    value = input_data.get(name)
    if not value:
        raise ValueError(f"'{name}' is required for this operation")
    return value


# operation -> (SDK coroutine name, kwargs builder). Flat names match rayfin.yml and
# the generated TypeScript SDK 1:1; kwargs differ per operation, so no generic splat.
# Excluded: userPhoto (raw bytes), the profile writes, httpRequest (untyped).
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
        "top": d.get("top"),
    }),
    "relevantPeopleAsync": ("relevant_people_async", lambda d: {
        "user_id": _required(d, "userId"),
    }),
    "myProfileAsync": ("my_profile_async", lambda d: {
        "select": d.get("select"),
    }),
    "searchUserAsync": ("search_user_async", lambda d: {
        "search_term": d.get("searchTerm"),
        "top": d.get("top"),
        "is_search_term_required": d.get("isSearchTermRequired"),
        "skip_token": d.get("skipToken"),
    }),
}


@udf.streaming_function()
async def rayfin_office365users_v1(payload: dict) -> fn.StreamResponse:
    # See _get_office365users_client for why this import is deferred.
    from azure.connectors import ConnectorException

    operation = (payload.get("operation") or "").strip()
    input_data = payload.get("input", {})

    # BaaS strips this key from the caller's input and re-injects the bound
    # connection's URL, so it is never app-named. This adapter derives no endpoint.
    connectionRuntimeUrl = input_data.get("connectionRuntimeUrl")

    # Not the security boundary -- BaaS enforces the rayfin.yml allow-list. These
    # just fail malformed input clearly instead of with an AttributeError.
    if not operation:
        raise ValueError("'operation' is required")
    if not connectionRuntimeUrl:
        raise ValueError("input.connectionRuntimeUrl is required")

    handler = _OFFICE365USERS_OPERATIONS.get(operation)
    if handler is None:
        supported = ", ".join(sorted(_OFFICE365USERS_OPERATIONS))
        raise ValueError(
            f"unsupported operation '{operation}'; expected one of: {supported}"
        )
    methodName, buildKwargs = handler
    kwargs = buildKwargs(input_data)

    client = await _get_office365users_client(connectionRuntimeUrl)

    try:
        result = await getattr(client, methodName)(**kwargs)
    except ConnectorException as exc:
        # Never log str(exc), exc.path or exc.operation: all three embed the
        # connection id, which is a bearer capability. The operation name is safe.
        logging.warning(
            "rayfin_office365users_v1: operation '%s' failed with status %s",
            operation,
            exc.status_code,
        )
        detail = exc.response_body or ""
        return fn.StreamResponse(
            iter([detail.encode("utf-8")]),
            media_type=_JSON_MEDIA_TYPE,
            status_code=exc.status_code or 502,
        )

    # The SDK returns None on an empty 2xx (e.g. no manager); emit JSON `null` so
    # the client can parse unconditionally. Upstream document passes through as-is.
    document = json.dumps(result) if result is not None else "null"

    # Single chunk, but StreamResponse keeps one adapter contract across connectors.
    return fn.StreamResponse(
        iter([document.encode("utf-8")]),
        media_type=_JSON_MEDIA_TYPE,
        status_code=200,
    )
