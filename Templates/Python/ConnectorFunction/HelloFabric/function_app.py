import fabric.functions as fn
import aiohttp
import asyncio
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
_LOG_ANALYTICS_BASE = "https://api.loganalytics.azure.com/v1/workspaces"
_LOG_ANALYTICS_RESOURCE = "https://api.loganalytics.io"
_MAXIMUM_TELEMETRY_QUERY_LENGTH = 16 * 1024
_MAXIMUM_TELEMETRY_TIMESPAN_SECONDS = 30 * 24 * 60 * 60
_MAXIMUM_TELEMETRY_ROWS = 10000
_TELEMETRY_TIMESPAN_PATTERN = re.compile(
    r"^P(?!$)(?:(\d+)D)?(?:T(?!$)(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$"
)
_WORKSPACE_ID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
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

    workspace_id = input_data.get("workspaceId")
    if (
        not isinstance(workspace_id, str)
        or not _WORKSPACE_ID_PATTERN.fullmatch(workspace_id)
    ):
        raise ValueError("workspaceId must be a valid GUID")

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
    url = f"{_LOG_ANALYTICS_BASE}/{workspace_id}/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": _JSON_MEDIA_TYPE,
        "Accept": _JSON_MEDIA_TYPE,
    }
    bounded_query = f"{query.rstrip()}\n| take {maximum_rows}"
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
