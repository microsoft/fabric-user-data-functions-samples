import fabric.functions as fn
import aiohttp
import asyncio
import logging
import os
from typing import Optional
from urllib.parse import urlparse

udf = fn.UserDataFunctions()

_POWERBI_BASE = os.environ.get("POWERBI_API_BASE", "https://powerbiapi.analysis-df.windows.net/v1.0/myorg")
_ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"

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


# ---------------------------------------------------------------------------
# rayfin_appinsights_v1 - Application Insights / Log Analytics query adapter
# ---------------------------------------------------------------------------
# Unlike rayfin_semantic_model_v1 (which forwards the caller's *delegated* token
# to Power BI), this operation reads telemetry using the connector function's own
# *system-assigned managed identity*. The BaaS workload grants that identity a
# Log Analytics read role (e.g. "Log Analytics Reader" / "Monitoring Reader") on
# the target App Insights resource at provisioning time. No caller token is
# accepted or forwarded here: the browser never holds a query credential, which
# is the whole point of the platform-owned connector in the RFC. Emit stays
# direct from the client; only *reads* are brokered through this MSI.

# Cached across warm invocations. DefaultAzureCredential probes IMDS on the first
# token acquisition; we never close the client (closing disposes the MI token
# cache and forces a fresh probe on the next call).
_logs_client = None            # azure.monitor.query.aio.LogsQueryClient
_logs_client_lock = asyncio.Lock()


async def _get_logs_client():
    global _logs_client
    if _logs_client is not None:
        return _logs_client
    async with _logs_client_lock:
        if _logs_client is None:
            # Deferred imports: the portal-generated requirements.txt is not
            # guaranteed to carry these wheels, and a module-scope import would
            # take down the whole function app on cold start if they're absent.
            from azure.identity.aio import DefaultAzureCredential
            from azure.monitor.query.aio import LogsQueryClient
            _logs_client = LogsQueryClient(DefaultAzureCredential())
    return _logs_client


def _parse_timespan(timespan: Optional[str]):
    """Map the RFC ``timespan`` input to what azure-monitor-query expects.

    Accepts either ``None`` (the query supplies its own time filter) or an
    ISO-8601 *duration* such as ``PT1H``, ``PT30M`` or ``P1D``, which the SDK
    interprets as ``now - duration .. now``. Absolute start/end ranges are
    intentionally out of scope for the POC.
    """
    if not timespan:
        return None
    import re
    from datetime import timedelta

    m = re.fullmatch(
        r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?", timespan
    )
    if not m or timespan in ("P", "PT"):
        raise ValueError(
            "timespan must be an ISO-8601 duration like PT1H, PT30M or P1D"
        )
    days, hours, minutes, seconds = (int(g) if g else 0 for g in m.groups())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


@udf.streaming_function()
async def rayfin_appinsights_v1(payload: dict) -> fn.StreamResponse:
    import json

    from azure.core.exceptions import HttpResponseError
    from azure.monitor.query import LogsQueryStatus

    operation = payload.get("operation") or "query"
    input_data = payload.get("input", {}) or {}

    if operation != "query":
        raise ValueError(f"unsupported operation '{operation}'; expected 'query'")

    kql = input_data.get("kql") or input_data.get("query")
    if not kql:
        raise ValueError("input.kql is required")

    # Log Analytics workspace id (the customer/workspace GUID the Azure Monitor
    # query API expects - NOT a Fabric/Power BI workspace id). This is a
    # *server-owned* field: BaaS strips any caller-supplied value and injects the
    # workspace bound to the app's App Insights resource (one per app, from the
    # TIPS pool). The adapter must never accept a caller-controlled workspace id -
    # that would let a caller redirect the query at any workspace this connector's
    # MSI can read. Reuses the connector's existing `workspaceId` target, so no
    # BaaS invoke-body change is needed (just point the connector config at the
    # Log Analytics workspace GUID).
    workspace_id = input_data.get("workspaceId")
    if not workspace_id:
        raise ValueError("input.workspaceId is required (injected by BaaS)")

    max_rows = input_data.get("maxRows")
    timespan = _parse_timespan(input_data.get("timespan"))

    client = await _get_logs_client()

    try:
        result = await client.query_workspace(
            workspace_id, query=kql, timespan=timespan
        )
    except HttpResponseError as exc:
        # Leak-safe: log operation + status only. Never str(exc) - the exception
        # text can echo the KQL and resource identifiers back to the caller.
        status = getattr(exc, "status_code", None)
        logging.error("rayfin_appinsights_v1 query failed status=%s", status)
        body = json.dumps(
            {"error": {"code": "QueryFailed", "message": "Application Insights query failed"}}
        )
        return fn.StreamResponse(
            iter([body.encode("utf-8")]),
            media_type="application/json",
            status_code=status or 502,
        )

    if result.status == LogsQueryStatus.PARTIAL:
        logging.warning("rayfin_appinsights_v1 partial result")
        tables = result.partial_data or []
    else:
        tables = result.tables or []

    rows_out = []
    truncated = False
    for table in tables:
        cols = list(table.columns)
        for row in table.rows:
            rows_out.append({col: val for col, val in zip(cols, row)})
            if max_rows and len(rows_out) >= max_rows:
                truncated = True
                break
        if truncated:
            break

    # default=str so datetime / timedelta / Decimal values from Kusto serialize.
    body = json.dumps(
        {"rows": rows_out, "count": len(rows_out), "truncated": truncated},
        default=str,
    )
    return fn.StreamResponse(iter([body.encode("utf-8")]), media_type="application/json")
