import fabric.functions as fn
import aiohttp
import asyncio
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
