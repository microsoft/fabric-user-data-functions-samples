import fabric.functions as fn
import aiohttp
import asyncio
import json
import logging
import os
from typing import Optional
from urllib.parse import urlparse

udf = fn.UserDataFunctions()

_POWERBI_BASE = os.environ.get("POWERBI_API_BASE", "https://powerbiapi.analysis-df.windows.net/v1.0/myorg")
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
