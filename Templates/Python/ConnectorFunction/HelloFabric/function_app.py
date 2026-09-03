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
# The connector above is handed a bearer token by the platform. This one is not.
# An ACN connection is reached through a connection runtime URL whose access
# policy is bound to *this* function's system-assigned managed identity, so the
# token is minted in-process by the typed `azure-connectors` SDK
# (DefaultAzureCredential -> https://apihub.azure.com/.default). Nothing has to
# be added to the platform's AudienceType enum for this to work.
#
# Trade-off: the SDK buffers and json.loads() the upstream response instead of
# handing back a byte stream, so this is not a pure pump like the connector
# above. That is fine here -- a manager profile is a single small object, not a
# result set -- and in exchange we get the SDK's retry/backoff and its typed
# request construction rather than hand-rolled URL formatting.
_clients: dict = {}
_clients_lock = asyncio.Lock()


async def _get_office365users_client(connectionRuntimeUrl: str):
    """Return a cached Office365usersClient for a given connection runtime URL.

    The client owns both an aiohttp session and a DefaultAzureCredential, and
    its close()/__aexit__ tears down both. So we deliberately never use
    `async with` and never close it: a per-invocation client would rebuild a TCP
    pool and re-probe the managed-identity endpoint on every call. Cached per
    runtime URL for the life of the worker, mirroring _get_session() above.
    """
    # Deferred import. requirements.txt in this template is generated by Fabric
    # Library Management, so we cannot guarantee `azure-connectors` is present
    # on every deployment. At module scope a missing wheel would raise at import
    # time and take down every function in this app; here it only fails this
    # one, with a clear error, leaving the other connectors serving.
    from azure.connectors.office365users import Office365usersClient

    client = _clients.get(connectionRuntimeUrl)
    if client is not None:
        return client

    async with _clients_lock:
        client = _clients.get(connectionRuntimeUrl)
        if client is None:
            # No credential argument: the client defaults to
            # ManagedIdentityTokenProvider, which wraps DefaultAzureCredential.
            client = Office365usersClient(connectionRuntimeUrl)
            _clients[connectionRuntimeUrl] = client
    return client


def _required(input_data: dict, name: str):
    value = input_data.get(name)
    if not value:
        raise ValueError(f"'{name}' is required for this resource")
    return value


# operation -> (SDK coroutine name, kwargs builder).
#
# Operation names are flat, matching what a builder writes in rayfin.yml
# (`operations: - name: managerAsync`). The same string travels the whole way
# down: builder config -> BaaS allow-list check -> this table. Nothing has to
# split or re-join it, which matters because these names do not decompose into
# a clean verb + noun pair -- `searchUserAsync` is not a "get", and
# `myProfileAsync` and `relevantPeopleAsync` have no standalone noun.
# `rayfin_kusto_v1` sets the same precedent with its flat `executeQuery`.
#
# The names mirror the generated Azure Connector Namespace TypeScript SDK
# method names 1:1, so an app calling `connectors.o365.managerAsync(...)` puts
# exactly that string on the wire and it lands here unmodified.
#
# The builders are not interchangeable and a generic **input_data splat would be
# wrong: manager/userProfile/directReports take `id`, relevantPeople
# takes `user_id`, and myProfile/searchUser take no user identifier at all.
# Encoding each shape here keeps that mapping in one readable place and makes
# the required-vs-optional argument per operation explicit.
#
# Every entry answers with a JSON document, which is what lets them share one
# response path below. Deliberately excluded: userPhoto (returns raw bytes, so
# json.dumps would raise), the two profile writes (this connector is read-only
# for now), and httpRequest (an untyped passthrough, which would hand any caller
# the connector's entire surface and defeat the point of an allow-list).
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

# Transitional aliases: the un-suffixed names this adapter shipped with, kept
# so already-captured request bodies keep working after the rename to the ACN
# `*Async` vocabulary. Remove once every caller has moved over.
_OFFICE365USERS_OPERATIONS.update({
    "getManager": _OFFICE365USERS_OPERATIONS["managerAsync"],
    "getUserProfile": _OFFICE365USERS_OPERATIONS["userProfileAsync"],
    "getDirectReports": _OFFICE365USERS_OPERATIONS["directReportsAsync"],
    "getRelevantPeople": _OFFICE365USERS_OPERATIONS["relevantPeopleAsync"],
    "getMyProfile": _OFFICE365USERS_OPERATIONS["myProfileAsync"],
    "searchUser": _OFFICE365USERS_OPERATIONS["searchUserAsync"],
})


@udf.streaming_function()
async def rayfin_office365users_v1(payload: dict) -> fn.StreamResponse:
    # See _get_office365users_client for why this import is deferred.
    from azure.connectors import ConnectorException

    # One flat operation name, exactly as declared in rayfin.yml. No verb +
    # resource split: see the table above for why these names do not decompose.
    operation = (payload.get("operation") or "").strip()
    input_data = payload.get("input", {})

    # The runtime URL is resolved server-side by BaaS from the connector config
    # and lives on `connection`, outside `input`, so that BaaS-injected data is
    # structurally separate from app-supplied data. That separation is
    # load-bearing: an ACN connection is a bearer capability, not a scoped
    # permission, so a caller who could name the connection would reach every
    # operation on it. This adapter never derives an endpoint itself.
    # `input.connectionRuntimeUrl` is accepted as a transitional fallback.
    connection = payload.get("connection") or {}
    connectionRuntimeUrl = (
        connection.get("runtimeUrl") or input_data.get("connectionRuntimeUrl")
    )

    # Narrowing a connection down to the operations an app declared is the app
    # backend's job -- BaaS checks the rayfin.yml allow-list before the call
    # reaches this function. The check below is not that boundary. It exists so
    # a malformed request fails here with a clear message instead of an
    # AttributeError, and so this adapter only ever reaches SDK methods whose
    # response shape it can actually handle.
    if not operation:
        raise ValueError("'operation' is required")
    if not connectionRuntimeUrl:
        raise ValueError("connection.runtimeUrl is required")

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
        # Relay the upstream error body verbatim so the caller sees the real
        # Graph error. Never relay -- or log -- str(exc), exc.path or
        # exc.operation: all three embed the full request path, which contains
        # the ACN connection id, and that connection is a bearer capability.
        # Log the operation name instead: it is already constrained to the
        # table above, so it says which call failed without leaking the target.
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

    # The SDK returns None when upstream answered 2xx with an empty body, e.g.
    # the user has no manager. Emit a JSON `null` rather than a zero-length body
    # so the client SDK can JSON.parse the response unconditionally.
    # Pass the upstream document straight through -- no envelope. Shaping the
    # result is the client SDK's job, same as for the connector above.
    document = json.dumps(result) if result is not None else "null"

    # Already buffered by the SDK above, so this is a single chunk. We still
    # return a StreamResponse to keep one uniform adapter contract across every
    # connector in this app.
    return fn.StreamResponse(
        iter([document.encode("utf-8")]),
        media_type=_JSON_MEDIA_TYPE,
        status_code=200,
    )
