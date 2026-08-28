import fabric.functions as fn
import aiohttp
import asyncio
import os
import uuid
from typing import NamedTuple, NoReturn, Optional, Tuple
from urllib.parse import urlparse

udf = fn.UserDataFunctions()

_POWERBI_BASE = os.environ.get("POWERBI_API_BASE", "https://dailyapi.powerbi.com/v1.0/myorg")
_ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"
_JSON_MEDIA_TYPE = "application/json"

_MCP_T1_FIELD_ORDER = (
    "transport",
    "version",
    "method",
    "protocolVersion",
    "headers",
    "body",
    "serverPolicy",
)
_MCP_T1_FIXED_VALUES = (
    "mcp-streamable-http",
    1,
    "POST",
    "2025-11-25",
)
_MCP_DENIED_HEADER_NAMES = frozenset(
    {
        "authorization",
        "baggage",
        "connection",
        "content-length",
        "cookie",
        "forwarded",
        "host",
        "keep-alive",
        "mcp-session-id",
        "origin",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "traceparent",
        "tracestate",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
_MCP_DENIED_HEADER_PREFIXES = ("x-forwarded-", "x-ms-", "x-rayfin-")
_MCP_ASCII_TOKEN_CHARACTERS = frozenset(
    "!#$%&'*+-.^_`|~0123456789"
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)
_MCP_T1_MAX_BODY_SIZE_BYTES = 5 * 1024 * 1024
_MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE = "Invalid MCP transport envelope."
_MCP_SERVER_POLICY_UNRESOLVED_MESSAGE = "MCP server policy is unresolved."


class _McpTransportContractError(ValueError):
    """Fixed, customer-data-free error for an invalid MCP T1 envelope."""


class _McpServerPolicyUnresolved(_McpTransportContractError):
    """Fixed error used while no trusted MCP server policy is defined."""


class _PendingMcpT1Transport(NamedTuple):
    transport: str
    version: int
    method: str
    protocol_version: str
    headers: Tuple[Tuple[str, Tuple[str, ...]], ...]
    body_bytes: bytes


def _is_ascii_http_token(value: object) -> bool:
    if type(value) is not str or not value:
        return False
    return all(character in _MCP_ASCII_TOKEN_CHARACTERS for character in value)


def _is_mcp_denied_header_name(name: object) -> bool:
    if type(name) is not str:
        return False
    lower_name = name.lower()
    return lower_name in _MCP_DENIED_HEADER_NAMES or lower_name.startswith(
        _MCP_DENIED_HEADER_PREFIXES
    )


def _mcp_connection_nominations(
    headers: Tuple[Tuple[str, Tuple[str, ...]], ...],
) -> Tuple[str, ...]:
    if type(headers) is not tuple:
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None

    nominations = []
    for header in headers:
        if type(header) is not tuple or len(header) != 2:
            raise _McpTransportContractError(
                _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
            ) from None
        name, values = header
        if type(name) is not str or type(values) is not tuple:
            raise _McpTransportContractError(
                _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
            ) from None
        if any(type(value) is not str for value in values):
            raise _McpTransportContractError(
                _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
            ) from None
        if name.lower() != "connection":
            continue
        for value in values:
            for part in value.split(","):
                token = part.strip(" \t")
                if not _is_ascii_http_token(token):
                    raise _McpTransportContractError(
                        _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
                    ) from None
                nominations.append(token.lower())
    return tuple(nominations)


def _has_mcp_connection_nominated_header(
    headers: Tuple[Tuple[str, Tuple[str, ...]], ...],
) -> bool:
    nominations = frozenset(_mcp_connection_nominations(headers))
    return any(
        name.lower() != "connection" and name.lower() in nominations
        for name, _values in headers
    )


def _extract_pending_mcp_t1_transport(
    envelope: object,
) -> _PendingMcpT1Transport:
    if type(envelope) is not dict:
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None

    envelope_items = tuple(envelope.items())
    if len(envelope_items) != len(_MCP_T1_FIELD_ORDER):
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None
    envelope_keys = tuple(key for key, _value in envelope_items)
    if any(type(key) is not str for key in envelope_keys):
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None
    if envelope_keys != _MCP_T1_FIELD_ORDER:
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None

    (
        transport,
        version,
        method,
        protocol_version,
        headers,
        body,
        server_policy,
    ) = tuple(value for _key, value in envelope_items)
    if server_policy is not None:
        raise _McpServerPolicyUnresolved(
            _MCP_SERVER_POLICY_UNRESOLVED_MESSAGE
        ) from None
    if (
        type(transport) is not str
        or type(version) is not int
        or type(method) is not str
        or type(protocol_version) is not str
        or type(headers) is not dict
        or type(body) is not str
    ):
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None
    if (
        transport,
        version,
        method,
        protocol_version,
    ) != _MCP_T1_FIXED_VALUES:
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None

    frozen_headers = []
    for name, values in tuple(headers.items()):
        if type(name) is not str or type(values) is not list:
            raise _McpTransportContractError(
                _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
            ) from None
        frozen_values = tuple(values)
        if any(type(value) is not str for value in frozen_values):
            raise _McpTransportContractError(
                _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
            ) from None
        frozen_headers.append((name, frozen_values))

    immutable_headers = tuple(frozen_headers)
    header_names = tuple(name for name, _values in immutable_headers)
    if any(
        not _is_ascii_http_token(name) or name != name.lower()
        for name in header_names
    ):
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None

    for index in range(1, len(header_names)):
        if header_names[index - 1] > header_names[index]:
            raise _McpTransportContractError(
                _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
            ) from None

    if _has_mcp_connection_nominated_header(immutable_headers):
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None
    if any(_is_mcp_denied_header_name(name) for name in header_names):
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None
    for name, values in immutable_headers:
        if name == "mcp-protocol-version" and values != ("2025-11-25",):
            raise _McpTransportContractError(
                _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
            ) from None

    if len(body) > _MCP_T1_MAX_BODY_SIZE_BYTES:
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None
    try:
        body_bytes = body.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None
    if len(body_bytes) > _MCP_T1_MAX_BODY_SIZE_BYTES:
        raise _McpTransportContractError(
            _MCP_TRANSPORT_CONTRACT_ERROR_MESSAGE
        ) from None

    return _PendingMcpT1Transport(
        transport,
        version,
        method,
        protocol_version,
        immutable_headers,
        body_bytes,
    )


def _prepare_mcp_transport(envelope: object) -> NoReturn:
    _extract_pending_mcp_t1_transport(envelope)
    raise _McpServerPolicyUnresolved(
        _MCP_SERVER_POLICY_UNRESOLVED_MESSAGE
    ) from None


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


class _ResponseBodyIterator:
    """Own an acquired response while relaying its body without buffering."""

    def __init__(self, response: aiohttp.ClientResponse):
        self._response = None
        self._iterator = None
        self._released = True
        self._response = response
        self._released = False
        self._iterator = response.content.iter_any().__aiter__()

    def _release_response(self):
        response = self._response
        if response is None:
            return
        self._response = None
        self._released = True
        response.release()

    def __del__(self):
        self._release_response()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._released:
            raise StopAsyncIteration
        try:
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            await self.aclose()
            raise
        except BaseException as read_error:
            try:
                await self.aclose()
            except BaseException as close_error:
                raise read_error from close_error
            raise

    async def aclose(self):
        if self._released:
            return
        self._release_response()
        close_iterator = getattr(self._iterator, "aclose", None)
        if close_iterator is not None:
            await close_iterator()


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
        resp.release()               # release the RESPONSE, never the shared session
        return fn.StreamResponse(
            iter([detail.encode("utf-8")]),
            media_type=resp.headers.get("Content-Type", "application/json"),
            status_code=resp.status,
        )

    return fn.StreamResponse(
        _ResponseBodyIterator(resp),
        media_type=_ARROW_MEDIA_TYPE,
    )


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
        resp.release()
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
    return fn.StreamResponse(
        _ResponseBodyIterator(resp),
        media_type=_JSON_MEDIA_TYPE,
    )
