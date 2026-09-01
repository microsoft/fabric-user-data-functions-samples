import fabric.functions as fn
import aiohttp
import asyncio
import json
import os
import uuid
from typing import NamedTuple, Optional, Tuple
from urllib.parse import urlparse

udf = fn.UserDataFunctions()

_POWERBI_BASE = os.environ.get("POWERBI_API_BASE", "https://dailyapi.powerbi.com/v1.0/myorg")
_ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"
_JSON_MEDIA_TYPE = "application/json"

_MCP_INPUT_FIELD_ORDER = ("version", "protocolVersion", "message")
_MCP_OUTPUT_FIELD_ORDER = ("version", "message")
_MCP_PROTOCOL_VERSION = "2026-07-28"
_MCP_ALLOWED_METHODS = frozenset(
    ("server/discover", "tools/list", "tools/call", "tasks/get", "tasks/cancel")
)
_MCP_ENDPOINT = "https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq"
_MCP_VARIANTS = "Fabric.Routing.M365.V2,Fabric.DisableMsitRedirect"
_MCP_REQUEST_LIMIT_BYTES = 5 * 1024 * 1024
_MCP_INVOKE_TIMEOUT_SECONDS = 5 * 60
_MCP_CONTRACT_ERROR_MESSAGE = "Invalid managed MCP request."
_MCP_CONFIGURATION_ERROR_MESSAGE = "Managed MCP limits are not configured."
_MCP_UPSTREAM_ERROR_MESSAGE = "Managed MCP upstream response is invalid."


class _McpContractError(ValueError):
    pass


class _McpConfigurationError(RuntimeError):
    pass


class _McpUpstreamError(RuntimeError):
    pass


class _McpManagedLimits(NamedTuple):
    max_output_bytes: int
    max_json_depth: int
    allowed_status_codes: Tuple[int, ...]
    final_task_statuses: Tuple[str, ...]
    max_poll_count: int


class _McpManagedRequest(NamedTuple):
    method: str
    routing_name: Optional[str]
    body_bytes: bytes


def _raise_mcp_contract_error():
    raise _McpContractError(_MCP_CONTRACT_ERROR_MESSAGE) from None


def _contains_session_id(value: object) -> bool:
    pending = [value]
    while pending:
        current = pending.pop()
        if type(current) is dict:
            for key, nested in tuple(current.items()):
                if type(key) is not str or key.lower() == "sessionid":
                    return True
                pending.append(nested)
        elif type(current) is list:
            pending.extend(current)
    return False


def _valid_routing_name(value: object) -> bool:
    return (
        type(value) is str
        and bool(value)
        and not any(character in "\r\n" or ord(character) < 0x20 for character in value)
    )


def _request_meta() -> dict:
    return {
        "io.modelcontextprotocol/protocolVersion": _MCP_PROTOCOL_VERSION,
        "io.modelcontextprotocol/clientInfo": {
            "name": "Fabric-User-Data-Functions",
            "version": "1.0.0",
        },
        "io.modelcontextprotocol/clientCapabilities": {
            "extensions": {"io.modelcontextprotocol/tasks": {}}
        },
    }


def _prepare_managed_mcp_request(payload: object) -> _McpManagedRequest:
    if type(payload) is not dict or tuple(payload) != _MCP_INPUT_FIELD_ORDER:
        _raise_mcp_contract_error()
    version, protocol_version, message = tuple(payload.values())
    if (
        type(version) is not int
        or version != 1
        or type(protocol_version) is not str
        or protocol_version != _MCP_PROTOCOL_VERSION
        or type(message) is not dict
        or tuple(message) != ("jsonrpc", "id", "method", "params")
        or _contains_session_id(payload)
    ):
        _raise_mcp_contract_error()

    jsonrpc, request_id, method, params = tuple(message.values())
    if (
        type(jsonrpc) is not str
        or jsonrpc != "2.0"
        or type(request_id) not in (int, str)
        or type(request_id) is bool
        or request_id == ""
        or type(method) is not str
        or method not in _MCP_ALLOWED_METHODS
        or type(params) is not dict
    ):
        _raise_mcp_contract_error()

    routing_name = None
    if method in ("server/discover", "tools/list"):
        if params:
            _raise_mcp_contract_error()
    elif method == "tools/call":
        if tuple(params) != ("name", "arguments"):
            _raise_mcp_contract_error()
        routing_name, arguments = tuple(params.values())
        if not _valid_routing_name(routing_name) or type(arguments) is not dict:
            _raise_mcp_contract_error()
    else:
        if tuple(params) != ("taskId",):
            _raise_mcp_contract_error()
        routing_name = params["taskId"]
        if not _valid_routing_name(routing_name):
            _raise_mcp_contract_error()

    forwarded_params = dict(params)
    forwarded_params["_meta"] = _request_meta()
    forwarded_message = {
        "jsonrpc": jsonrpc,
        "id": request_id,
        "method": method,
        "params": forwarded_params,
    }
    try:
        body_bytes = json.dumps(
            forwarded_message, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8", errors="strict")
    except (TypeError, ValueError, UnicodeEncodeError):
        _raise_mcp_contract_error()
    if len(body_bytes) > _MCP_REQUEST_LIMIT_BYTES:
        _raise_mcp_contract_error()
    return _McpManagedRequest(method, routing_name, body_bytes)


def _load_managed_mcp_limits() -> _McpManagedLimits:
    try:
        max_output_bytes = int(os.environ["FABRIC_MCP_MAX_OUTPUT_BYTES"])
        max_json_depth = int(os.environ["FABRIC_MCP_MAX_JSON_DEPTH"])
        allowed_status_codes = tuple(
            int(value)
            for value in os.environ["FABRIC_MCP_ALLOWED_STATUS_CODES"].split(",")
        )
        final_task_statuses = tuple(
            value
            for value in os.environ["FABRIC_MCP_FINAL_TASK_STATUSES"].split(",")
            if value
        )
        max_poll_count = int(os.environ["FABRIC_MCP_MAX_POLL_COUNT"])
    except (KeyError, TypeError, ValueError):
        raise _McpConfigurationError(_MCP_CONFIGURATION_ERROR_MESSAGE) from None
    limits = _McpManagedLimits(
        max_output_bytes,
        max_json_depth,
        allowed_status_codes,
        final_task_statuses,
        max_poll_count,
    )
    _validate_managed_mcp_limits(limits)
    return limits


def _validate_managed_mcp_limits(limits: object) -> None:
    if (
        type(limits) is not _McpManagedLimits
        or type(limits.max_output_bytes) is not int
        or type(limits.max_json_depth) is not int
        or type(limits.allowed_status_codes) is not tuple
        or type(limits.final_task_statuses) is not tuple
        or type(limits.max_poll_count) is not int
        or limits.max_output_bytes <= 0
        or limits.max_json_depth <= 0
        or not limits.allowed_status_codes
        or any(
            type(status) is not int or status < 100 or status > 599
            for status in limits.allowed_status_codes
        )
        or not limits.final_task_statuses
        or any(
            not _valid_routing_name(status) for status in limits.final_task_statuses
        )
        or limits.max_poll_count < 0
    ):
        raise _McpConfigurationError(_MCP_CONFIGURATION_ERROR_MESSAGE) from None


def _json_depth(value: object) -> int:
    maximum = 1
    pending = [(value, 1)]
    while pending:
        current, depth = pending.pop()
        maximum = max(maximum, depth)
        if type(current) is dict:
            pending.extend((nested, depth + 1) for nested in current.values())
        elif type(current) is list:
            pending.extend((nested, depth + 1) for nested in current)
    return maximum


def _validate_managed_mcp_response(
    content: bytes,
    request_id: object,
    limits: _McpManagedLimits,
) -> object:
    if not content:
        return None
    if len(content) > limits.max_output_bytes:
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    try:
        message = json.loads(content.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    if (
        type(message) is not dict
        or _contains_session_id(message)
        or _json_depth(message) > limits.max_json_depth
        or tuple(message) not in (
            ("jsonrpc", "id", "result"),
            ("jsonrpc", "id", "error"),
        )
        or message["jsonrpc"] != "2.0"
        or type(message["id"]) is not type(request_id)
        or message["id"] != request_id
    ):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    return message


async def _read_bounded_mcp_response(
    response: aiohttp.ClientResponse,
    max_output_bytes: int,
) -> bytes:
    content = bytearray()
    async for chunk in response.content.iter_any():
        if len(content) + len(chunk) > max_output_bytes:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        content.extend(chunk)
    return bytes(content)


async def _invoke_managed_mcp(
    payload: object,
    access_token: str,
    limits: _McpManagedLimits,
    session: aiohttp.ClientSession,
) -> dict:
    request = _prepare_managed_mcp_request(payload)
    _validate_managed_mcp_limits(limits)
    if type(access_token) is not str or not access_token:
        raise _McpConfigurationError(_MCP_CONFIGURATION_ERROR_MESSAGE) from None
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": _JSON_MEDIA_TYPE,
        "Accept": _JSON_MEDIA_TYPE,
        "MCP-Protocol-Version": _MCP_PROTOCOL_VERSION,
        "Mcp-Method": request.method,
        "X-Variants": _MCP_VARIANTS,
    }
    if request.routing_name is not None:
        headers["Mcp-Name"] = request.routing_name

    response = await session.post(
        _MCP_ENDPOINT,
        data=request.body_bytes,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=_MCP_INVOKE_TIMEOUT_SECONDS),
    )
    try:
        if response.status not in limits.allowed_status_codes:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        content = await _read_bounded_mcp_response(
            response,
            limits.max_output_bytes,
        )
        message = _validate_managed_mcp_response(
            content,
            payload["message"]["id"],
            limits,
        )
    finally:
        response.release()
    return {"version": 1, "message": message}


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


@udf.function()
async def rayfin_fabric_mcp_v1(payload: dict, accesstoken: str) -> dict:
    limits = _load_managed_mcp_limits()
    session = await _get_session()
    return await _invoke_managed_mcp(payload, accesstoken, limits, session)


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
        response.release()
        self._response = None
        self._released = True

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
