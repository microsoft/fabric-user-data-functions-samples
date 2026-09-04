import fabric.functions as fn
import aiohttp
import asyncio
import inspect
import json
import logging
import math
import os
import time
import uuid
from typing import Optional
from urllib.parse import urlparse, urlsplit

udf = fn.UserDataFunctions()

_POWERBI_BASE = os.environ.get("POWERBI_API_BASE", "https://dailyapi.powerbi.com/v1.0/myorg")
_ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"
_JSON_MEDIA_TYPE = "application/json"
_FABRIC_MCP_ENDPOINT = "https://api.fabric.microsoft.com/v1/mcp/fabriciq"
_FABRIC_MCP_MAX_BYTES = 5 * 1024 * 1024
_FABRIC_MCP_MAX_DEPTH = 64
_FABRIC_MCP_MAX_HEADERS = 32
_FABRIC_MCP_MAX_HEADER_NAME_LENGTH = 128
_FABRIC_MCP_MAX_HEADER_VALUE_LENGTH = 128
_FABRIC_MCP_MAX_PROTOCOL_VERSION_LENGTH = 128
_FABRIC_MCP_TIMEOUT_SECONDS = 5 * 60
_FABRIC_MCP_INPUT_FIELDS = ("version", "protocolVersion", "headers", "message")
_FABRIC_MCP_RESERVED_HEADERS = frozenset(
    (
        "accept",
        "authorization",
        "connection",
        "content-length",
        "content-type",
        "cookie",
        "host",
        "keep-alive",
        "proxy-authorization",
        "proxy-connection",
        "set-cookie",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    )
)
_FABRIC_MCP_REQUEST_ERROR = "Invalid Fabric MCP request."
_FABRIC_MCP_RESPONSE_ERROR = "Invalid Fabric MCP response."
_FABRIC_MCP_BOUNDS_ERROR = "Fabric MCP content exceeds server limits."
_FABRIC_MCP_CONFIGURATION_ERROR = "Fabric MCP server configuration is unavailable."
_FABRIC_MCP_UPSTREAM_ERROR = "Fabric MCP upstream request failed."
_fabric_mcp_logger = logging.getLogger(__name__)

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
            # No total/read timeout: a streamed response can take a while to
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
    """Own an acquired response until its streaming body is closed."""

    def __init__(self, response):
        self._response = response
        self._iterator = response.content.iter_any().__aiter__()

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            await self.aclose()
            raise
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self):
        if self._response is None:
            return
        response = self._response
        self._response = None
        response.release()
        close_iterator = getattr(self._iterator, "aclose", None)
        if close_iterator is not None:
            await close_iterator()


class FabricMcpRequestError(ValueError):
    pass


class FabricMcpResponseError(RuntimeError):
    pass


class FabricMcpBoundsError(FabricMcpRequestError):
    pass


class FabricMcpConfigurationError(RuntimeError):
    pass


def _fail_mcp_request():
    raise FabricMcpRequestError(_FABRIC_MCP_REQUEST_ERROR) from None


def _fail_mcp_response():
    raise FabricMcpResponseError(_FABRIC_MCP_RESPONSE_ERROR) from None


def _fail_mcp_bounds():
    raise FabricMcpBoundsError(_FABRIC_MCP_BOUNDS_ERROR) from None


def _validate_mcp_json(value, maximum_depth, fail):
    stack = [(value, 1, False)]
    active = set()
    while stack:
        current, depth, leaving = stack.pop()
        if leaving:
            active.remove(id(current))
            continue
        if depth > maximum_depth:
            fail()
        if current is None or type(current) in (bool, int, str):
            continue
        if type(current) is float:
            if not math.isfinite(current):
                fail()
            continue
        if type(current) not in (dict, list):
            fail()
        identity = id(current)
        if identity in active:
            fail()
        active.add(identity)
        stack.append((current, depth, True))
        items = current.items() if type(current) is dict else enumerate(current)
        items = tuple(items)
        if type(current) is dict and any(type(key) is not str for key, _ in items):
            fail()
        for _, item in reversed(items):
            stack.append((item, depth + 1, False))


def _dump_mcp_json(value, fail):
    try:
        return json.dumps(
            value, ensure_ascii=True, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError):
        fail()


def _encode_mcp_json(value, maximum_depth, fail):
    _validate_mcp_json(value, maximum_depth, fail)
    return _dump_mcp_json(value, fail)


def _safe_mcp_protocol_version(value):
    return (
        type(value) is str
        and 0 < len(value) <= _FABRIC_MCP_MAX_PROTOCOL_VERSION_LENGTH
        and value.isascii()
        and value.strip() == value
        and not any(
            ord(character) < 0x20 or ord(character) == 0x7F
            for character in value
        )
    )


def _safe_mcp_header_name(value):
    token_characters = "!#$%&'*+-.^_`|~"
    return (
        type(value) is str
        and 0 < len(value) <= _FABRIC_MCP_MAX_HEADER_NAME_LENGTH
        and value.isascii()
        and all(
            character.isalnum() or character in token_characters
            for character in value
        )
    )


def _reserved_mcp_header(value):
    lowered = value.lower()
    components = []
    component = []
    for character in lowered:
        if character.isalnum():
            component.append(character)
        elif component:
            components.append("".join(component))
            component = []
    if component:
        components.append("".join(component))
    compact = "".join(components)
    credential_component = any(
        item in ("credential", "credentials", "key", "token")
        or item.startswith(("credential", "key", "token"))
        for item in components
    )
    return (
        lowered in _FABRIC_MCP_RESERVED_HEADERS
        or lowered.startswith("mcp-")
        or lowered.startswith("proxy-")
        or credential_component
        or any(
            marker in compact
            for marker in (
                "apikey",
                "accesskey",
                "accesstoken",
                "credential",
                "secretkey",
                "subscriptionkey",
                "token",
            )
        )
    )


def _safe_mcp_header_value(value):
    return (
        type(value) is str
        and 0 < len(value) <= _FABRIC_MCP_MAX_HEADER_VALUE_LENGTH
        and value.isascii()
        and value.strip() == value
        and not any(
            ord(character) < 0x20 or ord(character) == 0x7F
            for character in value
        )
    )


def _safe_mcp_application_headers(headers):
    if type(headers) is not dict or len(headers) > _FABRIC_MCP_MAX_HEADERS:
        return False
    names = set()
    for name, value in headers.items():
        lowered = name.lower() if type(name) is str else ""
        if (
            lowered in names
            or not _safe_mcp_header_name(name)
            or _reserved_mcp_header(name)
            or not _safe_mcp_header_value(value)
        ):
            return False
        names.add(lowered)
    return True


def _parse_mcp_request(payload):
    if type(payload) is not dict:
        _fail_mcp_request()
    if (
        len(payload) != len(_FABRIC_MCP_INPUT_FIELDS)
        or frozenset(payload) != frozenset(_FABRIC_MCP_INPUT_FIELDS)
    ):
        _fail_mcp_request()
    version = payload["version"]
    protocol_version = payload["protocolVersion"]
    headers = payload["headers"]
    message = payload["message"]
    if (
        type(version) is not int
        or version != 1
        or not _safe_mcp_protocol_version(protocol_version)
        or not _safe_mcp_application_headers(headers)
        or type(message) is not dict
    ):
        _fail_mcp_request()

    envelope = _encode_mcp_json(
        payload, _FABRIC_MCP_MAX_DEPTH, _fail_mcp_request
    )
    if len(envelope) > _FABRIC_MCP_MAX_BYTES:
        _fail_mcp_bounds()
    message_bytes = _dump_mcp_json(message, _fail_mcp_request)
    return (
        protocol_version,
        headers,
        message_bytes,
        len(envelope),
    )


def _load_mcp_json(raw):
    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate member")
            result[key] = value
        return result

    return json.loads(raw, object_pairs_hook=reject_duplicates)


def _parse_mcp_sse(raw):
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        _fail_mcp_response()
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if not text.endswith("\n\n"):
        _fail_mcp_response()

    messages = []
    data = []
    event_name = None
    for line in text.split("\n"):
        if not line:
            if data:
                if event_name not in (None, "message"):
                    _fail_mcp_response()
                try:
                    messages.append(_load_mcp_json("\n".join(data)))
                except (
                    json.JSONDecodeError,
                    TypeError,
                    ValueError,
                    RecursionError,
                ):
                    _fail_mcp_response()
                data = []
                event_name = None
            elif event_name is not None:
                _fail_mcp_response()
        elif line.startswith(":"):
            continue
        elif line.startswith("event:") and event_name is None:
            event_name = line[6:].lstrip(" ")
        elif line.startswith("data:"):
            data.append(line[5:].lstrip(" "))
        else:
            _fail_mcp_response()
    if data or event_name is not None:
        _fail_mcp_response()
    return messages


def _parse_mcp_response(raw, content_type):
    media_type = content_type.lower().split(";", 1)[0].strip()
    message_validated = False
    if media_type == "text/event-stream":
        events = _parse_mcp_sse(raw)
        for event in events:
            _validate_mcp_json(
                event, _FABRIC_MCP_MAX_DEPTH - 1, _fail_mcp_bounds
            )
        if not events:
            _fail_mcp_response()
        message = events[-1]
        message_validated = True
    elif media_type == _JSON_MEDIA_TYPE:
        try:
            message = _load_mcp_json(raw.decode("utf-8", errors="strict"))
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
            RecursionError,
        ):
            _fail_mcp_response()
    else:
        _fail_mcp_response()

    if type(message) is not dict:
        _fail_mcp_response()
    if not message_validated:
        _validate_mcp_json(
            message, _FABRIC_MCP_MAX_DEPTH - 1, _fail_mcp_response
        )
    return message


async def _read_mcp_response(response):
    try:
        iterator = response.content.iter_any().__aiter__()
    except Exception:
        try:
            response.release()
        except Exception:
            response.close()
        raise FabricMcpResponseError(_FABRIC_MCP_UPSTREAM_ERROR) from None

    chunks = []
    total = 0
    failed = False
    try:
        async for chunk in iterator:
            chunk = chunk if type(chunk) is bytes else bytes(chunk)
            total += len(chunk)
            if total > _FABRIC_MCP_MAX_BYTES:
                _fail_mcp_bounds()
            chunks.append(chunk)
    except BaseException:
        failed = True
        raise
    finally:
        close_error = None
        try:
            response.release()
        except Exception as error:
            close_error = error
            try:
                response.close()
            except Exception:
                pass
        close_iterator = getattr(iterator, "aclose", None)
        if close_iterator is not None:
            try:
                await close_iterator()
            except Exception as error:
                close_error = close_error or error
                try:
                    response.close()
                except Exception:
                    pass
        if close_error is not None and not failed:
            raise FabricMcpResponseError(_FABRIC_MCP_UPSTREAM_ERROR) from None
    return b"".join(chunks)


def _load_mcp_endpoint():
    parsed = urlsplit(_FABRIC_MCP_ENDPOINT)
    if (
        "FABRIC_MCP_ENDPOINT" in os.environ
        or "FABRIC_MCP_RING" in os.environ
        or parsed.scheme != "https"
        or parsed.geturl() != _FABRIC_MCP_ENDPOINT
    ):
        raise FabricMcpConfigurationError(
            _FABRIC_MCP_CONFIGURATION_ERROR
        ) from None
    return _FABRIC_MCP_ENDPOINT


def _log_mcp(outcome, started, request_bytes, response_bytes):
    _fabric_mcp_logger.info(
        "Fabric MCP operation",
        extra={
            "fabric_mcp_outcome": outcome,
            "fabric_mcp_duration_ms": max(
                0, int((time.monotonic() - started) * 1000)
            ),
            "fabric_mcp_request_bytes": max(
                0, min(request_bytes, _FABRIC_MCP_MAX_BYTES)
            ),
            "fabric_mcp_response_bytes": max(
                0, min(response_bytes, _FABRIC_MCP_MAX_BYTES)
            ),
        },
    )


async def _invoke_fabric_mcp(
    payload, token_provider, session_provider=_get_session
):
    started = time.monotonic()
    request_bytes = 0
    try:
        (
            protocol_version,
            application_headers,
            message_bytes,
            request_bytes,
        ) = _parse_mcp_request(payload)
        try:
            token = token_provider()
            if inspect.isawaitable(token):
                token = await token
        except Exception:
            raise FabricMcpConfigurationError(
                _FABRIC_MCP_CONFIGURATION_ERROR
            ) from None
        if (
            type(token) is not str
            or not token
            or "\r" in token
            or "\n" in token
        ):
            raise FabricMcpConfigurationError(
                _FABRIC_MCP_CONFIGURATION_ERROR
            ) from None
        try:
            session = session_provider()
            if inspect.isawaitable(session):
                session = await session
        except Exception:
            raise FabricMcpConfigurationError(
                _FABRIC_MCP_CONFIGURATION_ERROR
            ) from None

        headers = {
            **application_headers,
            "Authorization": f"Bearer {token}",
            "Content-Type": _JSON_MEDIA_TYPE,
            "Accept": "application/json, text/event-stream",
            "Mcp-Protocol-Version": protocol_version,
        }

        async def send_once():
            response = await session.post(
                _load_mcp_endpoint(),
                data=message_bytes,
                headers=headers,
                timeout=aiohttp.ClientTimeout(
                    total=_FABRIC_MCP_TIMEOUT_SECONDS, sock_connect=30
                ),
                allow_redirects=False,
            )
            content_type = response.headers.get(
                "Content-Type", _JSON_MEDIA_TYPE
            )
            raw = await _read_mcp_response(response)
            if response.status < 200 or response.status >= 300:
                raise FabricMcpResponseError(
                    _FABRIC_MCP_UPSTREAM_ERROR
                ) from None
            response_message = _parse_mcp_response(raw, content_type)
            output = {"version": 1, "message": response_message}
            if (
                len(
                    _encode_mcp_json(
                        output, _FABRIC_MCP_MAX_DEPTH, _fail_mcp_bounds
                    )
                )
                > _FABRIC_MCP_MAX_BYTES
            ):
                _fail_mcp_bounds()
            return output, len(raw)

        output, response_bytes = await asyncio.wait_for(
            send_once(), timeout=_FABRIC_MCP_TIMEOUT_SECONDS
        )
    except asyncio.CancelledError:
        _log_mcp("cancelled", started, request_bytes, 0)
        raise
    except asyncio.TimeoutError:
        _log_mcp("timeout", started, request_bytes, 0)
        raise FabricMcpResponseError(_FABRIC_MCP_UPSTREAM_ERROR) from None
    except (aiohttp.ClientError, FabricMcpResponseError):
        _log_mcp("upstream", started, request_bytes, 0)
        raise FabricMcpResponseError(_FABRIC_MCP_UPSTREAM_ERROR) from None
    except (FabricMcpRequestError, FabricMcpConfigurationError):
        _log_mcp("boundary", started, request_bytes, 0)
        raise
    except Exception:
        _log_mcp("upstream", started, request_bytes, 0)
        raise FabricMcpResponseError(_FABRIC_MCP_UPSTREAM_ERROR) from None

    _log_mcp(
        "success",
        started,
        request_bytes,
        response_bytes,
    )
    return output


@udf.generic_connection(argName="fabricIqClient", audienceType="Fabric")
@udf.function()
async def rayfin_fabric_mcp_v1(
    payload: dict, fabricIqClient: fn.FabricItem
) -> dict:
    def token_provider():
        return fabricIqClient.get_access_token().get_token().token

    return await _invoke_fabric_mcp(payload, token_provider)


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
        _ResponseBodyIterator(resp), media_type=_ARROW_MEDIA_TYPE
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
        _ResponseBodyIterator(resp), media_type=_JSON_MEDIA_TYPE
    )
