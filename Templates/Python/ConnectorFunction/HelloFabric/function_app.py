import asyncio
import json
import os
import re
import uuid
from urllib.parse import unquote, urlparse

import aiohttp
import fabric.functions as fn

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
_session: aiohttp.ClientSession | None = None
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
# rayfin_fabric_mcp_v1 - generic MCP transport for allowlisted Fabric hosts.
# ---------------------------------------------------------------------------

_FABRIC_MCP_PROD_HOST = "api.fabric.microsoft.com"
_DEFAULT_FABRIC_MCP_ALLOWED_HOSTS = (
    _FABRIC_MCP_PROD_HOST,
    "dailyapi.fabric.microsoft.com",
)
_FABRIC_MCP_HOST_SUFFIXES = (".fabric.microsoft.com", ".powerbi.com")
_FABRIC_MCP_ALLOWED_HOSTS_ENV = "RAYFIN_FABRIC_MCP_ALLOWED_HOSTS"
_FABRIC_MCP_DEFAULT_PROTOCOL_VERSION = "2025-06-18"
_FABRIC_MCP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=100, sock_connect=30, sock_read=100
)
_FABRIC_MCP_MAX_RESPONSE_BYTES = 4 * 1024 * 1024

# `endpoint` is the one contract-approved target field. Other top-level fields
# that could redirect the request or replace delegated authentication are denied.
# Tool arguments inside `params` are opaque and are intentionally not inspected.
_TARGET_OVERRIDE_KEYS = frozenset(
    {
        "target",
        "baseurl",
        "url",
        "mcpurl",
        "workspaceid",
        "itemid",
        "host",
        "origin",
        "headers",
        "authorization",
        "token",
        "accesstoken",
        "access_token",
    }
)


def _is_safe_fabric_host(host):
    if not isinstance(host, str) or not host or host != host.lower():
        return False
    try:
        host.encode("ascii")
    except UnicodeEncodeError:
        return False
    if len(host) > 253:
        return False
    labels = host.split(".")
    if any(
        not label
        or label.startswith("xn--")
        or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
        is None
        for label in labels
    ):
        return False
    return any(
        host.endswith(suffix) and host != suffix[1:]
        for suffix in _FABRIC_MCP_HOST_SUFFIXES
    )


def _normalize_allowed_host_pattern(pattern):
    pattern = pattern.strip().lower()
    wildcard = pattern.startswith("*.")
    host = pattern[2:] if wildcard else pattern
    safe_wildcard_root = wildcard and any(
        host == suffix[1:] for suffix in _FABRIC_MCP_HOST_SUFFIXES
    )
    if (
        not pattern
        or any(char.isspace() or ord(char) < 32 for char in pattern)
        or any(char in pattern for char in ("/", "\\", "@", ":", "?", "#"))
        or "*" in host
        or not (_is_safe_fabric_host(host) or safe_wildcard_root)
    ):
        raise ValueError(f"Invalid Fabric MCP allowlist host: {pattern}")
    return f"*.{host}" if wildcard else host


def _get_allowed_fabric_mcp_hosts():
    configured = os.environ.get(_FABRIC_MCP_ALLOWED_HOSTS_ENV)
    values = (
        configured.split(",")
        if configured is not None
        else _DEFAULT_FABRIC_MCP_ALLOWED_HOSTS
    )
    if not values or any(not value.strip() for value in values):
        raise ValueError("Fabric MCP host allowlist must not be empty")
    return tuple(_normalize_allowed_host_pattern(value) for value in values)


def _host_matches_pattern(host, pattern):
    if pattern.startswith("*."):
        suffix = pattern[1:]
        return host.endswith(suffix) and host != pattern[2:]
    return host == pattern


def _validate_fabric_mcp_endpoint(endpoint):
    if not isinstance(endpoint, str) or not endpoint:
        raise ValueError("endpoint is required")
    if any(char.isspace() or ord(char) < 32 for char in endpoint):
        raise ValueError("endpoint contains invalid characters")

    parsed = urlparse(endpoint)
    if parsed.scheme.lower() != "https":
        raise ValueError("endpoint scheme must be https")
    if not parsed.netloc or parsed.username is not None or parsed.password is not None:
        raise ValueError("endpoint must not contain userinfo")
    if "@" in parsed.netloc:
        raise ValueError("endpoint must not contain embedded credentials")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("endpoint port is invalid") from exc
    if port not in (None, 443):
        raise ValueError("endpoint must use the default HTTPS port")

    host = (parsed.hostname or "").lower()
    if not _is_safe_fabric_host(host):
        raise ValueError("endpoint host is not a Fabric host")
    allowed_hosts = _get_allowed_fabric_mcp_hosts()
    if not any(_host_matches_pattern(host, pattern) for pattern in allowed_hosts):
        raise ValueError("endpoint host is not allowlisted")
    if (
        not parsed.path.startswith("/v1/mcp/")
        or "\\" in parsed.path
        or ";" in parsed.path
        or parsed.params
        or unquote(parsed.path) != parsed.path
        or any(segment in (".", "..") for segment in parsed.path.split("/"))
    ):
        raise ValueError("endpoint path must begin with /v1/mcp/")
    if parsed.fragment:
        raise ValueError("endpoint must not contain a fragment")
    return endpoint


def _detect_target_override(input_data):
    if not isinstance(input_data, dict):
        return None
    for key in input_data:
        if isinstance(key, str) and key.lower() in _TARGET_OVERRIDE_KEYS:
            return key
    return None


def _needs_msit_redirect_disable(url):
    return (urlparse(url).hostname or "").lower() != _FABRIC_MCP_PROD_HOST


def _header_get(headers, name):
    if not headers:
        return None
    for key, value in headers.items():
        if isinstance(key, str) and key.lower() == name.lower():
            return value
    return None


def _sdk_envelope(output=None, errors=None):
    error_list = list(errors or [])
    return {
        "status": "Failed" if error_list else "Succeeded",
        "output": output if output is not None else {},
        "errors": error_list,
    }


def _mcp_error(code, message, source="connector", retryable=False, http_status=None):
    error = {
        "code": code,
        "message": message,
        "source": source,
        "retryable": retryable,
    }
    if http_status is not None:
        error["httpStatus"] = http_status
    return error


def _stream_envelope(envelope):
    body = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
    return fn.StreamResponse(iter([body]), media_type=_JSON_MEDIA_TYPE)


def _jsonrpc_body(method, params):
    body = {"jsonrpc": "2.0", "method": method}
    request_id = None
    if not method.startswith("notifications/"):
        request_id = str(uuid.uuid4())
        body["id"] = request_id
    if params is not None:
        body["params"] = params
    return body, request_id


def _parse_sse_data(text, expected_request_id):
    messages = []
    data_lines = []

    def flush():
        if not data_lines:
            return
        try:
            message = json.loads("\n".join(data_lines))
        except (TypeError, ValueError):
            message = None
        if isinstance(message, dict):
            messages.append(message)
        data_lines.clear()

    for raw_line in text.splitlines():
        if raw_line == "":
            flush()
        elif raw_line.startswith("data:"):
            data_lines.append(raw_line[5:].lstrip(" "))
    flush()

    if expected_request_id is not None:
        matching = [
            message
            for message in messages
            if message.get("id") == expected_request_id
        ]
        return matching[-1] if matching else None
    return messages[-1] if messages else None


class _ResponseTooLarge(Exception):
    pass


async def _read_bounded_response_text(resp):
    content_length = getattr(resp, "content_length", None)
    if (
        isinstance(content_length, int)
        and content_length > _FABRIC_MCP_MAX_RESPONSE_BYTES
    ):
        raise _ResponseTooLarge()

    chunks = []
    size = 0
    async for chunk in resp.content.iter_chunked(64 * 1024):
        size += len(chunk)
        if size > _FABRIC_MCP_MAX_RESPONSE_BYTES:
            raise _ResponseTooLarge()
        chunks.append(chunk)
    return b"".join(chunks).decode("utf-8")


def _validate_mcp_input(input_data):
    if not isinstance(input_data, dict):
        raise TypeError("input must be a JSON object")

    override_key = _detect_target_override(input_data)
    if override_key is not None:
        raise ValueError(f"Caller may not override target/auth field: {override_key}")

    endpoint = _validate_fabric_mcp_endpoint(input_data.get("endpoint"))
    method = input_data.get("method")
    params = input_data.get("params", {})
    protocol_version = input_data.get(
        "protocolVersion", _FABRIC_MCP_DEFAULT_PROTOCOL_VERSION
    )
    session_id = input_data.get("sessionId")

    if (
        not isinstance(method, str)
        or not method
        or any(ord(char) < 32 for char in method)
    ):
        raise ValueError("method is required")
    if not isinstance(params, dict):
        raise TypeError("params must be a JSON object")
    if (
        not isinstance(protocol_version, str)
        or re.fullmatch(r"\d{4}-\d{2}-\d{2}", protocol_version) is None
    ):
        raise ValueError("protocolVersion must use YYYY-MM-DD format")
    if session_id is not None and (
        not isinstance(session_id, str)
        or not session_id
        or any(ord(char) < 32 for char in session_id)
    ):
        raise ValueError("sessionId must be a non-empty string")
    return endpoint, method, params, protocol_version, session_id


async def _execute_fabric_mcp(
    endpoint, method, params, protocol_version, session_id, accesstoken
):
    request, request_id = _jsonrpc_body(method, params)
    headers = {
        "Authorization": f"Bearer {accesstoken}",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "MCP-Protocol-Version": protocol_version,
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    if _needs_msit_redirect_disable(endpoint):
        headers["X-Variants"] = "Fabric.DisableMsitRedirect"

    session = await _get_session()
    resp = await session.post(
        endpoint,
        json=request,
        headers=headers,
        timeout=_FABRIC_MCP_REQUEST_TIMEOUT,
    )
    response_session_id = _header_get(resp.headers, "Mcp-Session-Id")

    output = {}
    if response_session_id:
        output["sessionId"] = response_session_id
    if resp.status >= 400:
        resp.release()
        error = _mcp_error(
            "UpstreamHttpError",
            f"Upstream returned HTTP {resp.status}",
            source="upstream",
            retryable=resp.status == 429 or resp.status >= 500,
            http_status=resp.status,
        )
        return _sdk_envelope(output, [error])

    try:
        response_text = await _read_bounded_response_text(resp)
    finally:
        resp.release()

    if not response_text.strip() and request_id is None:
        return _sdk_envelope(output)

    content_type = (_header_get(resp.headers, "Content-Type") or "").lower()
    if "text/event-stream" in content_type:
        message = _parse_sse_data(response_text, request_id)
    else:
        try:
            message = json.loads(response_text)
        except (TypeError, ValueError):
            message = None

    if not isinstance(message, dict):
        error = _mcp_error(
            "InvalidUpstreamResponse",
            "Upstream returned an invalid MCP response",
            source="upstream",
            retryable=True,
            http_status=resp.status,
        )
        return _sdk_envelope(output, [error])
    if request_id is not None and message.get("id") != request_id:
        error = _mcp_error(
            "MismatchedResponseId",
            "Upstream response did not match the request",
            source="upstream",
            retryable=True,
            http_status=resp.status,
        )
        return _sdk_envelope(output, [error])
    if message.get("error") is not None:
        rpc_error = message["error"]
        if isinstance(rpc_error, dict):
            error = dict(rpc_error)
            error.setdefault("source", "upstream")
            error.setdefault("retryable", False)
        else:
            error = _mcp_error(
                "McpError", "Upstream returned an MCP error", source="upstream"
            )
        return _sdk_envelope(output, [error])

    output.update(message)
    if response_session_id:
        output["sessionId"] = response_session_id
    return _sdk_envelope(output)


@udf.streaming_function()
async def rayfin_fabric_mcp_v1(payload: dict, accesstoken: str) -> fn.StreamResponse:
    if not isinstance(payload, dict):
        return _stream_envelope(
            _sdk_envelope(
                errors=[
                    _mcp_error(
                        "InvalidInput",
                        "payload must be a JSON object",
                        retryable=False,
                    )
                ]
            )
        )

    operation = payload.get("operation")
    if operation != "executeQuery":
        raise ValueError(f"Unsupported operation: {operation}")

    input_data = payload.get("input", {})
    try:
        endpoint, method, params, protocol_version, session_id = _validate_mcp_input(
            input_data
        )
        if not isinstance(accesstoken, str) or not accesstoken:
            raise ValueError("accesstoken is required")
    except (TypeError, ValueError) as exc:
        return _stream_envelope(
            _sdk_envelope(
                errors=[_mcp_error("InvalidInput", str(exc), retryable=False)]
            )
        )

    try:
        envelope = await _execute_fabric_mcp(
            endpoint,
            method,
            params,
            protocol_version,
            session_id,
            accesstoken,
        )
    except asyncio.TimeoutError:
        envelope = _sdk_envelope(
            errors=[
                _mcp_error(
                    "Timeout",
                    "Upstream request timed out",
                    source="upstream",
                    retryable=True,
                )
            ]
        )
    except (aiohttp.ClientError, OSError):
        envelope = _sdk_envelope(
            errors=[
                _mcp_error(
                    "UpstreamError",
                    "Upstream request failed",
                    source="upstream",
                    retryable=True,
                )
            ]
        )
    except (UnicodeDecodeError, _ResponseTooLarge):
        envelope = _sdk_envelope(
            errors=[
                _mcp_error(
                    "InvalidUpstreamResponse",
                    "Upstream response could not be read",
                    source="upstream",
                    retryable=True,
                )
            ]
        )
    return _stream_envelope(envelope)
