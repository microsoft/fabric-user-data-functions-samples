import fabric.functions as fn
import aiohttp
import asyncio
import os
import uuid
import json
import logging
import datetime
import email.utils
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
# rayfin_fabric_aihub_v1 - MCP (Model Context Protocol) adapter over a FIXED
# Fabric AI Hub endpoint. Unlike the streaming connectors above, this one PARSES
# and NORMALIZES upstream responses and returns a dict (never a byte stream).
# ---------------------------------------------------------------------------

# Environment-derived fixed target. The DAILY default is required. Caller input
# must NEVER influence this URL (see _detect_target_override).
_FABRIC_AIHUB_MCP_URL = os.environ.get(
    "FABRIC_AIHUB_MCP_URL",
    "https://dailyapi.fabric.microsoft.com/v1/mcp/fabricaihub/integrations/m365",
)
_FABRIC_AIHUB_PROD_HOST = "api.fabric.microsoft.com"
_FABRIC_AIHUB_CONNECTOR_NAME = "fabric-aihub"
_MCP_PROTOCOL_VERSION = "2025-06-18"
_MCP_CLIENT_NAME = "rayfin-fabric-aihub"
_MCP_CLIENT_VERSION = "1"

# AI Hub adapter operations are SHORT request/reply round trips (NOT streamed),
# so they must not inherit the shared session's unbounded read timeout (which
# exists so long-draining DAX streams are not cut off). Bind every AI Hub call
# with a finite per-request timeout to avoid hanging forever on `resp.text()`.
_AIHUB_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=100, sock_connect=30, sock_read=100)

# Caller-supplied keys that would (or could) redirect the request away from the
# fixed AI Hub endpoint. Any of these in payload["input"] is rejected outright.
# Tool-specific `artifactId` is intentionally NOT here: it is a legitimate tool
# argument that rides INSIDE startTask arguments and never affects the URL.
_TARGET_OVERRIDE_KEYS = frozenset(
    {"endpoint", "target", "baseurl", "url", "mcpurl", "workspaceid", "itemid", "host", "origin"}
)


def _detect_target_override(input_data):
    if not isinstance(input_data, dict):
        return None
    for key in input_data.keys():
        if isinstance(key, str) and key.lower() in _TARGET_OVERRIDE_KEYS:
            return key
    return None


def _needs_msit_redirect_disable(url):
    # X-Variants: Fabric.DisableMsitRedirect applies ONLY to non-production hosts
    # (daily/test). Production (api.fabric.microsoft.com) must never receive it.
    # Kept in one place so the host->header rule lives in exactly one spot.
    host = (urlparse(url).netloc or "").split("@")[-1].split(":")[0].lower()
    return host != _FABRIC_AIHUB_PROD_HOST


def _mcp_headers(url, invocation_id, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "x-ms-client-request-id": invocation_id,
    }
    if _needs_msit_redirect_disable(url):
        headers["X-Variants"] = "Fabric.DisableMsitRedirect"
    return headers


def _jsonrpc_body(method, params, notification=False):
    # Fresh JSON-RPC 2.0 id per request. A NOTIFICATION (per JSON-RPC 2.0 / MCP)
    # carries NO `id` field and expects no response; used for
    # notifications/initialized in the MCP lifecycle.
    body = {"jsonrpc": "2.0", "method": method}
    if not notification:
        body["id"] = str(uuid.uuid4())
    if params is not None:
        body["params"] = params
    return body


def _header_get(headers, *names):
    if not headers:
        return None
    try:
        items = list(headers.items())
    except AttributeError:
        return None
    lowered = {}
    for k, v in items:
        if isinstance(k, str):
            lowered[k.lower()] = v
    for name in names:
        val = lowered.get(name.lower())
        if val is not None:
            return val
    return None


def _extract_correlation(headers):
    request_id = _header_get(headers, "x-ms-request-id", "RequestId")
    root_activity_id = _header_get(headers, "x-ms-root-activity-id", "RootActivityId")
    return request_id, root_activity_id


def _parse_retry_after(value):
    # Returns (seconds, raw). `seconds` is a non-negative int when Retry-After is
    # a bare delta-seconds integer OR an HTTP-date we can turn into a future
    # delay; `raw` is the original string when it is neither, so an unparseable
    # value is surfaced under a distinct diagnostics key instead of being dropped
    # or mislabeled as a seconds delay.
    if value is None:
        return (None, None)
    text = str(value).strip()
    if text == "":
        return (None, None)
    try:
        return (max(0, int(text)), None)
    except (ValueError, TypeError):
        pass
    try:
        dt = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        dt = None
    if dt is not None:
        try:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            delta = (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            return (max(0, int(delta)), None)
        except (ValueError, OverflowError, OSError):
            pass
    return (None, text)


def _error_envelope(code, message, source, retryable, userError, httpStatus=None,
                    taskId=None, invocationId=None, requestId=None, rootActivityId=None,
                    causeCode=None, diagnostics=None):
    err = {
        "code": code,
        "message": message,
        "source": source,
        "retryable": retryable,
        "userError": userError,
        "connectorName": _FABRIC_AIHUB_CONNECTOR_NAME,
    }
    if httpStatus is not None:
        err["httpStatus"] = httpStatus
    if taskId is not None:
        err["taskId"] = taskId
    if invocationId is not None:
        err["invocationId"] = invocationId
    if requestId is not None:
        err["requestId"] = requestId
    if rootActivityId is not None:
        err["rootActivityId"] = rootActivityId
    if causeCode is not None:
        err["causeCode"] = causeCode
    if diagnostics is not None:
        err["diagnostics"] = diagnostics
    return {"error": err}


def _try_json(text):
    if text is None:
        return None
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return None


def _parse_sse(text):
    # Parse an SSE stream and return the LAST event whose data is a JSON-RPC
    # message containing `result` or `error`. Lines starting with ':' are
    # comments (e.g. keep-alive/ping) and ignored; a blank line terminates an
    # event; multiple `data:` lines within one event are joined with newlines.
    if not text:
        return None
    last = None
    data_lines = []

    def _flush(collected):
        if not collected:
            return None
        msg = _try_json("\n".join(collected))
        if isinstance(msg, dict) and ("result" in msg or "error" in msg):
            return msg
        return None

    for raw in text.split("\n"):
        line = raw.rstrip("\r")
        if line == "":
            found = _flush(data_lines)
            data_lines = []
            if found is not None:
                last = found
            continue
        if line.startswith(":"):
            continue
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip(" "))
    found = _flush(data_lines)
    if found is not None:
        last = found
    return last


_HTTP_ERROR_MAP = {
    400: ("BadRequest", False, True),
    401: ("Unauthorized", False, True),
    403: ("Forbidden", False, True),
    404: ("NotFound", False, True),
    408: ("Timeout", True, False),
    429: ("Throttled", True, False),
}


def _sanitize_for_log(value, max_len=120):
    # Caller-derived values can contain CR/LF or other control chars (log
    # forging / injection) or be arbitrarily long. Replace every control char
    # and cap the length before the value reaches any log line or user-facing
    # error message.
    if value is None:
        return ""
    text = str(value)
    cleaned = "".join("?" if (ord(c) < 32 or ord(c) == 127) else c for c in text)
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len] + "...(truncated)"
    return cleaned


def _merge_preserving(base, overlay, overlay_label):
    # Non-destructive merge: overlay keys are added to base. When a key already
    # exists in base with a DIFFERENT value, the base value stays in place and
    # the colliding overlay value is preserved (never silently dropped) under
    # base["_collisions"][overlay_label][key], so nothing is unrecoverable.
    for k, v in overlay.items():
        if k not in base:
            base[k] = v
        elif base[k] != v:
            base.setdefault("_collisions", {}).setdefault(overlay_label, {})[k] = v
    return base


def _shape_result(result):
    # Lossless normalization: prefer copying the whole server result and adding
    # to it over rebuilding field-by-field, so unknown/additive keys survive. A
    # `task` wrapper is unwrapped to the top level; task fields win at the top,
    # and any colliding OUTER sibling value is preserved under
    # merged["_collisions"]["outer"][key] so no field is ever lost.
    if isinstance(result, dict) and isinstance(result.get("task"), dict):
        out = dict(result)
        task = out.pop("task")
        merged = dict(task)
        for k, v in out.items():
            if k not in merged:
                merged[k] = v
            elif merged[k] != v:
                merged.setdefault("_collisions", {}).setdefault("outer", {})[k] = v
        return merged
    if isinstance(result, dict):
        return dict(result)
    return result

async def _read_mcp_response(resp, operation, invocation_id, known_task_id):
    # ONE shared response handler used by every operation. Handles JSON, SSE,
    # 202 acks, JSON-RPC errors on HTTP 200, and HTTP error statuses; preserves
    # upstream correlation ids and returns a normalized envelope.
    status = resp.status
    headers = getattr(resp, "headers", None) or {}
    request_id, root_activity_id = _extract_correlation(headers)
    content_type = (_header_get(headers, "Content-Type") or "").lower()
    retry_after_seconds, retry_after_raw = _parse_retry_after(_header_get(headers, "Retry-After"))
    # Per MCP 2025-06-18 a stateful server returns an Mcp-Session-Id on
    # initialize that must be echoed on later requests (case-insensitive).
    session_id = _header_get(headers, "Mcp-Session-Id")

    # diagnostics carry ONLY safe data (status, retry-after, correlation ids,
    # operation, content-type). NEVER tool arguments, answers, or business data.
    diagnostics = {"operation": operation, "httpStatus": status}
    if content_type:
        diagnostics["contentType"] = content_type
    if request_id:
        diagnostics["requestId"] = request_id
    if root_activity_id:
        diagnostics["rootActivityId"] = root_activity_id
    if retry_after_seconds is not None:
        diagnostics["retryAfterSeconds"] = retry_after_seconds
    elif retry_after_raw is not None:
        # Neither delta-seconds nor a parseable HTTP-date: keep the raw value
        # under a distinct key so it is never mislabeled as a seconds delay.
        diagnostics["retryAfter"] = retry_after_raw

    # Read the body exactly once, then release the response so its connection
    # returns to the shared pool. NEVER close the shared session here.
    try:
        try:
            body_text = await resp.text()
        except (UnicodeDecodeError, LookupError):
            # A body that cannot be decoded (bad charset / invalid bytes) must
            # not escape as an unhandled error; surface the structured envelope.
            # `resp.release()` in the enclosing finally still runs exactly once.
            env = _error_envelope(
                "UpstreamError", "Unparseable upstream response", source="upstream",
                retryable=True, userError=False, httpStatus=status,
                taskId=known_task_id, invocationId=invocation_id,
                requestId=request_id, rootActivityId=root_activity_id,
                diagnostics=diagnostics,
            )
            return {"kind": "error", "envelope": env}
    finally:
        await resp.release()

    if status >= 400:
        if status in _HTTP_ERROR_MAP:
            code, retryable, user_err = _HTTP_ERROR_MAP[status]
        elif 500 <= status <= 599:
            code, retryable, user_err = ("UpstreamError", True, False)
        else:
            code, retryable, user_err = ("HttpError", False, True)
        env = _error_envelope(
            code, f"Upstream returned HTTP {status}", source="upstream",
            retryable=retryable, userError=user_err, httpStatus=status,
            taskId=known_task_id, invocationId=invocation_id,
            requestId=request_id, rootActivityId=root_activity_id,
            diagnostics=diagnostics,
        )
        return {"kind": "error", "envelope": env}

    if status == 202:
        # Bare ACK when the body is empty/non-JSON; a JSON body is a real task
        # response and is parsed like the JSON case below. Never invent a taskId.
        parsed = _try_json(body_text) if (body_text and body_text.strip()) else None
        if parsed is None:
            ack = {"status": "accepted"}
            if known_task_id:
                ack["taskId"] = known_task_id
            return {"kind": "ack", "ack": ack, "requestId": request_id,
                    "rootActivityId": root_activity_id, "sessionId": session_id}
        msg = parsed
    elif "text/event-stream" in content_type:
        msg = _parse_sse(body_text)
    else:
        msg = _try_json(body_text)

    if not isinstance(msg, dict):
        env = _error_envelope(
            "UpstreamError", "Unparseable upstream response", source="upstream",
            retryable=True, userError=False, httpStatus=status,
            taskId=known_task_id, invocationId=invocation_id,
            requestId=request_id, rootActivityId=root_activity_id,
            diagnostics=diagnostics,
        )
        return {"kind": "error", "envelope": env}

    if msg.get("error") is not None:
        # A JSON-RPC error is an error even when the HTTP status is 200.
        rpc_err = msg.get("error")
        cause = rpc_err.get("code") if isinstance(rpc_err, dict) else None
        env = _error_envelope(
            "UpstreamError", "Upstream returned a JSON-RPC error", source="upstream",
            retryable=False, userError=False, httpStatus=status,
            taskId=known_task_id, invocationId=invocation_id,
            requestId=request_id, rootActivityId=root_activity_id,
            causeCode=(str(cause) if cause is not None else None),
            diagnostics=diagnostics,
        )
        return {"kind": "error", "envelope": env}

    result = msg.get("result") if ("result" in msg) else msg
    return {"kind": "result", "result": result, "requestId": request_id,
            "rootActivityId": root_activity_id, "sessionId": session_id}


def _finish(handled, invocation_id, known_task_id):
    kind = handled.get("kind")
    if kind == "error":
        env = handled["envelope"]
        env["error"].setdefault("invocationId", invocation_id)
        return env
    if kind == "ack":
        return handled["ack"]
    return _shape_result(handled.get("result"))


async def _op_get_info(session, url, headers, invocation_id):
    # getInfo performs the MCP 2025-06-18 lifecycle: initialize ->
    # notifications/initialized -> tools/list. A stateful server returns an
    # Mcp-Session-Id on initialize that must be echoed on every later request,
    # and expects the initialized notification before normal requests.
    init_params = {
        "protocolVersion": _MCP_PROTOCOL_VERSION,
        "clientInfo": {"name": _MCP_CLIENT_NAME, "version": _MCP_CLIENT_VERSION},
        "capabilities": {},
    }
    resp1 = await session.post(url, json=_jsonrpc_body("initialize", init_params),
                               headers=headers, timeout=_AIHUB_REQUEST_TIMEOUT)
    handled1 = await _read_mcp_response(resp1, "getInfo", invocation_id, None)
    if handled1.get("kind") != "result":
        return _finish(handled1, invocation_id, None)
    init_result = handled1.get("result") or {}

    # Echo the server-assigned session id (if any) on all subsequent requests.
    session_id = handled1.get("sessionId")
    call_headers = dict(headers)
    if session_id:
        call_headers["Mcp-Session-Id"] = session_id

    # Signal readiness with the initialized NOTIFICATION (no id, no response).
    # Best-effort: a failure here must not fail getInfo.
    try:
        notif_resp = await session.post(
            url, json=_jsonrpc_body("notifications/initialized", None, notification=True),
            headers=call_headers, timeout=_AIHUB_REQUEST_TIMEOUT)
        release = getattr(notif_resp, "release", None)
        if release is not None:
            await release()
    except (asyncio.TimeoutError, aiohttp.ClientError):
        pass

    resp2 = await session.post(url, json=_jsonrpc_body("tools/list", {}),
                               headers=call_headers, timeout=_AIHUB_REQUEST_TIMEOUT)
    handled2 = await _read_mcp_response(resp2, "getInfo", invocation_id, None)
    if handled2.get("kind") != "result":
        return _finish(handled2, invocation_id, None)
    tools_result = handled2.get("result") or {}

    # Preserve every field both responses returned. `tools` is authoritative from
    # tools/list; all other tools/list fields are merged non-destructively so a
    # value colliding with an initialize field is kept (never dropped) under
    # out["_collisions"] rather than silently discarded.
    out = dict(init_result) if isinstance(init_result, dict) else {}
    if isinstance(tools_result, dict):
        tools_list = tools_result.get("tools", [])
        overlay = {k: v for k, v in tools_result.items() if k != "tools"}
        _merge_preserving(out, overlay, "toolsList")
        if "tools" in out and out["tools"] != tools_list:
            out.setdefault("_collisions", {}).setdefault("initialize", {})["tools"] = out["tools"]
        out["tools"] = tools_list
    else:
        out.setdefault("tools", [])
    logging.info("rayfin_fabric_aihub_v1 end op=getInfo invocationId=%s httpStatus=200", invocation_id)
    return out


async def _op_start_task(session, url, headers, invocation_id, input_data):
    tool_name = input_data.get("toolName")
    if not tool_name or not str(tool_name).strip():
        logging.warning("rayfin_fabric_aihub_v1 missing toolName op=startTask invocationId=%s httpStatus=400 retryable=False", invocation_id)
        return _error_envelope("MissingInput", "toolName is required", source="connector",
                               retryable=False, userError=True, httpStatus=400, invocationId=invocation_id)
    arguments = input_data.get("arguments")
    if arguments is None:
        arguments = {}
    ttl = input_data.get("ttl")
    # Augment tools/call for tasks: a task request (with ttl when supplied) asks
    # the server to create a task. artifactId, if present, stays inside arguments.
    params = {"name": tool_name, "arguments": arguments, "task": {}}
    if ttl is not None:
        params["task"]["ttl"] = ttl
    resp = await session.post(url, json=_jsonrpc_body("tools/call", params),
                              headers=headers, timeout=_AIHUB_REQUEST_TIMEOUT)
    handled = await _read_mcp_response(resp, "startTask", invocation_id, None)
    logging.info("rayfin_fabric_aihub_v1 end op=startTask invocationId=%s", invocation_id)
    # _finish returns task metadata when a task was created, or the immediate
    # tool result (content/isError/structuredContent) when the server answered
    # inline -- both losslessly.
    return _finish(handled, invocation_id, None)


async def _op_task_by_id(session, url, headers, invocation_id, input_data, method):
    task_id = input_data.get("taskId")
    if not task_id or not str(task_id).strip():
        logging.warning("rayfin_fabric_aihub_v1 missing taskId method=%s invocationId=%s httpStatus=400 retryable=False", method, invocation_id)
        return _error_envelope("MissingInput", "taskId is required", source="connector",
                               retryable=False, userError=True, httpStatus=400, invocationId=invocation_id)
    resp = await session.post(url, json=_jsonrpc_body(method, {"taskId": task_id}),
                              headers=headers, timeout=_AIHUB_REQUEST_TIMEOUT)
    handled = await _read_mcp_response(resp, method, invocation_id, task_id)
    logging.info("rayfin_fabric_aihub_v1 end method=%s invocationId=%s", method, invocation_id)
    return _finish(handled, invocation_id, task_id)


@udf.generic_connection(argName="fabricClient", audienceType="Fabric")
@udf.function()
async def rayfin_fabric_aihub_v1(payload: dict, fabricClient: fn.FabricItem) -> dict:
    # SAFE LOGGING RULE: only operation name, invocationId, requestId,
    # rootActivityId, httpStatus and retryable may ever be logged. NEVER log tool
    # arguments, MCP content, questions/answers, business data, user email/OID,
    # or the token/Authorization header.
    invocation_id = str(uuid.uuid4())
    # Validate caller-controlled types BEFORE any attribute access or logging so
    # a malformed payload returns a structured envelope instead of raising
    # AttributeError, and so no unsanitized caller value is logged.
    if not isinstance(payload, dict):
        logging.warning("rayfin_fabric_aihub_v1 rejected non-dict payload invocationId=%s httpStatus=400 retryable=False", invocation_id)
        return _error_envelope("BadRequest", "payload must be a JSON object",
                               source="connector", retryable=False, userError=True,
                               httpStatus=400, invocationId=invocation_id)
    raw_operation = payload.get("operation")
    if raw_operation is None:
        operation = "getInfo"
    elif isinstance(raw_operation, str):
        operation = raw_operation.strip()
    else:
        logging.warning("rayfin_fabric_aihub_v1 rejected non-string operation invocationId=%s httpStatus=400 retryable=False", invocation_id)
        return _error_envelope("BadRequest", "operation must be a string",
                               source="connector", retryable=False, userError=True,
                               httpStatus=400, invocationId=invocation_id)
    input_data = payload.get("input")
    if not isinstance(input_data, dict):
        input_data = {}

    # Sanitize the caller-controlled operation once; use it for every log line
    # and user-facing message so embedded CR/LF cannot forge log entries.
    safe_operation = _sanitize_for_log(operation)
    logging.info("rayfin_fabric_aihub_v1 start op=%s invocationId=%s", safe_operation, invocation_id)

    # Fixed-target enforcement (security-critical): caller input must NEVER
    # influence the URL. Reject any target-like override before doing anything.
    override_key = _detect_target_override(input_data)
    if override_key is not None:
        logging.warning("rayfin_fabric_aihub_v1 rejected target override op=%s invocationId=%s httpStatus=400 retryable=False", safe_operation, invocation_id)
        return _error_envelope(
            "InvalidTargetOverride",
            f"Caller may not override the target endpoint (key '{_sanitize_for_log(override_key)}')",
            source="connector", retryable=False, userError=True, httpStatus=400,
            invocationId=invocation_id,
        )

    url = _FABRIC_AIHUB_MCP_URL
    # Delegated Fabric OBO seam: the generic connection yields a pre-minted
    # Fabric-audience bearer. Never returned, logged, or echoed.
    token = fabricClient.get_access_token().get_token().token
    headers = _mcp_headers(url, invocation_id, token)
    session = await _get_session()

    try:
        if operation == "getInfo":
            return await _op_get_info(session, url, headers, invocation_id)
        if operation == "startTask":
            return await _op_start_task(session, url, headers, invocation_id, input_data)
        if operation == "getTask":
            return await _op_task_by_id(session, url, headers, invocation_id, input_data, "tasks/get")
        if operation == "getTaskResult":
            return await _op_task_by_id(session, url, headers, invocation_id, input_data, "tasks/result")
        if operation == "cancelTask":
            return await _op_task_by_id(session, url, headers, invocation_id, input_data, "tasks/cancel")
        logging.warning("rayfin_fabric_aihub_v1 unsupported op=%s invocationId=%s httpStatus=400 retryable=False", safe_operation, invocation_id)
        return _error_envelope("UnsupportedOperation", f"Unsupported operation '{safe_operation}'",
                               source="connector", retryable=False, userError=True,
                               httpStatus=400, invocationId=invocation_id)
    except asyncio.TimeoutError:
        logging.error("rayfin_fabric_aihub_v1 timeout op=%s invocationId=%s retryable=True", safe_operation, invocation_id)
        return _error_envelope("Timeout", "Upstream request timed out", source="upstream",
                               retryable=True, userError=False, httpStatus=408, invocationId=invocation_id)
    except aiohttp.ClientError:
        logging.error("rayfin_fabric_aihub_v1 network error op=%s invocationId=%s retryable=True", safe_operation, invocation_id)
        return _error_envelope("UpstreamError", "Upstream request failed", source="upstream",
                               retryable=True, userError=False, invocationId=invocation_id)
