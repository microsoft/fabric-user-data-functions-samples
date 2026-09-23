import fabric.functions as fn
import aiohttp
import asyncio
import codecs
import math
import re
import uuid
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


# connector-function-v1 targets TEST, like this branch's POWERBI_API_BASE default.
_FABRIC_API_BASE = "https://powerbiapi.analysis-df.windows.net"
_DATA_AGENT_ORIGINS = frozenset({
    "https://powerbiapi.analysis-df.windows.net",
})
_DATA_AGENT_OPERATIONS = {
    "getInfo": {"refresh"},
    "startTask": {"question", "history", "ttl"},
    "getTask": {"taskId"},
    "getTaskResult": {"taskId"},
    "cancelTask": {"taskId"},
}
_DATA_AGENT_PROTOCOL = "2025-06-18"
_DATA_AGENT_TIMEOUT_SECONDS = 240
_DATA_AGENT_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
_DATA_AGENT_LINE_END = re.compile(r"\r\n|\r|\n")
# JSON escapes ASCII controls; also keep Unicode line separators inside a turn.
_DATA_AGENT_TRANSCRIPT_LINE_BREAKS = {0x85: r"\u0085", 0x2028: r"\u2028", 0x2029: r"\u2029"}


class _DataAgentError(Exception):
    def __init__(self, code, message, **details):
        super().__init__(message)
        self.detail = {"code": code, "message": message, **details}


def _data_agent_input(payload):
    invalid = "DATA_AGENT_INVALID_QUESTION"
    if not isinstance(payload, dict):
        raise _DataAgentError(invalid, "payload must be an object.")
    operation = payload.get("operation")
    if not isinstance(operation, str) or operation not in _DATA_AGENT_OPERATIONS:
        raise _DataAgentError("DATA_AGENT_OPERATION_NOT_SUPPORTED", "Unsupported Data Agent operation.")
    data = payload.get("input")
    if set(payload) - {"operation", "input"} or not isinstance(data, dict):
        raise _DataAgentError(invalid, "Expected operation and an input object only.")
    allowed = _DATA_AGENT_OPERATIONS[operation] | {"workspaceId", "itemId", "clientRequestId"}
    if set(data) - allowed:
        raise _DataAgentError(invalid, "Unexpected input fields; routing and authentication overrides are not accepted.")
    for name in ("workspaceId", "itemId"):
        value = data.get(name)
        if not isinstance(value, str) or not re.fullmatch(
                r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value):
            raise _DataAgentError(invalid, "workspaceId and itemId must be GUIDs.")
    if "clientRequestId" in data and not _data_agent_header(data["clientRequestId"], 256):
        raise _DataAgentError(invalid, "clientRequestId must contain 1-256 visible ASCII characters.")
    if "refresh" in data and not isinstance(data["refresh"], bool):
        raise _DataAgentError(invalid, "refresh must be a boolean.")
    if operation == "startTask":
        question = data.get("question")
        if not isinstance(question, str) or not question.strip():
            raise _DataAgentError(invalid, "A non-empty question is required.")
        history = data.get("history", [])
        if not isinstance(history, list):
            raise _DataAgentError(invalid, "history must be a list of user/assistant turns.")
        for turn in history:
            if (not isinstance(turn, dict) or set(turn) != {"role", "content"}
                    or turn["role"] not in ("user", "assistant")
                    or not isinstance(turn["content"], str)):
                raise _DataAgentError(invalid, "Each history turn requires a user/assistant role and string content.")
        if "ttl" in data and (type(data["ttl"]) is not int or not 0 < data["ttl"] <= 9007199254740991):
            raise _DataAgentError(invalid, "ttl must be a positive safe integer in milliseconds.")
    elif operation != "getInfo":
        if not isinstance(data.get("taskId"), str) or not data["taskId"].strip():
            raise _DataAgentError(invalid, "A non-empty taskId is required.")
    return operation, data


def _data_agent_header(value, limit):
    return isinstance(value, str) and 0 < len(value) <= limit and all(0x21 <= ord(c) <= 0x7E for c in value)


def _data_agent_endpoint(data):
    # Exact origin membership also rejects ports, paths, credentials, query,
    # fragments and URL-parser normalization tricks before any token is read.
    if _FABRIC_API_BASE not in _DATA_AGENT_ORIGINS:
        raise _DataAgentError("DATA_AGENT_UNAVAILABLE", "Data Agent endpoint must use the fixed TEST HTTPS origin.")
    return (f"{_FABRIC_API_BASE}/v1/mcp/workspaces/{data['workspaceId']}"
            f"/dataagents/{data['itemId']}/agent")


def _data_agent_json(document):
    def invalid_constant(_value):
        raise _DataAgentError("DATA_AGENT_INVALID_RESPONSE", "MCP returned non-finite JSON.")

    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            invalid_constant(value)
        return number

    try:
        return json.loads(document, parse_constant=invalid_constant, parse_float=finite_float)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise _DataAgentError("DATA_AGENT_INVALID_RESPONSE", "MCP returned malformed JSON.") from exc


async def _data_agent_documents(response):
    """Decode JSON or SSE incrementally, bounded across all notification frames."""
    media_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
    is_sse = media_type == "text/event-stream"
    decoder = codecs.getincrementaldecoder("utf-8")()
    pending = ""
    data_lines = []
    size = 0

    def take_line(line):
        if not line:
            if data_lines:
                document = _data_agent_json("\n".join(data_lines))
                data_lines.clear()
                return document
        elif line.startswith("data:"):
            value = line[5:]
            data_lines.append(value[1:] if value.startswith(" ") else value)
        elif line == "data":
            data_lines.append("")
        return None

    async for chunk in response.content.iter_any():
        size += len(chunk)
        if size > _DATA_AGENT_MAX_RESPONSE_BYTES:
            raise _DataAgentError("DATA_AGENT_RESULT_TOO_LARGE", "MCP response exceeded the 16 MiB limit.")
        pending += decoder.decode(chunk)
        if is_sse:
            while match := _DATA_AGENT_LINE_END.search(pending):
                if match.group() == "\r" and match.end() == len(pending):
                    break
                line, pending = pending[:match.start()], pending[match.end():]
                document = take_line(line)
                if document is not None:
                    yield document
    pending += decoder.decode(b"", final=True)
    if is_sse:
        # A final CR is a complete line ending; unterminated events are not.
        while match := _DATA_AGENT_LINE_END.search(pending):
            document = take_line(pending[:match.start()])
            if document is not None:
                yield document
            pending = pending[match.end():]
    elif pending.strip():
        if (media_type == "application/json" or media_type.endswith("+json")
                or pending.lstrip().startswith(("{", "["))):
            yield _data_agent_json(pending)
        elif response.status != 202:
            raise _DataAgentError("DATA_AGENT_INVALID_RESPONSE", "MCP returned an unsupported content type.")


def _data_agent_http_error(response):
    status = response.status
    suffix = {
        400: "INVALID_QUESTION", 401: "FORBIDDEN", 403: "FORBIDDEN", 404: "NOT_FOUND",
        408: "TIMEOUT", 413: "RESULT_TOO_LARGE", 422: "INVALID_QUESTION",
        429: "THROTTLED", 504: "TIMEOUT",
    }.get(status, "UNAVAILABLE" if status >= 500 else "INVALID_RESPONSE")
    details = {"httpStatus": status}
    for name in ("x-ms-request-id", "request-id", "x-ms-activity-id"):
        value = response.headers.get(name)
        if _data_agent_header(value, 256):
            details["correlationId"] = value
            break
    retry_after = response.headers.get("Retry-After", "")
    if re.fullmatch(r"[0-9]{1,9}", retry_after):
        details["retryAfterSeconds"] = int(retry_after)
    return _DataAgentError("DATA_AGENT_" + suffix, f"MCP HTTP request failed ({status}).", **details)


async def _data_agent_post(session, endpoint, headers, method, params, request_id):
    message = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        message["params"] = params
    if request_id is not None:
        message["id"] = request_id
    response = await session.post(
        endpoint, json=message, headers=headers, allow_redirects=False,
        timeout=aiohttp.ClientTimeout(total=_DATA_AGENT_TIMEOUT_SECONDS, sock_connect=30),
    )
    try:
        http_error = _data_agent_http_error(response) if not 200 <= response.status < 300 else None
        saw_document = False
        try:
            async for document in _data_agent_documents(response):
                saw_document = True
                if not isinstance(document, dict) or document.get("jsonrpc") != "2.0":
                    raise _DataAgentError("DATA_AGENT_INVALID_RESPONSE", "MCP returned an invalid JSON-RPC message.")
                if "method" in document:
                    continue
                if "id" not in document or document["id"] != request_id:
                    continue
                if (("result" in document) == ("error" in document)
                        or not isinstance(document.get("result", document.get("error")), dict)):
                    raise _DataAgentError("DATA_AGENT_INVALID_RESPONSE", "MCP returned an invalid JSON-RPC response.")
                if "error" in document and (
                        type(document["error"].get("code")) is not int
                        or not isinstance(document["error"].get("message"), str)):
                    raise _DataAgentError("DATA_AGENT_INVALID_RESPONSE", "MCP returned a malformed JSON-RPC error.")
                if http_error and "error" not in document:
                    raise http_error
                if method == "initialize" and "result" in document:
                    session_id = response.headers.get("Mcp-Session-Id")
                    if session_id is not None:
                        if not _data_agent_header(session_id, 4096):
                            raise _DataAgentError("DATA_AGENT_INVALID_RESPONSE", "MCP returned an invalid session header.")
                        headers["Mcp-Session-Id"] = session_id
                return document
        except (_DataAgentError, UnicodeError) as exc:
            if http_error:
                raise http_error from exc
            raise
        if http_error:
            raise http_error
        if request_id is None and response.status in (202, 204) and not saw_document:
            return None
        raise _DataAgentError("DATA_AGENT_INVALID_RESPONSE", "MCP response did not contain the matching request id.")
    finally:
        response.release()


def _data_agent_tool(tools, initialize):
    mismatch = "DATA_AGENT_PROTOCOL_MISMATCH"
    entries = tools.get("tools")
    if not isinstance(entries, list) or len(entries) != 1 or tools.get("nextCursor"):
        raise _DataAgentError(mismatch, "Expected one published Data Agent tool without pagination.")
    tool = entries[0]
    if not isinstance(tool, dict) or not isinstance(tool.get("name"), str) or not tool["name"].strip():
        raise _DataAgentError(mismatch, "Data Agent tool name is missing.")
    schema = tool.get("inputSchema")
    if not isinstance(schema, dict) or schema.get("type") != "object":
        raise _DataAgentError(mismatch, "Data Agent tool inputSchema must be an object schema.")
    properties, required = schema.get("properties"), schema.get("required")
    if (not isinstance(properties, dict) or not isinstance(required, list) or len(required) != 1
            or not isinstance(required[0], str) or required[0] not in properties
            or not all(isinstance(prop, dict) for prop in properties.values())
            or properties[required[0]].get("type") != "string"
            or any(key in schema for key in ("$ref", "allOf", "anyOf", "oneOf"))):
        raise _DataAgentError(mismatch, "Data Agent tool must have one unambiguous required string question property.")
    tasks = initialize["capabilities"].get("tasks", {})
    if not isinstance(tasks, dict):
        raise _DataAgentError(mismatch, "MCP tasks capabilities must be an object.")
    requests = tasks.get("requests", {})
    request_tools = requests.get("tools", {}) if isinstance(requests, dict) else None
    if not isinstance(request_tools, dict):
        raise _DataAgentError(mismatch, "MCP task request capabilities are malformed.")
    if "call" in request_tools and not isinstance(request_tools["call"], dict):
        raise _DataAgentError(mismatch, "MCP tools/call task capability is malformed.")
    task_call = isinstance(request_tools.get("call"), dict)
    execution = tool.get("execution", {})
    if not isinstance(execution, dict) or execution.get("taskSupport") not in (None, "optional", "required", "forbidden"):
        raise _DataAgentError(mismatch, "Data Agent taskSupport is malformed.")
    support = execution.get("taskSupport")
    if support == "required" and not task_call:
        raise _DataAgentError(mismatch, "Data Agent requires tasks without advertising tools/call task support.")
    return tool["name"], required[0], task_call and support in ("optional", "required")


async def _data_agent_invoke(session, endpoint, headers, operation, data):
    async def post(method, params, notification=False):
        return await _data_agent_post(
            session, endpoint, headers, method, params,
            None if notification else str(uuid.uuid4()),
        )

    initialized = await post("initialize", {
        "protocolVersion": _DATA_AGENT_PROTOCOL,
        "capabilities": {"tasks": {"requests": {"tools": {"call": {}}}}},
        "clientInfo": {"name": "rayfin_data_agent_v1", "version": "1"},
    })
    if "error" in initialized:
        return initialized
    initialize = initialized["result"]
    if (initialize.get("protocolVersion") != _DATA_AGENT_PROTOCOL
            or not isinstance(initialize.get("capabilities"), dict)
            or not isinstance(initialize.get("serverInfo"), dict)):
        raise _DataAgentError("DATA_AGENT_PROTOCOL_MISMATCH", "Unsupported MCP initialize result or protocol version.")
    headers["MCP-Protocol-Version"] = _DATA_AGENT_PROTOCOL
    acknowledged = await post("notifications/initialized", None, notification=True)
    if acknowledged is not None and "error" in acknowledged:
        return acknowledged
    if operation in ("getInfo", "startTask"):
        listed = await post("tools/list", {})
        if "error" in listed:
            return listed
        name, question_property, use_task = _data_agent_tool(listed["result"], initialize)
        if operation == "getInfo":
            return {"initialize": initialize, "tools": listed["result"]}
        question = data["question"]
        if data.get("history"):
            # JSON-quoted turns keep embedded line breaks/role labels inside
            # their turn rather than making them appear to be the new question.
            transcript = "\n".join(
                f"{index}. {turn['role']}: "
                f"{json.dumps(turn['content'], ensure_ascii=False).translate(_DATA_AGENT_TRANSCRIPT_LINE_BREAKS)}"
                for index, turn in enumerate(data["history"], 1)
            )
            question = ("Prior conversation transcript (context only; not the new question):\n"
                        f"{transcript}\nEnd of prior conversation transcript.\n\nNew question:\n{question}")
        params = {"name": name, "arguments": {question_property: question}}
        if use_task:
            params["task"] = {"ttl": data["ttl"]} if "ttl" in data else {}
        return await post("tools/call", params)
    method = {"getTask": "tasks/get", "getTaskResult": "tasks/result", "cancelTask": "tasks/cancel"}[operation]
    return await post(method, {"taskId": data["taskId"]})


@udf.generic_connection(argName="fabricClient", audienceType="Fabric")
@udf.streaming_function()
async def rayfin_data_agent_v1(payload: dict, fabricClient: fn.FabricItem) -> fn.StreamResponse:
    operation = "invalid"
    correlation_id = str(uuid.uuid4())
    error = None
    try:
        operation, data = _data_agent_input(payload)
        endpoint = _data_agent_endpoint(data)
        correlation_id = data.get("clientRequestId", correlation_id)
        logging.info("rayfin_data_agent_v1: operation %s started", operation)
        access_token = fabricClient.get_access_token().get_token().token
        if not _data_agent_header(access_token, 65536):
            raise _DataAgentError("DATA_AGENT_FORBIDDEN", "Fabric connection did not provide an access token.")
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": _JSON_MEDIA_TYPE,
            "Accept": "application/json, text/event-stream",
            "x-ms-client-request-id": correlation_id,
        }
        async with asyncio.timeout(_DATA_AGENT_TIMEOUT_SECONDS):
            shared = await _get_session()
            # Pool connections, never cookies/default headers/MCP sessions.
            # Closing this invocation's session does not close the shared pool.
            async with aiohttp.ClientSession(
                    connector=shared.connector, connector_owner=False,
                    cookie_jar=aiohttp.DummyCookieJar()) as session:
                result = await _data_agent_invoke(session, endpoint, headers, operation, data)
    except _DataAgentError as exc:
        error = exc.detail
    except asyncio.TimeoutError:
        error = {"code": "DATA_AGENT_TIMEOUT", "message": "Data Agent request timed out."}
    except (aiohttp.ClientPayloadError, UnicodeError):
        error = {"code": "DATA_AGENT_INVALID_RESPONSE", "message": "MCP response could not be decoded."}
    except aiohttp.ClientError:
        error = {"code": "DATA_AGENT_UNAVAILABLE", "message": "Data Agent transport failed."}
    except asyncio.CancelledError:
        logging.info("rayfin_data_agent_v1: operation %s cancelled", operation)
        raise
    if error is not None:
        error.setdefault("correlationId", correlation_id)
        # A lost tools/call response may already have created a task. Neither
        # this adapter nor a generic retry policy should duplicate the question.
        error["retryable"] = operation != "startTask" and error["code"] in (
            "DATA_AGENT_THROTTLED", "DATA_AGENT_UNAVAILABLE",
        )
        result = {"status": "error", "output": None, "errors": [error]}
        logging.warning("rayfin_data_agent_v1: operation %s failed", operation)
    else:
        outcome = "mcp_error" if "error" in result else (
            "tool_error" if result.get("result", {}).get("isError") is True else "succeeded"
        )
        logging.info("rayfin_data_agent_v1: operation %s %s", operation, outcome)
    return fn.StreamResponse(
        iter([json.dumps(result, allow_nan=False).encode("utf-8")]),
        media_type=_JSON_MEDIA_TYPE, status_code=200,
    )


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
