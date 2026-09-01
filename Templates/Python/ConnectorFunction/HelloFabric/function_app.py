import fabric.functions as fn
import aiohttp
import asyncio
import json
import logging
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
_MCP_PROFILES = {"daily": (_MCP_ENDPOINT, _MCP_VARIANTS)}
_MCP_REQUEST_LIMIT_BYTES = 5 * 1024 * 1024
_MCP_INVOKE_TIMEOUT_SECONDS = 5 * 60
_MCP_MAX_SSE_EVENTS = 256
_MCP_MAX_SSE_LINES_PER_EVENT = 8
_MCP_CONTRACT_ERROR_MESSAGE = "Invalid managed MCP request."
_MCP_CONFIGURATION_ERROR_MESSAGE = "Managed MCP limits are not configured."
_MCP_UPSTREAM_ERROR_MESSAGE = "Managed MCP upstream response is invalid."
_MCP_REMOTE_ERROR_MESSAGE = "Managed MCP upstream request failed."
_MCP_LOGGER = logging.getLogger(__name__)
_MCP_TELEMETRY_EVENTS = frozenset(
    ("upstream_response", "request_completed", "request_failed")
)
_MCP_TELEMETRY_FAILURES = frozenset(("transport", "protocol"))


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
    task_statuses: Tuple[str, ...]
    final_task_statuses: Tuple[str, ...]
    max_status_message_bytes: int
    max_routing_name_bytes: int
    min_poll_interval_ms: int
    max_poll_interval_ms: int
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


def _valid_routing_name(value: object, max_bytes: Optional[int] = None) -> bool:
    if type(value) is not str or not value or not value.isascii():
        return False
    if any(ord(character) < 0x21 or ord(character) > 0x7E for character in value):
        return False
    return max_bytes is None or len(value) <= max_bytes


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


def _validate_request_meta(meta: object) -> None:
    if (
        type(meta) is not dict
        or tuple(meta)
        != (
            "io.modelcontextprotocol/protocolVersion",
            "io.modelcontextprotocol/clientInfo",
            "io.modelcontextprotocol/clientCapabilities",
        )
        or meta["io.modelcontextprotocol/protocolVersion"] != _MCP_PROTOCOL_VERSION
    ):
        _raise_mcp_contract_error()
    client_info = meta["io.modelcontextprotocol/clientInfo"]
    capabilities = meta["io.modelcontextprotocol/clientCapabilities"]
    if (
        type(client_info) is not dict
        or tuple(client_info) != ("name", "version")
        or not _valid_routing_name(client_info["name"])
        or not _valid_routing_name(client_info["version"])
        or type(capabilities) is not dict
        or tuple(capabilities) != ("extensions",)
        or type(capabilities["extensions"]) is not dict
        or tuple(capabilities["extensions"]) != ("io.modelcontextprotocol/tasks",)
        or type(
            capabilities["extensions"]["io.modelcontextprotocol/tasks"]
        )
        is not dict
        or capabilities["extensions"]["io.modelcontextprotocol/tasks"]
    ):
        _raise_mcp_contract_error()


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
    if method == "server/discover":
        if tuple(params) != ("_meta",):
            _raise_mcp_contract_error()
    elif method == "tools/list":
        if tuple(params) not in (("_meta",), ("cursor", "_meta")):
            _raise_mcp_contract_error()
        if "cursor" in params and (
            type(params["cursor"]) is not str or not params["cursor"]
        ):
            _raise_mcp_contract_error()
    elif method == "tools/call":
        if tuple(params) != ("name", "arguments", "_meta"):
            _raise_mcp_contract_error()
        routing_name = params["name"]
        arguments = params["arguments"]
        if not _valid_routing_name(routing_name) or type(arguments) is not dict:
            _raise_mcp_contract_error()
    else:
        if tuple(params) != ("taskId", "_meta"):
            _raise_mcp_contract_error()
        routing_name = params["taskId"]
        if not _valid_routing_name(routing_name):
            _raise_mcp_contract_error()
    _validate_request_meta(params["_meta"])

    forwarded_params = dict(params)
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
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError):
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
        task_statuses = tuple(
            value
            for value in os.environ["FABRIC_MCP_TASK_STATUSES"].split(",")
            if value
        )
        final_task_statuses = tuple(
            value
            for value in os.environ["FABRIC_MCP_FINAL_TASK_STATUSES"].split(",")
            if value
        )
        max_status_message_bytes = int(
            os.environ["FABRIC_MCP_MAX_STATUS_MESSAGE_BYTES"]
        )
        max_routing_name_bytes = int(
            os.environ["FABRIC_MCP_MAX_ROUTING_NAME_BYTES"]
        )
        min_poll_interval_ms = int(os.environ["FABRIC_MCP_MIN_POLL_INTERVAL_MS"])
        max_poll_interval_ms = int(os.environ["FABRIC_MCP_MAX_POLL_INTERVAL_MS"])
        max_poll_count = int(os.environ["FABRIC_MCP_MAX_POLL_COUNT"])
    except (KeyError, TypeError, ValueError):
        raise _McpConfigurationError(_MCP_CONFIGURATION_ERROR_MESSAGE) from None
    limits = _McpManagedLimits(
        max_output_bytes,
        max_json_depth,
        allowed_status_codes,
        task_statuses,
        final_task_statuses,
        max_status_message_bytes,
        max_routing_name_bytes,
        min_poll_interval_ms,
        max_poll_interval_ms,
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
        or type(limits.task_statuses) is not tuple
        or type(limits.final_task_statuses) is not tuple
        or type(limits.max_status_message_bytes) is not int
        or type(limits.max_routing_name_bytes) is not int
        or type(limits.min_poll_interval_ms) is not int
        or type(limits.max_poll_interval_ms) is not int
        or type(limits.max_poll_count) is not int
        or limits.max_output_bytes <= 0
        or limits.max_json_depth <= 0
        or not limits.allowed_status_codes
        or any(
            type(status) is not int or status < 200 or status > 299
            for status in limits.allowed_status_codes
        )
        or not limits.task_statuses
        or any(not _valid_routing_name(status) for status in limits.task_statuses)
        or not limits.final_task_statuses
        or any(
            not _valid_routing_name(status) for status in limits.final_task_statuses
        )
        or not frozenset(limits.final_task_statuses).issubset(limits.task_statuses)
        or limits.max_status_message_bytes <= 0
        or limits.max_routing_name_bytes <= 0
        or limits.min_poll_interval_ms < 0
        or limits.max_poll_interval_ms < limits.min_poll_interval_ms
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


def _managed_mcp_profile() -> Tuple[str, str]:
    profile = _MCP_PROFILES.get(os.environ.get("FABRIC_MCP_PROFILE", "daily"))
    if profile is None:
        raise _McpConfigurationError(_MCP_CONFIGURATION_ERROR_MESSAGE) from None
    endpoint, variants = profile
    parsed = urlparse(endpoint)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "fabriciq.svc.cloud.microsoft"
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != "/v1/mcp/fabriciq"
        or parsed.params
        or parsed.query
        or parsed.fragment
        or variants != _MCP_VARIANTS
    ):
        raise _McpConfigurationError(_MCP_CONFIGURATION_ERROR_MESSAGE) from None
    return endpoint, variants


def _mcp_media_type(content_type: object) -> str:
    if type(content_type) is not str or not content_type:
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    parts = [part.strip().lower() for part in content_type.split(";")]
    media_type = parts[0]
    if media_type not in (_JSON_MEDIA_TYPE, "text/event-stream"):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    if len(parts) > 2 or (len(parts) == 2 and parts[1] != "charset=utf-8"):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    return media_type


def _emit_mcp_telemetry(
    event: str,
    *,
    method: Optional[str] = None,
    status_code: Optional[int] = None,
    transport: Optional[str] = None,
    failure: Optional[str] = None,
) -> None:
    if (
        event not in _MCP_TELEMETRY_EVENTS
        or (method is not None and method not in _MCP_ALLOWED_METHODS)
        or (
            status_code is not None
            and (type(status_code) is not int or not 100 <= status_code <= 599)
        )
        or (transport is not None and transport not in ("json", "sse"))
        or (failure is not None and failure not in _MCP_TELEMETRY_FAILURES)
    ):
        return
    fields = {
        key: value
        for key, value in (
            ("mcp_event", event),
            ("mcp_method", method),
            ("mcp_status_code", status_code),
            ("mcp_transport", transport),
            ("mcp_failure", failure),
        )
        if value is not None
    }
    _MCP_LOGGER.info("managed_mcp_operation", extra=fields)


def _validate_managed_mcp_response(
    content: bytes,
    content_type: str,
    request: _McpManagedRequest,
    request_id: object,
    limits: _McpManagedLimits,
) -> object:
    if not content:
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    if len(content) > limits.max_output_bytes:
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    try:
        decoded = content.decode("utf-8", errors="strict")
        media_type = _mcp_media_type(content_type)
        if media_type == _JSON_MEDIA_TYPE:
            message = json.loads(decoded)
        elif media_type == "text/event-stream":
            message = _extract_sse_jsonrpc_response(decoded, request_id)
        else:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
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
    if "error" in message:
        error = message["error"]
        if (
            type(error) is not dict
            or tuple(error) not in (("code", "message"), ("code", "message", "data"))
            or type(error["code"]) is not int
            or type(error["message"]) is not str
        ):
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": error["code"],
                "message": _MCP_REMOTE_ERROR_MESSAGE,
            },
        }

    result = message["result"]
    if request.method == "server/discover":
        _validate_server_discovery(result)
    elif request.method == "tools/list":
        if type(result) is not dict:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    elif request.method == "tools/call":
        if type(result) is not dict:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        if "resultType" not in result:
            _validate_sync_tool_result(result)
        elif result["resultType"] == "task":
            if tuple(result) != ("resultType", "task"):
                raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
            _validate_mcp_task(
                result["task"],
                None,
                limits,
                result_type="task",
                include_result_type=False,
            )
        else:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    elif request.method == "tasks/get":
        _validate_mcp_task(
            result,
            request.routing_name,
            limits,
            result_type=(
                result.get("resultType") if type(result) is dict else ""
            ),
            include_result_type=True,
        )
    elif (
        type(result) is not dict
        or tuple(result) != ("resultType",)
        or result["resultType"] != "complete"
    ):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    return message


def _extract_sse_jsonrpc_response(payload: str, request_id: object) -> object:
    if "\r" in payload.replace("\r\n", ""):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    normalized = payload.replace("\r\n", "\n")
    if not normalized.endswith("\n\n"):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    events = normalized[:-2].split("\n\n")
    if (
        not events
        or len(events) > _MCP_MAX_SSE_EVENTS
        or any(not event for event in events)
    ):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None

    matching_response = None
    for event in events:
        lines = event.split("\n")
        if len(lines) > _MCP_MAX_SSE_LINES_PER_EVENT:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        event_name = None
        data_lines = []
        for line in lines:
            if line.startswith("event:"):
                if event_name is not None:
                    raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
                event_name = line[6:].lstrip(" ")
                if event_name != "message":
                    raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip(" "))
            else:
                raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        if event_name != "message" or not data_lines:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        try:
            candidate = json.loads("\n".join(data_lines))
        except (json.JSONDecodeError, RecursionError):
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        if type(candidate) is not dict or candidate.get("jsonrpc") != "2.0":
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        if "id" in candidate:
            if (
                type(candidate["id"]) is not type(request_id)
                or candidate["id"] != request_id
            ):
                continue
            if matching_response is not None:
                raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
            matching_response = candidate
            continue
        if (
            set(candidate)
            not in (
                {"jsonrpc", "method"},
                {"jsonrpc", "method", "params"},
            )
            or type(candidate.get("method")) is not str
            or not candidate["method"].startswith("notifications/")
            or ("params" in candidate and type(candidate["params"]) is not dict)
        ):
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    if matching_response is None:
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    return matching_response


def _validate_server_discovery(result: object) -> None:
    if type(result) is not dict:
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    supported_versions = result.get("supportedVersions")
    capabilities = result.get("capabilities")
    if (
        type(supported_versions) is not list
        or _MCP_PROTOCOL_VERSION not in supported_versions
        or any(type(version) is not str for version in supported_versions)
        or type(capabilities) is not dict
        or type(capabilities.get("extensions")) is not dict
        or "io.modelcontextprotocol/tasks" not in capabilities["extensions"]
        or type(capabilities["extensions"]["io.modelcontextprotocol/tasks"])
        is not dict
    ):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None


def _validate_sync_tool_result(result: object) -> None:
    if (
        type(result) is not dict
        or tuple(result) not in (
            ("content", "isError"),
            ("content", "isError", "structuredContent"),
        )
        or type(result["content"]) is not list
        or type(result["isError"]) is not bool
        or (
            "structuredContent" in result
            and type(result["structuredContent"]) is not dict
        )
    ):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None


def _validate_mcp_task(
    result: object,
    expected_task_id: Optional[str],
    limits: _McpManagedLimits,
    result_type: str,
    include_result_type: bool,
) -> None:
    if type(result) is not dict:
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    allowed_fields = frozenset(
        (
            "resultType",
            "taskId",
            "status",
            "statusMessage",
            "pollIntervalMs",
            "createdAt",
            "lastUpdatedAt",
            "ttlMs",
            "result",
            "error",
        )
    )
    if (
        any(type(key) is not str or key not in allowed_fields for key in result)
        or (
            include_result_type
            and (
                type(result.get("resultType")) is not str
                or result["resultType"] != result_type
            )
        )
        or (not include_result_type and "resultType" in result)
        or type(result.get("taskId")) is not str
        or not _valid_routing_name(
            result["taskId"], limits.max_routing_name_bytes
        )
        or (
            expected_task_id is not None
            and result["taskId"] != expected_task_id
        )
        or type(result.get("status")) is not str
        or result["status"] not in limits.task_statuses
    ):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None

    status = result["status"]
    is_terminal = status in limits.final_task_statuses
    expected_result_type = "complete" if is_terminal else "task"
    if result_type != expected_result_type:
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    for timestamp_name in ("createdAt", "lastUpdatedAt"):
        if timestamp_name in result and (
            type(result[timestamp_name]) is not str or not result[timestamp_name]
        ):
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    if "ttlMs" in result and (
        type(result["ttlMs"]) is not int or result["ttlMs"] < 0
    ):
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    if "statusMessage" in result:
        status_message = result["statusMessage"]
        if status_message is None:
            status_message_size = 0
        elif type(status_message) is not str:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        else:
            try:
                status_message_size = len(
                    status_message.encode("utf-8", errors="strict")
                )
            except UnicodeEncodeError:
                raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        if status_message_size > limits.max_status_message_bytes:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    if "pollIntervalMs" in result:
        poll_interval_ms = result["pollIntervalMs"]
        if (
            type(poll_interval_ms) is not int
            or poll_interval_ms < limits.min_poll_interval_ms
            or poll_interval_ms > limits.max_poll_interval_ms
        ):
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    has_result = "result" in result
    has_error = "error" in result
    if status == "completed":
        valid_payload = has_result and not has_error and result["result"] is not None
    elif status == "failed":
        valid_payload = (
            has_error
            and not has_result
            and type(result["error"]) is dict
        )
    else:
        valid_payload = not has_result and not has_error
    if not valid_payload:
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None


class _ManagedMcpResponseOwner:
    def __init__(self, response: aiohttp.ClientResponse):
        self._response = response

    def release(self) -> None:
        response = self._response
        if response is None:
            return
        response.release()
        self._response = None

    def release_with_retry(self) -> None:
        try:
            self.release()
        except BaseException as release_error:
            try:
                self.release()
            except BaseException as retry_error:
                response = self._response
                try:
                    response.close()
                except BaseException as close_error:
                    raise release_error from close_error
                self._response = None
                raise release_error from retry_error
            raise


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
    if request.routing_name is not None and not _valid_routing_name(
        request.routing_name,
        limits.max_routing_name_bytes,
    ):
        _raise_mcp_contract_error()
    if type(access_token) is not str or not access_token:
        raise _McpConfigurationError(_MCP_CONFIGURATION_ERROR_MESSAGE) from None
    endpoint, variants = _managed_mcp_profile()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": _JSON_MEDIA_TYPE,
        "Accept": f"{_JSON_MEDIA_TYPE},text/event-stream",
        "MCP-Protocol-Version": _MCP_PROTOCOL_VERSION,
        "Mcp-Method": request.method,
        "X-Variants": variants,
    }
    if request.routing_name is not None:
        headers["Mcp-Name"] = request.routing_name

    try:
        response = await session.post(
            endpoint,
            data=request.body_bytes,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=_MCP_INVOKE_TIMEOUT_SECONDS),
            allow_redirects=False,
        )
    except (aiohttp.ClientError, asyncio.TimeoutError):
        _emit_mcp_telemetry(
            "request_failed", method=request.method, failure="transport"
        )
        raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
    owner = _ManagedMcpResponseOwner(response)
    try:
        if response.status not in limits.allowed_status_codes:
            raise _McpUpstreamError(_MCP_UPSTREAM_ERROR_MESSAGE) from None
        media_type = _mcp_media_type(response.headers.get("Content-Type", ""))
        _emit_mcp_telemetry(
            "upstream_response",
            method=request.method,
            status_code=response.status,
            transport="json" if media_type == _JSON_MEDIA_TYPE else "sse",
        )
        content = await _read_bounded_mcp_response(
            response,
            limits.max_output_bytes,
        )
        message = _validate_managed_mcp_response(
            content,
            response.headers.get("Content-Type", ""),
            request,
            payload["message"]["id"],
            limits,
        )
        _emit_mcp_telemetry("request_completed", method=request.method)
    except _McpUpstreamError:
        _emit_mcp_telemetry(
            "request_failed", method=request.method, failure="protocol"
        )
        raise
    finally:
        owner.release_with_retry()
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
