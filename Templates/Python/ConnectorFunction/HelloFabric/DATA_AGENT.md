# Fabric Data Agent adapter (experimental)

`rayfin_data_agent_v1(payload, fabricClient)` uses the Fabric generic connection and a connector-neutral `_mcp_*` section in `function_app.py`.
No extra module, wheel, or package is required.
The SDK owns JSON-RPC message construction, bounded JSON/SSE decoding, response-ID and shape validation, discovery, history formatting, task selection, and error classification.
See the [SDK README](https://github.com/microsoft/project-rayfin/blob/main/packages/typescript-sdk/connector-fabric-dataagent/README.md) for the app API, errors, and migration guidance.

| Operation | Input, in addition to backend-injected `workspaceId` and `itemId` | MCP exchange |
| --- | --- | --- |
| `getInfo` | JSON-RPC `message` | SDK reads `initialize` and `tools/list` separately and caches their metadata |
| `startTask` | JSON-RPC `message` | `tools/call` with SDK-composed arguments and task option; raw task or immediate result |
| `getTask` | JSON-RPC `message` | `tasks/get` |
| `getTaskResult` | JSON-RPC `message` | `tasks/result` |
| `cancelTask` | JSON-RPC `message` | `tasks/cancel` |

Every invocation initializes MCP and attempts session `DELETE` in `finally`.
There are no automatic retries.
The requested response retains upstream status, content type, and streamed body without an envelope; the adapter does not filter or forward arbitrary response headers.
Only the Fabric JSON initialization handshake is consumed in Python to establish the session and send the initialized notification; requested response parsing lives in the SDK.
The message method must match the authorized outer connector operation, preventing a `getInfo` caller from submitting `tools/call`.
**Breaking contract:** `history` is rejected even when empty; older flat-field requests without a JSON-RPC `message` are also rejected.
Upgrade the SDK and adapter together.

## Endpoint and environment

The endpoint is `{origin}/v1/mcp/workspaces/{workspaceId}/dataagents/{itemId}/agent`.
The deployment's `POWERBI_API_BASE` uses a Power BI host, not a Fabric host.
An explicit HTTPS-origin map chooses the MCP origin; unsupported origins fail with HTTP 503 before obtaining a token.

| Power BI host | Fabric MCP origin | Ring |
| --- | --- | --- |
| `powerbiapi.analysis-df.windows.net` | `https://powerbiapi.analysis-df.windows.net` | TEST/EDOG; this branch's default |
| `dailyapi.powerbi.com` | `https://dailyapi.fabric.microsoft.com` | Daily |
| `dxtapi.powerbi.com` | `https://dxtapi.fabric.microsoft.com` | DXT/BCDR |
| `api.powerbi.com` | `https://api.fabric.microsoft.com` | MSIT and production |

MSIT and production have the same configured Power BI origin, so this setting cannot select `msitapi.fabric.microsoft.com` separately.
MSIT routing through the shared Fabric origin and DXT reachability must be confirmed with real agents before promotion; changing the map alone is not evidence of ring availability.
Redirects and caller-supplied routing/authentication values cannot change that endpoint.
Backend authorization and routing checks remain authoritative; the adapter keeps URL-path GUID validation.

Transport work is bounded to 230 seconds with up to 5 seconds for cleanup, below the documented 240-second UDF limit and BaaS's five-minute deadline.
The template dependency pins are unchanged; ring-specific package/runtime validation and automatic provisioning remain release prerequisites.
Binding pattern: `PYTHON/VariableLibrary/chat_completion_with_azure_openai.py`.

Run `python -B test_data_agent.py`; set `DATA_AGENT_BASELINE_DIR` to an exported base template directory to include peer/archive preservation checks without depending on git refs.
