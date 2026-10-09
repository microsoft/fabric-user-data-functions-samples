# Fabric Data Agent adapter (experimental)

`rayfin_data_agent_v1(payload, fabricClient)` uses the Fabric generic connection and a connector-neutral `_mcp_*` section in `function_app.py`.
No extra module, wheel, or package is required.
The SDK owns discovery policy, history formatting, task selection, and error classification.
See the [SDK README](https://github.com/microsoft/project-rayfin/blob/main/packages/typescript-sdk/connector-fabric-dataagent/README.md) for the app API, errors, and migration guidance.

| Operation | Input, in addition to backend-injected `workspaceId` and `itemId` | MCP exchange |
| --- | --- | --- |
| `getInfo` | None | `initialize`, notification, `tools/list`; returns raw initialization and tool metadata |
| `startTask` | `toolName`, `questionProperty`, composed `question`, boolean `useTask`; optional `ttl` | `tools/call` with the SDK-selected arguments and task option; returns either a task or immediate answer |
| `getTask` | `taskId` | `tasks/get` |
| `getTaskResult` | `taskId` | `tasks/result` |
| `cancelTask` | `taskId` | `tasks/cancel` |

Every invocation initializes MCP and attempts session `DELETE` in `finally`.
There are no automatic retries.
HTTP failures retain upstream status, body, and end-to-end response headers; hop-by-hop, cookie, session, and credential headers are not forwarded.
**Breaking contract:** `history` is rejected even when empty; older SDK requests without tool metadata are also rejected.
Upgrade the SDK and adapter together.

## Endpoint and environment

The endpoint is `{origin}/v1/mcp/workspaces/{workspaceId}/dataagents/{itemId}/agent`.
The origin is extracted from the deployment's `POWERBI_API_BASE`, never from the invocation payload, and must exactly match TEST, Daily, DXT, MSIT, or production hosts in `_DATA_AGENT_ORIGINS`.
This branch defaults to the TEST host `powerbiapi.analysis-df.windows.net`; other rings must set `POWERBI_API_BASE` appropriately.
Redirects and caller-supplied routing/authentication values cannot change that endpoint.
Backend authorization and routing checks remain authoritative; the adapter keeps URL-path GUID validation.

Transport work is bounded to 230 seconds with up to 5 seconds for cleanup, below the documented 240-second UDF limit and BaaS's five-minute deadline.
The template dependency pins are unchanged; ring-specific package/runtime validation and automatic provisioning remain release prerequisites.
Binding pattern: `PYTHON/VariableLibrary/chat_completion_with_azure_openai.py`.

Run `python -B test_data_agent.py`; set `DATA_AGENT_BASELINE_DIR` to an exported base template directory to include peer/archive preservation checks without depending on git refs.
