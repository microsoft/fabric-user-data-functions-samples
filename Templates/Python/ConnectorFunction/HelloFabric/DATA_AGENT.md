# Fabric Data Agent connector adapter

`rayfin_data_agent_v1` implements the experimental `fabric-dataagent` / `FabricDataAgent` connector, function slug `data_agent`, version `1`.
It shares the HelloFabric UDF app and HTTP connection pool without changing the other adapters.
CLI authoring remains held; this adapter does not enable a rollout or establish a production deployment.

## Invocation contract

The public signature is `rayfin_data_agent_v1(payload: dict, fabricClient: fn.FabricItem) -> fn.StreamResponse`.
The `Fabric` generic connection supplies `fabricClient`; it is not an app parameter.
Only `payload` appears in the function metadata parameters.
The backend must replace caller routing with the connector's server-owned `workspaceId` and `itemId` before invocation.
GUID validation in this adapter is not a substitute for backend authorization and operation allow-lists.

```json
{
  "operation": "startTask",
  "input": {
    "workspaceId": "11111111-1111-4111-8111-111111111111",
    "itemId": "22222222-2222-4222-8222-222222222222",
    "question": "Summarize sales",
    "history": [
      {"role": "user", "content": "Use last quarter"},
      {"role": "assistant", "content": "Understood"}
    ],
    "ttl": 60000
  }
}
```

The example GUIDs are synthetic.
Unknown fields, caller endpoints, tokens, headers, session IDs, and the legacy `action` discriminator are rejected.
`ask` is a browser-side SDK helper, never an adapter operation.
There is no `executeQuery` bridge.

| Operation | Additional input | MCP exchange after initialization |
| --- | --- | --- |
| `getInfo` | Optional boolean `refresh` | `tools/list`; returns `{initialize, tools}` results |
| `startTask` | Required nonblank `question`; optional `history`, `ttl` | `tools/list`, then one `tools/call` |
| `getTask` | Required nonblank opaque `taskId` | `tasks/get` |
| `getTaskResult` | Required nonblank opaque `taskId` | `tasks/result` |
| `cancelTask` | Required nonblank opaque `taskId` | `tasks/cancel` |

All operations also accept an optional `clientRequestId` of 1-256 visible ASCII characters; otherwise the adapter generates one.
`refresh` is validated but makes no difference because there is no adapter metadata cache.
`ttl` is a positive JavaScript-safe integer in milliseconds and is sent only when task augmentation is negotiated.
Task IDs are not interpreted as GUIDs.

History is ordered and included before the new question as a labelled transcript, with each turn's content JSON-quoted to distinguish embedded newlines from transcript labels.
Non-ASCII multilingual text remains readable, while quotes, backslashes, ASCII controls, and Unicode line separators U+0085/U+2028/U+2029 remain escaped within each turn.
Only `user` and `assistant` string-content turns are accepted.
Without history, the question is passed unchanged.
This formatting distinguishes prior context; it is not a prompt-injection security boundary.

## Deployment environment and authentication

This template targets `connector-function-v1`, the TEST deployment branch, rather than `connector-function-daily`.
Its fixed endpoint origin is `https://powerbiapi.analysis-df.windows.net`, matching this branch's existing `POWERBI_API_BASE` origin.
Normal TEST deployment needs no new environment variable.
It must supply the TEST/PPE Fabric-audience credential (`https://analysis.windows-int.net/powerbi/api`) through the generic binding.
The adapter neither acquires an all-commercial token itself nor changes a token's audience.

The adapter has no environment-variable routing override and does not read `FABRIC_API_BASE`.
Changing the existing semantic-model `POWERBI_API_BASE` setting does not change this adapter's endpoint.
Only this exact origin is accepted, with no trailing slash, port, path, credentials, query, fragment, or whitespace:

```text
https://powerbiapi.analysis-df.windows.net
```

Production, daily, DXT, MSIT, and OneBox-specific origins are not accepted by this test-branch adapter.
OneBox deployment is not established: its effective connector-template branch and endpoint routing must be confirmed separately.
No inheritance from the TEST rollout or automatic substitution of a OneBox endpoint is assumed.
This template does not change the approved onebox/test-only rollout.

The adapter constructs the data-plane URL itself:

```text
https://powerbiapi.analysis-df.windows.net/v1/mcp/workspaces/{workspaceId}/dataagents/{itemId}/agent
```

Origin validation occurs before token acquisition or network access.
Every POST disables redirects.
The credential is obtained using `fabricClient.get_access_token().get_token().token`, matching the connector token-credential pattern.
It must contain the TEST/PPE Fabric-audience token supplied by the host.
No access token is accepted in `payload`, stored globally, or logged.

Each invocation creates a cookie-disabled HTTP session over `_get_session()`'s existing connector with `connector_owner=False`.
This reuses TCP connections, not user credentials, cookies, default headers, or MCP session IDs.
An MCP session header returned by initialization is reused only within that invocation.
There is no persistent task, conversation, or user cache.

**Deployment requirement:** validate the runtime's `Fabric` generic binding and delegated token forwarding in the intended test environment.
Public generic-connection documentation describes owner identity; it does not prove production on-behalf-of behavior for this connector.
The host must supply the authorized user's Fabric token, a published agent, and server-owned routing.
The TEST branch default is usable without deployment environment injection.
Authorized TEST resources are still required to validate the live MCP route and generic binding; DXT/MSIT smoke results are not TEST deployment proof.
Tenant SSO policy and delegated authorization also require runtime validation.
These local tests do not establish live deployment, capacity/tenant prerequisites, or production authorization.

## Protocol and failures

Each invocation initializes protocol `2025-06-18`, advertises `tasks.requests.tools.call`, and sends `notifications/initialized`.
Other negotiated protocol versions fail explicitly rather than silently switching semantics.
Discovery requires exactly one tool, no continuation cursor, and one unambiguous required string question property in an object schema.
Optional properties are not guessed or populated.
Task augmentation requires both server capability `tasks.requests.tools.call` and tool `execution.taskSupport` of `optional` or `required`.
Otherwise the call returns the immediate answer; a required-task tool without server task capability fails explicitly.

JSON and SSE responses are decoded by matching the JSON-RPC request ID, ignoring notifications and unrelated IDs.
SSE parsing supports split UTF-8, multiline data, and LF/CRLF/CR frame endings.
An initialized notification can return a plain-text HTTP 202 acknowledgment.
JSON-bearing 202 responses are parsed, not discarded.
Responses are released synchronously in `finally`, including failures and cancellation.

Successful task operations and `startTask` return the server's JSON-RPC object without normalizing its fields.
MCP errors, tool `isError`, content, `structuredContent`, `_meta`, `artifactName`, `deepLinkUrl`, and canonical or legacy task fields remain intact.
HTTP errors carrying a matching valid JSON-RPC error are also relayed unchanged.
Other known transport, parsing, validation, or protocol failures return HTTP 200 with `{status: "error", output: null, errors: [...]}` so the SDK can classify them without backend non-2xx body sanitization.
Errors contain a known `DATA_AGENT_*` code and correlation ID, plus HTTP status and numeric `Retry-After` when available.
Upstream error-page bodies are neither returned nor logged.
Unexpected programming or credential-provider exceptions propagate to the host instead of being disguised as success.
Logs contain only the operation and outcome.

Network work has a 240-second invocation deadline and a 30-second socket-connect timeout.
Each upstream body, including SSE notification traffic, is limited to 16 MiB.
The final JSON result is buffered; this is not a token-by-token output stream.
There are no automatic retries, including after a session 404, and `startTask` adapter errors are marked nonretryable because the question may already have been accepted.
MCP errors remain raw, so callers must not blindly retry `tools/call` failures either.
The SDK owns polling and explicit `cancelTask`; cancelling an in-flight HTTP request releases resources but does not claim the server task stopped.
Server sessions are not deleted at invocation end, because task lifetime must not be shortened.
Cross-invocation task lookup therefore requires the service to associate tasks with the authorized principal rather than a retained client session.

## Local verification and packaging

Use Python 3.11 or later with the existing `aiohttp` dependency.
From this directory:

```powershell
python -B test_data_agent.py
python -B -W error::ResourceWarning -m unittest discover -p 'test_*.py'
python -B ..\..\..\..\.github\scripts\check_zip_permissions.py SourceCode.zip Deploy.zip
```

The adapter suite includes fake transports, loopback HTTP concurrency/cookie isolation, cancellation and deadline cleanup, peer regressions, archive content/permission preservation, and execution through Python's ZIP importer.
Loopback tests replace only the endpoint helper; exact origin validation, TEST routing without settings, and resistance to environment routing overrides are tested separately.
Peer regressions cover this branch's semantic-model and office365users adapters; no Kusto adapter or daily-only tests are imported into this branch.
Tests require Git and compare peer code, metadata, and archive members with `origin/connector-function-v1`, not the daily branch.
`SourceCode.zip` and `Deploy.zip` must embed the adjacent `function_app.py` and `fabric_lib/functions.metadata` with canonical LF line endings, matching Git's normalized source bytes.
Archive tests normalize Windows checkout line endings before comparing and require LF-only packed content.
Preserve all other archive members, ordering, timestamps, and permission attributes; do not package tests, new tooling, secrets, or repository instructions.

## Grounding

The binding adapts `PYTHON/VariableLibrary/chat_completion_with_azure_openai.py` from the official samples index.
The streaming decorator follows this branch's `rayfin_semantic_model_v1`; token-credential access follows the shipped `rayfin_kusto_v1` reference pattern, without bringing that daily-branch adapter into this template.
These patterns are combined only because the public generic-binding sample is not a streaming connector adapter.
The response contract follows the Data Agent SDK rather than the chat-completion sample's string result.

- [Official Python samples index](https://raw.githubusercontent.com/microsoft/fabric-user-data-functions-samples/refs/heads/main/PYTHON/samples-llms.txt)
- [Data Agent MCP endpoint and prerequisites](https://learn.microsoft.com/fabric/data-science/data-agent-mcp-server)
- [UDF programming model](https://learn.microsoft.com/fabric/data-engineering/user-data-functions/python-programming-model)
- [FabricItem credential API](https://learn.microsoft.com/python/api/fabric-user-data-functions/fabric.functions.fabricitem)
