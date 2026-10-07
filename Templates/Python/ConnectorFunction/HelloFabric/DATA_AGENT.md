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
This template does not modify FeatureManagement configuration or broaden any rollout.

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

**TEST deployment requirement:** validate the runtime's `Fabric` generic binding and delegated token forwarding in the intended TEST environment.
Public generic-connection documentation describes owner identity; it does not prove production on-behalf-of behavior for this connector.
The host must supply the authorized user's Fabric token, a published agent, and server-owned routing.
The TEST branch default is usable without deployment environment injection.
The controlled Daily validation below exercised real Fabric sign-in, the generic binding, and delegated connector invocation.
It does not establish that the unchanged TEST archives, TEST/PPE credential audience, or TEST MCP route work.
Authorized TEST resources are still required for that environment-specific validation; neither Daily E2E nor earlier DXT/MSIT protocol smoke substitutes for it.

## Protocol and failures

Each invocation initializes protocol `2025-06-18`, advertises `tasks.requests.tools.call`, and sends `notifications/initialized`.
Other negotiated protocol versions fail explicitly rather than silently switching semantics.
Discovery requires exactly one tool, no continuation cursor, and one unambiguous required string question property in an object schema.
Optional properties are not guessed or populated.
Task augmentation requires both server capability `tasks.requests.tools.call` and tool `execution.taskSupport` of `optional` or `required`.
Otherwise the call returns the immediate answer; a required-task tool without server task capability fails explicitly.
Callers must handle both task and immediate-answer results.
In the October 7 Daily validation, calls without an explicit `ttl` completed inline; setting `ttl: 300000` exercised task creation, polling, result retrieval, and cancellation.
That is an observed test setting, not a new required input or a guarantee that every server will return a task.

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

## Controlled Daily validation (October 7, 2026)

A manually published Daily adaptation was exercised through a deployed Fabric app using the published Rayfin CLI and SDK packages at `1.36.0-alpha.1917`.
The app used the typed connector and real Fabric sign-in; no app-specific proxy, token stub, or direct MCP question bypassed the connector.
Backend telemetry confirmed the Data Agent flight evaluated to true for the approved app workspace and invocations used delegated-user authentication.
The host-contract release and backend descriptor prerequisites are merged and were exercised by that deployed runtime.

This was not an unchanged deployment of the TEST archives in this branch.
The Daily copy used the fixed Daily origin in both the base URL and exact-origin allowlist, and retained the generated Daily UDF's existing peer adapters.
All 11 adapter functions/classes match this implementation after normalizing the environment-specific error wording.
The successful publication's exported source and library settings were read back and verified.

| Check | Observed result |
| --- | --- |
| Fabric sign-in and `getInfo` | Agent name, description, MCP protocol, tool schema, and task capabilities returned through the app |
| `ask` and conversation history | An answer rendered; a follow-up sent two prior turns and returned the expected table name |
| `startTask`, `getTask`, `getTaskResult` | With explicit five-minute TTL, task progress moved from working to completed and the final result rendered |
| `cancelTask` | App cancellation returned `DATA_AGENT_TASK_CANCELLED`; a separate read-only MCP status check confirmed the server task was cancelled |
| Data-backed question | After the agent owner corrected the connected data source, a fresh-history aggregate question returned a numeric count through the unchanged app and adapter |

The aggregate response also included a generated-file notice, so it was not literally count-only.
No customer records were displayed; the generated file was not opened or downloaded.
The returned count was not independently compared against the database.
Second-identity behavior, RLS/OLS, cancellation before a task ID is available, and broader release readiness remain outside this validation.

### Dependencies and the remaining SDK compatibility check

The successful Daily publication used these public-library settings:

| Library | Published setting |
| --- | --- |
| `fabric-user-data-functions` | `1.0rc` (Portal selector; the exact resolved SDK version was not established) |
| `requests` | `2.33.1` |
| `aiohttp` | `3.14.1` |
| `asyncio` | `4.0.0` |
| `azurefunctions-extensions-http-fastapi` | `1.0.1` |
| `azure-connectors` | `0.5.0b1` |

Both PR archives already contain all five non-SDK dependency pins above, and their source and function metadata match the adjacent files.
Their SDK requirement remains `fabric-user-data-functions ~= 1.0`, which is not the same setting as the successful Daily publication's `1.0rc` selector.
Several library settings changed together before the successful publication; the E2E result does not isolate the RC selector as necessary.

**Before treating the TEST package as merge-ready, resolve this compatibility difference:** validate the exact packaged SDK requirement with the required Fabric generic binding and streaming APIs in TEST, or establish the supported compatible SDK setting and regenerate the archives through Fabric's Library Management packaging process.
Do not change the shared SDK requirement speculatively based only on the Daily result.
The Fabric UDF SDK is automatically supplied; do not add a duplicate library entry.

### Remaining delivery checks

This branch remains TEST-only, and CLI authoring remains held.
Delivery to `connector-function-daily` requires a separate environment-matched template change; do not replace this branch's TEST origin with Daily.
After the target template is available, validate a fresh app's automatic `rayfin up` provisioning without manual UDF edits or library additions.
The controlled manual publication does not prove that automatic packaging/provisioning path or the upgrade behavior of existing installations.

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
- [Manage UDF libraries](https://learn.microsoft.com/fabric/data-engineering/user-data-functions/how-to-manage-libraries)
