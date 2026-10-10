# Connector function template

The semantic-model, Kusto, and Fabric MCP functions use the Fabric Python SDK's app-wide HTTP streaming mode.
Do not mix `@udf.function()` with `@udf.streaming_function()` in this template: the Functions host cannot index that combination.

## Fabric MCP response

`rayfin_fabric_mcp_v1` returns a `StreamResponse` with media type `application/json`.
Its body is this top-level UTF-8 JSON envelope, rather than the classic UDF
`output.message` envelope:

```json
{"status": 200, "headers": {"Content-Type": "application/json"}, "message": "..."}
```

`status` is the actual upstream HTTP status, including 3xx/4xx/5xx. `message` is
the entire UTF-8 response text (empty body becomes `""`), without JSON-RPC or SSE
interpretation. An invalid URL rejected by the HTTP client, credential failure,
timeout, connection failure, incomplete body, or invalid UTF-8 fails the
invocation rather than inventing an HTTP success. The function name, binding,
and adapter version remain unchanged.

The function's `payload` uses the standard BaaS wrapper
`{operation: "executeQuery", input: {protocolVersion, headers, message}}`.
The operation must be `executeQuery` and `input` must be an object; direct
input and mixed wrapper/direct fields are rejected. Only `input.message`
is JSON-serialized into one POST, without interpreting its contents.
Request preparation is unchanged:
missing Content-Type defaults to
`application/json`, missing Accept to `application/json, text/event-stream`,
and missing MCP-Protocol-Version to `input.protocolVersion`. Header presence
is checked case-insensitively and caller values are not overwritten.
Only incoming Authorization is filtered; there is no new Host, framing,
hop-by-hop, or Connection-nominated header filter.
Authorization is replaced case-insensitively with the existing Fabric binding's
delegated token. The shared HTTP session and its timeout policy are reused;
other ConnectorFunction entry points are unchanged.

The destination derives only from the existing server-managed `POWERBI_API_BASE`,
read at module load. Its existing default is
`https://dailyapi.powerbi.com/v1.0/myorg`, so an absent setting targets **Daily**,
not Prod. `FABRIC_API_BASE` is no longer read.
The parsed hostname selects the following origin transformation, then the fixed
route `/v1/mcp/fabriciq` is appended:

| Configured PowerBI hostname | Fabric origin |
| --- | --- |
| `api.powerbi.com` (Prod, MSIT, MSITBCDR, ONEBOX) | `https://api.fabric.microsoft.com` |
| `dailyapi.powerbi.com` | `https://dailyapi.fabric.microsoft.com` |
| `dxtapi.powerbi.com` (including BCDR) | `https://dxtapi.fabric.microsoft.com` |
| `powerbiapi.analysis-df.windows.net` (TEST/EDOG) | `https://powerbiapi.analysis-df.windows.net` |

All approved configurations use HTTPS; the implementation retains the configured
scheme. Other configured hosts retain their parsed origin rather than inventing
a Fabric hostname. The PowerBI path, query, and fragment are not forwarded.
An explicitly empty setting is not replaced with the default and fails in the
HTTP client.
The server setting, not caller data, is the trust boundary: administrators remain
responsible for configuring a trusted destination. The HTTP envelope change does
not add destination or request-header validation.
Redirects are never followed; a redirect response is returned as an envelope
including its Location header when present.

### Response metadata and actual use

**All upstream header names are returned without an allowlist or exclusions.**
The contract is
`Record<string, string>`: repeated names are compared case-insensitively and only
the first value and its original name casing are kept, including an empty first
value. Later values are discarded, not joined or represented as arrays.
No metadata value is inferred from the payload or status.

The following are examples of protocol metadata, not an allowlist:

| Header | Consumer use / evidence boundary |
| --- | --- |
| Content-Type | Allows the client transport to select JSON versus SSE processing. Local forwarding tests cover both; Python does not parse either. |
| MCP-Session-Id | Enables client-owned session correlation if the server supplies it. Preservation is tested; a requirement for Fabric is not established. |
| MCP-Protocol-Version | Preserves upstream version metadata if supplied. The original outgoing fallback remains; a response-header requirement is not established. |
| Retry-After | Preserves upstream backoff guidance (for example, 429). No Python retry or delay is added; actual use depends on the client. |

This includes Set-Cookie, any credential-bearing headers, Content-Length,
Transfer-Encoding, Location, and arbitrary application metadata when present.
They are fields inside the JSON envelope, **not** applied as HTTP headers of the
function response. Consequently upstream cookies/credentials can be visible to
the caller; this is not a sanitized metadata policy. The first-value rule does not preserve multiple Set-Cookie
values. No tokens, header dumps, prompts, results, or bodies are logged.
Buffering ends when **this HTTP response** ends, not when an MCP task
finishes. There is no MCP parser, dispatch, session management, polling, task
wait, or application retry.

### Validation and compatibility

The tests use the existing Fabric runtime stub and synthetic credentials.
Loopback integration exercises real aiohttp request/response behavior while a
test-only adapter substitutes the network URL; it is not a TLS, hosted binding,
Edog, or chat E2E test. It covers preserved request fallbacks and caller headers,
authorization, full JSON/SSE including split UTF-8, empty bodies, redirects, error HTTP
statuses, timeouts, disconnects, and decoding failures.
Response tests cover arbitrary headers, cookies/credentials/framing as metadata,
and first-value selection for repeated names with differing casing or empty values.

Local validation covers the portable source and bundles. Hosted validation used
a separate Edog-only test package: the body of `_invoke_fabric_mcp` was applied
to the actual deployed baseline, preserving its environment-specific fallback
and all other source, metadata, and dependencies. This repository retains the
portable Prod fallback; the Edog test package is not shipped here.

In an existing embedded chat, the Python-only phase passed discovery, listing
three tools, one Ask operation, task progress, and completion with the unchanged
baseline SDK. A subsequent phase held Python fixed and passed the same scenario
with the SDK's HTTP-envelope adaptation. The client retained its custom
transport and existing Tasks support; no transport migration is implied.

These are embedded happy-path results for the environment-matched test package,
not direct E2E validation of the portable ZIP or a Prod rollout. Live cancellation,
all error paths, direct-hosted apps, other apps, and performance were not covered.
The baseline client ignores the new status/metadata; clients using those fields
must adopt the complete envelope. Separate test phases do not establish that
the updated SDK can run against the old message-only response.

## Metadata and archives

Keep the reviewed `functions.metadata` bindings, payload parameters, and `Fabric` audience intact.
The MCP return type must be `StreamResponse`.
The Fabric SDK 1.0.142 metadata generator omits streaming payload parameters and generic audience information, so its output must not replace the reviewed metadata wholesale.

From the repository root, run the regression tests and rebuild both deployment archives:

```shell
python -m pytest Templates/Python/ConnectorFunction/HelloFabric/test_function_app.py Templates/Python/ConnectorFunction/HelloFabric/test_fabric_mcp.py Templates/Python/ConnectorFunction/HelloFabric/test_fabric_mcp_daily.py
python .github/scripts/repack_connector_function.py Templates/Python/ConnectorFunction/HelloFabric
python .github/scripts/repack_connector_function.py Templates/Python/ConnectorFunction/HelloFabric --check
python .github/scripts/check_zip_permissions.py Templates/Python/ConnectorFunction/HelloFabric/Deploy.zip Templates/Python/ConnectorFunction/HelloFabric/SourceCode.zip
```

Keep `function_app.py`, `functions.metadata`, `SourceCode.zip`, and `Deploy.zip`
consistent. This envelope-only change does not require a metadata edit:
`StreamResponse`, payload parameters, and all existing bindings stay intact.
Rebuild the archives with the existing script; do not replace metadata for the
semantic-model or Kusto functions.
Updating these files does not itself update existing deployed function apps.

Binding reference reviewed: `PYTHON/VariableLibrary/chat_completion_with_azure_openai.py`
(generic connection and `get_access_token()`); the existing Fabric-audience
binding and streaming decorators are preserved rather than replaced by that
sample's non-streaming entry point.
