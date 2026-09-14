# Connector function template

The semantic-model, Kusto, and Fabric MCP functions use the Fabric Python SDK's app-wide HTTP streaming mode.
Do not mix `@udf.function()` with `@udf.streaming_function()` in this template: the Functions host cannot index that combination.

## Fabric MCP response

`rayfin_fabric_mcp_v1` returns a `StreamResponse` with media type `application/json`.
Its UTF-8 JSON body contains the top-level `message` field expected by the connector transport, rather than the classic UDF `output.message` response envelope.
The function name and adapter version remain unchanged.

The default Fabric origin remains `https://api.fabric.microsoft.com`.
Alternate environments must supply the managed `FABRIC_API_BASE` setting; do not hardcode an environment-specific origin in the template or its archives.

## Metadata and archives

Keep the reviewed `functions.metadata` bindings, payload parameters, and `Fabric` audience intact.
The MCP return type must be `StreamResponse`.
The Fabric SDK 1.0.142 metadata generator omits streaming payload parameters and generic audience information, so its output must not replace the reviewed metadata wholesale.

From the repository root, run the regression tests and rebuild both deployment archives:

```shell
python -m pytest Templates/Python/ConnectorFunction/HelloFabric/test_function_app.py Templates/Python/ConnectorFunction/HelloFabric/test_fabric_mcp.py
python .github/scripts/repack_connector_function.py Templates/Python/ConnectorFunction/HelloFabric
```

Commit `function_app.py`, `functions.metadata`, `SourceCode.zip`, and `Deploy.zip` together.
Updating these files does not itself update existing deployed function apps.
