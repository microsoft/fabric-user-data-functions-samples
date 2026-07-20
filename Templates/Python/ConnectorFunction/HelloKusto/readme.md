# Rayfin Kusto / Eventhouse Connector Function

This is a Rayfin **connector function** (a Fabric User Data Function) that lets Fabric
data apps run KQL against a Kusto / Eventhouse cluster. The function is named
`rayfin_kusto_v1` and is invoked by the Fabric app backend.

## Invocation contract

The Fabric app backend wraps each call as `{ accesstoken, payload }`, where
`payload = { "operation": "executeQuery", "input": { ... } }`.

`payload.input` contains:

| Field             | Source                          | Description                                             |
| ----------------- | ------------------------------- | ------------------------------------------------------- |
| `queryServiceUri` | Injected by the Fabric backend  | Absolute https cluster query URL. The UDF never resolves an endpoint. |
| `databaseName`    | Injected by the Fabric backend  | Target KQL database name.                               |
| `query`           | Supplied by the caller          | The KQL query text to execute.                          |
| `workspaceId`     | Injected by the Fabric backend  | Not used by the UDF.                                    |
| `itemId`          | Injected by the Fabric backend  | Not used by the UDF.                                    |

The `accesstoken` is an OBO token already scoped to the Kusto audience. The UDF calls
the cluster's native `POST {queryServiceUri}/v1/rest/query` endpoint.

## Output contract

On success the function emits `application/json` with the envelope the Rayfin SDK
(`@microsoft/rayfin-connector-kusto`) expects:

```json
{
  "status": "Succeeded",
  "output": {
    "tables": [
      {
        "name": "PrimaryResult",
        "columns": [ { "name": "Col", "type": "string" } ],
        "rows": [ [ "value" ] ]
      }
    ],
    "clientRequestId": "KPC.rayfin_kusto_v1;<guid>",
    "activityId": "<x-ms-activity-id or null>"
  },
  "errors": []
}
```

Kusto's native `/v1/rest/query` response uses a different shape
(`{ "Tables": [ { "TableName", "Columns": [...], "Rows": [...] } ] }` with a trailing
table-of-contents table when multiple tables are returned). The function transforms the
native Kusto v1 response into the envelope above, mirroring KWE's
`normalizeResultV1`: with a single table it is the primary result; otherwise the last
table is a TOC whose `Kind`/`PrettyName` columns describe the others, and only
`Kind == "QueryResult"` tables are surfaced. A trailing non-array error object on a
result table's rows is stripped.

Non-2xx responses from the cluster are surfaced verbatim; the Fabric app backend
sanitizes non-2xx bodies before relaying them to the caller.

## Steps to run this project locally

1. Ensure the most up to date version of azure functions core tools are installed `https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=windows%2Cisolated-process%2Cnode-v4%2Cpython-v2%2Chttp-trigger%2Ccontainer-apps&pivots=programming-language-python#install-the-azure-functions-core-tools`.
2. Install Python 3.11 and add it to your PATH environment variable (you can download from https://www.python.org/downloads/windows/).
3. You must have the latest ODBC driver(18) installed (you can download from https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16)
4. Configure your first function.
5. Create a virtual environment for the python sample. In VSCode, this can be done through installing the Python extension in the VSCode marketplace and then using the shortcut Ctrl+Shift+P to find the task "Python: Create Environment", clicking the Venv environment type, picking the python version you downloaded, and then selecting requirements.txt as the dependencies to install.
6. Hit `F5` or open the debugging window in VSCode to run and debug the function.
