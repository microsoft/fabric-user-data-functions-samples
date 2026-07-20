import fabric.functions as fn
import aiohttp
import asyncio
import json
import os
import uuid
from typing import Optional

udf = fn.UserDataFunctions()

_JSON_MEDIA_TYPE = "application/json"

# Shared, lazily-created session reused across invocations for connection pooling
# (keep-alive to the Kusto/Eventhouse cluster, no per-call TLS handshake). Created
# inside the event loop on first use; never closed per-invoke.
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is not None and not _session.closed:
        return _session
    async with _session_lock:
        if _session is None or _session.closed:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
            connector = aiohttp.TCPConnector(
                limit=100,
                keepalive_timeout=60,
                ttl_dns_cache=300,
            )
            _session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return _session


def _split_v1_rows(rows):
    # A v1 QueryResult table may append a trailing error object (not an array).
    # Strip it and surface its exceptions, mirroring KWE splitV1Rows.
    if rows and not isinstance(rows[-1], list):
        last = rows[-1]
        errors = None
        if isinstance(last, dict):
            errors = last.get("Exceptions") or last.get("OneApiErrors")
        return rows[:-1], errors
    return rows, None


def _map_table(table):
    data_rows, row_errors = _split_v1_rows(table.get("Rows", []) or [])
    columns = [
        {"name": c.get("ColumnName"), "type": c.get("ColumnType") or c.get("DataType")}
        for c in (table.get("Columns", []) or [])
    ]
    return {"name": table.get("TableName"), "columns": columns, "rows": data_rows}, row_errors


def _transform_v1(doc):
    # Kusto native v1 -> Rayfin connector output shape. Mirrors
    # packages/client/src/clients/kusto/kustoRequest.ts::normalizeResultV1.
    tables = doc.get("Tables", []) or []
    out_tables = []
    errors = []
    if len(tables) == 1:
        tbl, row_errors = _map_table(tables[0])
        out_tables.append(tbl)
        if row_errors:
            errors.extend(row_errors)
        return out_tables, errors
    toc = tables[-1]
    toc_cols = toc.get("Columns", []) or []
    kind_idx = next((i for i, c in enumerate(toc_cols) if c.get("ColumnName") == "Kind"), -1)
    pretty_idx = next((i for i, c in enumerate(toc_cols) if c.get("ColumnName") == "PrettyName"), -1)
    for i, row in enumerate(toc.get("Rows", []) or []):
        if not isinstance(row, list):
            continue
        kind = row[kind_idx] if 0 <= kind_idx < len(row) else None
        if kind == "QueryResult" and i < len(tables):
            tbl, row_errors = _map_table(tables[i])
            pretty = row[pretty_idx] if 0 <= pretty_idx < len(row) else None
            if pretty:
                tbl["name"] = pretty
            out_tables.append(tbl)
            if row_errors:
                errors.extend(row_errors)
    return out_tables, errors


@udf.streaming_function()
async def rayfin_kusto_v1(payload: dict, accesstoken: str) -> fn.StreamResponse:
    input_data = payload.get("input", {})

    # queryServiceUri + databaseName are resolved and injected by the Fabric app
    # backend (ConnectorFunctionInvocationWorkflow.ResolveKustoQueryEndpointAsync);
    # the caller only supplies `query`.
    query_service_uri = input_data.get("queryServiceUri")
    database_name = input_data.get("databaseName")
    kql_query = input_data.get("query")

    if not query_service_uri or not database_name or not kql_query:
        raise ValueError("queryServiceUri, databaseName and query are required")

    client_request_id = f"KPC.rayfin_kusto_v1;{uuid.uuid4()}"

    url = f"{query_service_uri.rstrip('/')}/v1/rest/query"
    headers = {
        "Authorization": f"Bearer {accesstoken}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "x-ms-client-request-id": client_request_id,
    }
    body = {"db": database_name, "csl": kql_query, "properties": {}}

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

    activity_id = resp.headers.get("x-ms-activity-id")
    raw = await resp.text()
    await resp.release()

    doc = json.loads(raw)
    tables, errors = _transform_v1(doc)

    # Only surface errors when there is no data (avoid the SDK discarding a
    # successful result over a soft/partial warning row).
    errors_out = [] if tables else [{"message": str(e)} for e in errors]

    envelope = {
        "status": "Failed" if errors_out else "Succeeded",
        "output": {
            "tables": tables,
            "clientRequestId": client_request_id,
            "activityId": activity_id,
        },
        "errors": errors_out,
    }

    return fn.StreamResponse(
        iter([json.dumps(envelope).encode("utf-8")]),
        media_type=_JSON_MEDIA_TYPE,
    )
