import fabric.functions as fn
import requests
import json

udf = fn.UserDataFunctions()

@udf.function()
def rayfin_semantic_model_v1(payload: dict, accesstoken: str) -> dict:
    """
    Power BI Connector UDF
    Operation: executeQuery
    """
    operation = payload.get("operation")
    input_data = payload.get("input", {})

    if operation != "executeQuery":
        raise ValueError(f"Unsupported operation: {operation}")

    dataset_id = input_data.get("itemId")
    workspace_id = input_data.get("workspaceId")
    dax_query = input_data.get("query")

    if not workspace_id or not dataset_id or not dax_query:
        raise ValueError("workspaceId, datasetId and query are required")

    # ---- Power BI ExecuteQueries API ----
    url = (
        f"https://dailyapi.powerbi.com/v1.0/myorg/"
        f"datasets/{dataset_id}/executeQueries"
    )

    headers = {
        "Authorization": f"Bearer {accesstoken}",
        "Content-Type": "application/json",
        "User-Agent": "rayfin-fabric-semanticmodel/1.0"
    }

    body = {
        "queries": [
            {
                "query": dax_query
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    response = requests.post(url, headers=headers, json=body)
    request_id = response.headers.get("RequestId")

    if response.status_code != 200:
        try:
            err = (response.json() or {}).get("error") or {}
        except Exception:
            err = {}
        raise RuntimeError(json.dumps({
            "httpStatus": response.status_code,
            "code": err.get("code"),
            "message": err.get("message") or response.text,
            "requestId": request_id,
        }))

    result = response.json() or {}
    results = result.get("results") or []
    first = results[0] if results else {}

    return {
        "tables": first.get("tables", []),
        "requestId": request_id,
        "queryError": first.get("error"),
        "responseError": result.get("error"),
        "informationProtectionLabel": result.get("informationProtectionLabel"),
    }
