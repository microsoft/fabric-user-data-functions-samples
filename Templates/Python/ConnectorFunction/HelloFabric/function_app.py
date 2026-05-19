from typing import Any
import fabric.functions as fn
import logging
import requests

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

    dataset_id = input_data.get("datasetId")
    workspace_id = input_data.get("workspaceId")
    dax_query = input_data.get("query")

    if not dataset_id or not dax_query:
        raise ValueError("datasetId and query are required")

    # ---- Power BI ExecuteQueries API ----
    url = (
        f"https://powerbiapi.analysis-df.windows.net/v1.0/myorg/groups/{workspace_id}/"
        f"datasets/{dataset_id}/executeQueries"
    )

    headers = {
        "Authorization": f"Bearer {accesstoken}",
        "Content-Type": "application/json"
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

    if response.status_code != 200:
        raise RuntimeError(
            f"Power BI query failed ({response.status_code}): {response.text}"
        )

    result = response.json()

    # ---- Normalize output for SDK ----
    return {
        "tables": result["results"][0]["tables"]
    }
