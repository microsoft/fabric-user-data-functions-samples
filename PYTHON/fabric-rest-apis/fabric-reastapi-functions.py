import fabric.functions as fn
import logging
import json
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from microsoft_fabric_api import FabricClient

udf = fn.UserDataFunctions()


# ──────────────────────────────────────────────────────
# Helper: Build a FabricClient using SPN credentials
# ──────────────────────────────────────────────────────
def _create_fabric_client(
    tenantid: str, clientid: str, client_secret: str
) -> FabricClient:
    """
    Creates a FabricClient authenticated via Service Principal
    using ClientSecretCredential from azure-identity.
    """
    logging.info(f"tenantid '{tenantid}' " f"clientid '{clientid}'.")
    credential = ClientSecretCredential(
        tenant_id=tenantid, client_id=clientid, client_secret=client_secret
    )
    fabric_client = FabricClient(credential)
    logging.info("FabricClient created successfully with SPN credentials.")
    return fabric_client


# ──────────────────────────────────────────────────────
# Helper: Trigger a Fabric Data Pipeline via SDK
# ──────────────────────────────────────────────────────
def _run_fabric_pipeline(
    fabric_client: FabricClient, workspaceid: str, pipelineid: str
) -> dict:
    """
    Triggers a Fabric Data Pipeline using the FabricClient SDK's
    Job Scheduler - Run On Demand Item Job API.
    """
    try:
        # The SDK wraps the Job Scheduler REST API
        # run_on_demand_item_job triggers the pipeline and returns 202 Accepted
        response = fabric_client.core.job_scheduler.run_on_demand_item_job(
            workspace_id=workspaceid, item_id=pipelineid, job_type="Pipeline"
        )
        logging.info(
            f"Pipeline '{pipelineid}' triggered successfully "
            f"in workspace '{workspaceid}'."
        )
        return {
            "status": "success",
            "message": f"Pipeline {pipelineid} triggered successfully.",
            "details": str(response) if response else "Accepted (202)",
        }
    except Exception as e:
        logging.error(f"Pipeline trigger failed: {str(e)}")
        return {"status": "error", "message": str(e)}


# ──────────────────────────────────────────────────────
# Main UDF: Invoke Fabric Pipeline via SPN + Key Vault
# ──────────────────────────────────────────────────────
@udf.generic_connection(argName="keyVaultClient", audienceType="KeyVault")
@udf.function()
def invoke_pipeline_with_spn(
    keyVaultClient: fn.FabricItem,
    workspaceid: str,
    pipelineid: str,
    tenantid: str,
    clientid: str,
    keyVaultUrl: str,
    spnSecretName: str,
) -> str:
    """
    Description:
        Invokes a Fabric Data Pipeline using a Service Principal.
        The SPN client secret is securely retrieved from Azure Key Vault
        via the native Fabric generic connection. Uses the official
        microsoft-fabric-api SDK (FabricClient) for pipeline invocation.

    Args:
        - keyVaultClient (FabricItem): Fabric generic connection to Key Vault
        - workspaceid (str): Target Fabric workspace GUID
        - pipelineid (str): Target Data Pipeline item GUID
        - tenantid (str): Azure AD tenant ID for the SPN
        - clientid (str): App (client) ID of the Service Principal

    Returns:
        str: JSON string with the pipeline trigger result
    """
    try:
        logging.info("Fetching secret from KV")
        # Step 1: Get the Key Vault credential from Fabric connection
        credential = keyVaultClient.get_access_token()
        client = SecretClient(vault_url=keyVaultUrl, credential=credential)
        spn_secret = client.get_secret(spnSecretName).value
        logging.info("Fetched secret from KV")

        # Step 2: Create FabricClient with SPN credentials
        fabric_client = _create_fabric_client(
            tenantid=tenantid,
            clientid=clientid,
            client_secret=spn_secret,
        )
        logging.info("Created Fabric client")

        # Step 3: Trigger the pipeline using FabricClient SDK
        result = _run_fabric_pipeline(
            fabric_client=fabric_client, workspaceid=workspaceid, pipelineid=pipelineid
        )
        logging.info("Ran Fabric pipeline")

        return json.dumps(result, indent=2)
    except Exception as e:
        logging.error(f"invoke_pipeline_with_spn failed: {str(e)}")
        return json.dumps({"status": "error", "message": str(e)})
