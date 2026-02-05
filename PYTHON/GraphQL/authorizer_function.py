from typing import TYPE_CHECKING
import logging 

@udf.function()
def invokeauthudf(request: dict) -> dict:
    token_claims: dict = request.get("tokenClaims", {})
    query: str = request.get("query", "")
    variables: dict = request.get("variables", {})

    domain = "YOUR-ENTRA-DOMAIN"
    spn = "YOUR-SPN-APP-ID"

    # Extract claims
    tid_claim = token_claims.get("tid")
    upn_claim = token_claims.get("upn")
    appid_claim = token_claims.get("appid")

    logging.info(f"Query: {query}")
    logging.info(f"Claims: {token_claims}")
    logging.info(f"Tenant: {tid_claim}")
    logging.info(f"UPN: {upn_claim}")
    logging.info(f"SPN: {appid_claim}")

    # Authorization logic
    roles = []

    if upn_claim is not None:
        is_authorized = domain in upn_claim
        roles = ["Default"]
    else:
        is_authorized = spn in appid_claim
        roles = ["SPN"]

    logging.info(f"Authorized: {is_authorized}")
    logging.info(f"Roles: {roles}")
    logging.info(f"SPN: {spn}")
    logging.info(f"App ID: {appid_claim}")

    return {
        "isAuthorized": is_authorized,
        "roles": roles
    }
