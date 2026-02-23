import datetime
import json
import re
import uuid

'''
Fabric User Data Function - Data Agent Integration SDK
======================================================

Connects to a published Microsoft Fabric Data Agent using the
OpenAI Assistants API pattern with token passthrough authentication.

Architecture:
  Notebook (has notebookutils) --> mints bearer token --> passes to UDF --> UDF calls Fabric API

The UDF sandbox CANNOT mint its own tokens because:
  - notebookutils is not available in UDF runtime
  - azure-identity cannot be pip installed (proxy blocks PyPI)
  - IMDS managed identity endpoint is not reachable
  - No identity environment variables are set

Therefore ALL public functions accept a "bearertoken" parameter that the
notebook caller must provide via:
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")

API Pattern (from official MS docs):
  1. POST /assistants             --> create assistant (model="not used")
  2. POST /threads                --> create thread
  3. POST /threads/{id}/messages  --> add user message
  4. POST /threads/{id}/runs      --> create run (with assistant_id)
  5. GET  /threads/{id}/runs/{id} --> poll until terminal state
  6. GET  /threads/{id}/messages  --> read response
  7. DELETE /threads/{id}         --> cleanup

CRITICAL: All API calls require ?api-version=2024-05-01-preview

Prerequisites:
  - A published Fabric Data Agent with a valid endpoint URL
  - User Data Functions tenant setting enabled
  - The invoking user must have access to the underlying data sources

UDF Runtime Constraints (verified via diagnostic probe):
  - Python 3.12.7 (conda-forge)
  - Available: requests, azure.core, urllib.request, fabric.functions
  - NOT available: azure.identity, msal, notebookutils
  - Network: Fabric API reachable (HTTPS), PyPI blocked, IMDS refused
  - No underscore characters allowed in UDF parameter names
  - @udf.function() must be the decorator directly above def
  - Other decorators (@udf.context, @udf.connection) go above @udf.function()
'''

# =============================================================================
# Configuration
# =============================================================================

API_VERSION = "2024-05-01-preview"
DEFAULT_TIMEOUT = 120
POLL_INTERVAL = 3

# =============================================================================
# Helpers (not decorated -- safe to call from decorated functions)
# =============================================================================

def get_access_token(bearertoken: str = "") -> str:
    '''
    Returns a valid access token for the Fabric API.

    Primary path: use the caller-provided bearertoken (from notebook).
    Fallback: attempt auto-detection (best-effort, usually fails in UDF sandbox).
    '''
    if bearertoken and bearertoken.strip():
        logging.info("Using caller-provided bearer token")
        return bearertoken.strip()

    # Auto-detection fallback (best-effort)
    errors = []

    try:
        from notebookutils import mssparkutils
        return mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
    except Exception as e:
        errors.append(f"notebookutils: {e}")

    try:
        from azure.identity import DefaultAzureCredential
        token = DefaultAzureCredential().get_token("https://api.fabric.microsoft.com/.default")
        return token.token
    except Exception as e:
        errors.append(f"DefaultAzureCredential: {e}")

    try:
        import urllib.request
        url = ("http://169.254.169.254/metadata/identity/oauth2/token"
               "?api-version=2018-02-01&resource=https://api.fabric.microsoft.com")
        req = urllib.request.Request(url, headers={"Metadata": "true"})
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read().decode("utf-8"))
        if data.get("access_token"):
            return data["access_token"]
    except Exception as e:
        errors.append(f"IMDS: {e}")

    raise RuntimeError(
        f"No bearer token provided and auto-detection failed.\n"
        f"Details: {'; '.join(errors)}\n\n"
        f"Pass a token from the notebook caller:\n"
        f"  token = notebookutils.credentials.getToken('https://api.fabric.microsoft.com')\n"
        f"  notebookutils.udf.run('MyUDF', 'ask_agent', ..., bearertoken=token)"
    )


def parse_agent_url(dataAgentUrl: str) -> dict:
    '''
    Parses a Fabric Data Agent URL and returns workspace ID, artifact ID, and base URL.

    Supports two URL formats:
      Format 1 (REST API): https://api.fabric.microsoft.com/v1/workspaces/{ws}/dataagents/{art}/aiassistant/openai
      Format 2 (Portal):   https://<env>.fabric.microsoft.com/groups/{ws}/aiskills/{art}
    '''
    # Format 1: REST API endpoint
    m = re.search(
        r"https://api\.fabric\.microsoft\.com/v1/workspaces/([^/]+)/data[aA]gents?/([^/]+)",
        dataAgentUrl
    )
    if m:
        ws, art = m.group(1), m.group(2)
        base = f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/dataagents/{art}/aiassistant/openai"
        return {"workspaceId": ws, "artifactId": art, "baseUrl": base}

    # Format 2: Portal URL
    m = re.search(
        r"https://([^/]+)/groups/([^/]+)/aiskills/([^/\?]+)",
        dataAgentUrl
    )
    if m:
        ws, art = m.group(2), m.group(3)
        base = f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/dataagents/{art}/aiassistant/openai"
        return {"workspaceId": ws, "artifactId": art, "baseUrl": base}

    raise ValueError(
        f"Invalid Data Agent URL format.\n"
        f"Expected one of:\n"
        f"  https://api.fabric.microsoft.com/v1/workspaces/<id>/dataagents/<id>/aiassistant/openai\n"
        f"  https://<env>.fabric.microsoft.com/groups/<id>/aiskills/<id>\n"
        f"Got: {dataAgentUrl}"
    )


def build_url(base: str, path: str) -> str:
    '''Appends path and api-version query parameter to the base URL.'''
    full = f"{base}{path}"
    sep = "&" if "?" in full else "?"
    return f"{full}{sep}api-version={API_VERSION}"


def make_headers(token: str) -> dict:
    '''Builds standard headers for Fabric Data Agent API calls.'''
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "ActivityId": str(uuid.uuid4())
    }


def utcnow() -> str:
    '''Returns current UTC timestamp in ISO format (timezone-aware).'''
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def call_agent(
    dataAgentUrl: str,
    prompt: str,
    bearertoken: str = "",
    history: list = None,
    includeDetails: bool = False,
    timeoutSeconds: int = DEFAULT_TIMEOUT
) -> dict:
    '''
    Core implementation -- sends a prompt to a Fabric Data Agent.

    NOT decorated with @udf.function() so it can be safely called from
    multiple decorated entry points without coroutine wrapper issues.

    Follows the official MS OpenAI Assistants API pattern exactly.
    '''
    import requests

    logging.info(f"Processing Data Agent request - Prompt: {prompt[:100]}...")

    thread_id = None
    headers = None
    base = None

    try:
        url_parts = parse_agent_url(dataAgentUrl)
        base = url_parts["baseUrl"]
        access_token = get_access_token(bearertoken=bearertoken)
        headers = make_headers(access_token)

        logging.info(f"API base URL: {base}")

        # Step 1: Create assistant
        r = requests.post(build_url(base, "/assistants"), headers=headers,
                          json={"model": "not used"}, timeout=30)
        r.raise_for_status()
        assistant_id = r.json().get("id")
        logging.info(f"Assistant: {assistant_id}")

        # Step 2: Create thread
        r = requests.post(build_url(base, "/threads"), headers=headers, timeout=30)
        r.raise_for_status()
        thread_id = r.json().get("id")
        logging.info(f"Thread: {thread_id}")

        # Step 3a: Add conversation history (if provided)
        if history:
            for msg in history:
                requests.post(build_url(base, f"/threads/{thread_id}/messages"),
                              headers=headers, json=msg, timeout=30)

        # Step 3b: Add the current user message
        r = requests.post(build_url(base, f"/threads/{thread_id}/messages"),
                          headers=headers,
                          json={"role": "user", "content": prompt}, timeout=30)
        r.raise_for_status()

        # Step 4: Create run (with assistant_id)
        r = requests.post(build_url(base, f"/threads/{thread_id}/runs"),
                          headers=headers,
                          json={"assistant_id": assistant_id}, timeout=30)
        r.raise_for_status()
        run_id = r.json().get("id")
        logging.info(f"Run: {run_id}")

        # Step 5: Poll for completion
        terminal = {"completed", "failed", "cancelled", "requires_action", "expired"}
        start = time.time()
        status = "queued"
        run_data = {}

        while status not in terminal:
            if time.time() - start > timeoutSeconds:
                raise TimeoutError(f"Timed out after {timeoutSeconds}s (last status: {status})")
            time.sleep(POLL_INTERVAL)
            r = requests.get(build_url(base, f"/threads/{thread_id}/runs/{run_id}"),
                             headers=headers, timeout=30)
            r.raise_for_status()
            run_data = r.json()
            status = run_data.get("status", "unknown")
            logging.info(f"Status: {status}")

        if status != "completed":
            err = run_data.get("last_error", {}).get("message", f"Run finished: {status}")
            raise RuntimeError(err)

        # Step 6: Retrieve messages (ascending order)
        r = requests.get(build_url(base, f"/threads/{thread_id}/messages?order=asc"),
                         headers=headers, timeout=30)
        r.raise_for_status()
        messages = r.json().get("data", [])

        # Extract the last assistant message
        answer = ""
        for msg in reversed(messages):
            if msg.get("role") == "assistant":
                content = msg.get("content", [])
                if isinstance(content, list) and content:
                    answer = content[0].get("text", {}).get("value", "")
                break

        # Build result
        result = {
            "success": True,
            "answer": answer,
            "timestamp": utcnow(),
            "error": None
        }

        # Step 7 (optional): Get run steps for details
        if includeDetails:
            steps_r = requests.get(
                build_url(base, f"/threads/{thread_id}/runs/{run_id}/steps"),
                headers=headers, timeout=30
            )
            if steps_r.status_code == 200:
                run_steps = steps_r.json().get("data", [])
                steps = []
                queries = []
                for step in run_steps:
                    tool_name = "N/A"
                    step_details = step.get("step_details", {})
                    for tc in step_details.get("tool_calls", []):
                        if "function" in tc:
                            tool_name = tc["function"].get("name", "N/A")
                            try:
                                args = json.loads(tc["function"].get("arguments", "{}"))
                                if "query" in args:
                                    queries.append({"tool": tool_name, "query": args["query"]})
                            except (json.JSONDecodeError, TypeError):
                                pass
                    steps.append({
                        "stepId": step.get("id"),
                        "type": step.get("type"),
                        "status": step.get("status"),
                        "toolName": tool_name
                    })
                result["steps"] = steps
                result["generatedQueries"] = queries

        # Add conversation history to result if this was a contextual call
        if history is not None:
            updated = (history or []).copy()
            updated.append({"role": "user", "content": prompt})
            updated.append({"role": "assistant", "content": answer})
            result["conversationHistory"] = updated

        logging.info("Data Agent request completed successfully")
        return result

    except Exception as e:
        logging.error(f"Data Agent request failed: {e}")
        return {
            "success": False,
            "answer": None,
            "timestamp": utcnow(),
            "error": str(e)
        }
    finally:
        # Cleanup: delete thread to avoid resource leaks
        if thread_id and headers and base:
            try:
                requests.delete(build_url(base, f"/threads/{thread_id}"),
                                headers=headers, timeout=10)
                logging.info(f"Thread {thread_id} cleaned up")
            except Exception:
                logging.warning(f"Failed to clean up thread {thread_id}")


# =============================================================================
# Public UDF Entry Points
# =============================================================================
# RULES (learned the hard way):
#   1. @udf.function() must be the decorator DIRECTLY above def
#   2. Other decorators go ABOVE @udf.function()
#   3. Parameter names CANNOT contain underscores
#   4. Never call a @udf.function() decorated function from another one
#      (returns coroutine proxy, not the value) -- use call_agent() helper
# =============================================================================

@udf.function()
def ask_agent(
    dataAgentUrl: str,
    prompt: str,
    bearertoken: str = "",
    includeDetails: bool = False,
    timeoutSeconds: int = DEFAULT_TIMEOUT
) -> dict:
    '''
    Sends a natural language prompt to a published Fabric Data Agent.

    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.
        prompt: The natural language question to ask.
        bearertoken: Bearer token from notebook caller (REQUIRED in UDF runtime).
                     Get via: notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        includeDetails: If True, includes step info and generated queries.
        timeoutSeconds: Maximum wait time (default: 120).

    Returns:
        dict with: success, answer, timestamp, error, and optionally steps/generatedQueries.

    Notebook usage:
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        result = notebookutils.udf.run("MyUDF", "ask_agent",
            dataAgentUrl="https://api.fabric.microsoft.com/v1/workspaces/.../dataagents/.../aiassistant/openai",
            prompt="What were total sales last quarter?",
            bearertoken=token)
    '''
    return call_agent(
        dataAgentUrl=dataAgentUrl,
        prompt=prompt,
        bearertoken=bearertoken,
        includeDetails=includeDetails,
        timeoutSeconds=timeoutSeconds
    )


@udf.function()
def ask_agent_simple(
    dataAgentUrl: str,
    prompt: str,
    bearertoken: str = ""
) -> str:
    '''
    Simplified version -- returns just the answer text.

    Args:
        dataAgentUrl: The published endpoint URL.
        prompt: The natural language question.
        bearertoken: Bearer token from notebook caller (REQUIRED in UDF runtime).

    Returns:
        str: The answer text, or an error message.

    Notebook usage:
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        answer = notebookutils.udf.run("MyUDF", "ask_agent_simple",
            dataAgentUrl="...", prompt="How many customers?", bearertoken=token)
    '''
    result = call_agent(
        dataAgentUrl=dataAgentUrl,
        prompt=prompt,
        bearertoken=bearertoken
    )
    if result.get("success"):
        return result.get("answer", "No answer received")
    return f"Error: {result.get('error', 'Unknown error')}"


@udf.function()
def ask_agent_with_history(
    dataAgentUrl: str,
    prompt: str,
    conversationHistory: list,
    bearertoken: str = ""
) -> dict:
    '''
    Sends a prompt with conversation history for multi-turn conversations.

    Args:
        dataAgentUrl: The published endpoint URL.
        prompt: The current question.
        conversationHistory: Previous messages as [{"role":"user","content":"..."},...]
        bearertoken: Bearer token from notebook caller (REQUIRED in UDF runtime).

    Returns:
        dict with: success, answer, conversationHistory (updated), timestamp, error.

    Notebook usage:
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        history = []
        result = notebookutils.udf.run("MyUDF", "ask_agent_with_history",
            dataAgentUrl="...", prompt="Total sales?",
            conversationHistory=history, bearertoken=token)
        history = result["conversationHistory"]
    '''
    return call_agent(
        dataAgentUrl=dataAgentUrl,
        prompt=prompt,
        bearertoken=bearertoken,
        history=conversationHistory
    )


@udf.function()
def validate_agent(
    dataAgentUrl: str,
    bearertoken: str = ""
) -> dict:
    '''
    Validates connectivity to a Fabric Data Agent without sending a query.

    Args:
        dataAgentUrl: The published endpoint URL.
        bearertoken: Bearer token from notebook caller (REQUIRED in UDF runtime).

    Returns:
        dict with: isValid, workspaceId, artifactId, message, error.
    '''
    import requests

    logging.info("Validating Data Agent connection...")

    try:
        url_parts = parse_agent_url(dataAgentUrl)
        base = url_parts["baseUrl"]
        access_token = get_access_token(bearertoken=bearertoken)
        headers = make_headers(access_token)

        # Test by creating and immediately deleting a thread
        r = requests.post(build_url(base, "/threads"), headers=headers, timeout=30)

        if r.status_code == 200:
            thread_id = r.json().get("id")
            try:
                requests.delete(build_url(base, f"/threads/{thread_id}"),
                                headers=headers, timeout=10)
            except Exception:
                pass
            return {
                "isValid": True,
                "workspaceId": url_parts["workspaceId"],
                "artifactId": url_parts["artifactId"],
                "message": "Connection validated successfully",
                "error": None
            }
        else:
            return {
                "isValid": False,
                "workspaceId": url_parts["workspaceId"],
                "artifactId": url_parts["artifactId"],
                "message": f"Connection failed: HTTP {r.status_code}",
                "error": r.text
            }
    except ValueError as ve:
        return {"isValid": False, "workspaceId": None, "artifactId": None,
                "message": "Invalid URL format", "error": str(ve)}
    except Exception as e:
        return {"isValid": False, "workspaceId": None, "artifactId": None,
                "message": "Validation failed", "error": str(e)}
