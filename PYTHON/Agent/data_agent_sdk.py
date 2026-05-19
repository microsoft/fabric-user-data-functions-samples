import json
import re
import uuid

# =============================================================================
# Fabric User Data Function - Data Agent Integration
# Version: 2.2.0
# =============================================================================
#
# SETUP REQUIREMENTS:
#   1. Create a User Data Function item in your Fabric workspace.
#   2. Paste this entire file as the function source code.
#   3. Publish the Fabric Data Agent and copy its published URL.
#   4. Enable the "User Data Functions" tenant setting in the admin portal.
#   5. The invoking user must have access to the Data Agent's data sources.
#   6. No additional packages needed (uses only pre-installed libraries).
#
# AUTHENTICATION:
#   The UDF sandbox cannot mint its own tokens. The notebook caller must
#   provide a bearer token via the "bearertoken" parameter:
#
#       token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
#       result = notebookutils.udf.run("MyUDF", "ask_agent",
#           dataAgentUrl="...", prompt="...", bearertoken=token)
#
# UDF RUNTIME CONSTRAINTS:
#   - Parameter names CANNOT contain underscores.
#   - @udf.function() must be directly above the def line.
#   - Never call a @udf.function() function from another (coroutine issue).
#   - Available: requests, azure.core, json, re, uuid, fabric.functions
#   - NOT available: azure.identity, msal, notebookutils
# =============================================================================

# -- Configuration ------------------------------------------------------------
API_VERSION = "2024-05-01-preview"
DEFAULT_TIMEOUT = 120   # seconds
POLL_INTERVAL = 3       # seconds between status polls

# =============================================================================
# Internal Helpers (not decorated - safe to call from decorated functions)
# =============================================================================

def get_access_token(bearertoken: str = "") -> str:
    '''
    Description: Returns a valid Fabric API access token using the provided bearer token or auto-detection fallback.

    Args:
    - bearertoken (str): Pre-fetched bearer token from notebook caller. If empty, auto-detection is attempted.

    Returns: str: A valid bearer token string.

    Example:
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        access = get_access_token(bearertoken=token)
    '''
    # Primary: caller-provided token
    if bearertoken and bearertoken.strip():
        logging.info("Using caller-provided bearer token")
        return bearertoken.strip()

    # Fallback: auto-detection (best-effort, usually fails in UDF sandbox)
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
    Description: Parses a Fabric Data Agent URL into workspace ID, artifact ID, and base API URL.

    Args:
    - dataAgentUrl (str): REST API URL or portal URL of the Data Agent.

    Returns: dict: Keys "workspaceId", "artifactId", "baseUrl".

    Example:
        parts = parse_agent_url("https://api.fabric.microsoft.com/v1/workspaces/abc/dataagents/def/aiassistant/openai")
        print(parts["workspaceId"])  # "abc"
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

    # Format 2: Fabric portal URL
    m = re.search(
        r"https://([^/]+)/groups/([^/]+)/aiskills/([^/\?]+)",
        dataAgentUrl
    )
    if m:
        ws, art = m.group(2), m.group(3)
        base = f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/dataagents/{art}/aiassistant/openai"
        return {"workspaceId": ws, "artifactId": art, "baseUrl": base}

    raise ValueError(
        f"Invalid Data Agent URL. Expected REST API or portal format. Got: {dataAgentUrl}"
    )


def build_url(base: str, path: str) -> str:
    '''
    Description: Appends a path and the required api-version query parameter to the base URL.

    Args:
    - base (str): The OpenAI-compatible base URL for the Data Agent.
    - path (str): The API path to append (e.g., "/threads", "/assistants").

    Returns: str: Complete URL with api-version query parameter.

    Example:
        url = build_url("https://api.fabric.microsoft.com/.../openai", "/threads")
        # "https://api.fabric.microsoft.com/.../openai/threads?api-version=2024-05-01-preview"
    '''
    full = f"{base}{path}"
    sep = "&" if "?" in full else "?"
    return f"{full}{sep}api-version={API_VERSION}"


def make_headers(token: str) -> dict:
    '''
    Description: Builds standard HTTP headers for Fabric Data Agent API calls.

    Args:
    - token (str): A valid bearer token for the Fabric API.

    Returns: dict: HTTP headers with Authorization, Content-Type, Accept, and ActivityId.

    Example:
        headers = make_headers("eyJ0eXAiOiJKV1Qi...")
    '''
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "ActivityId": str(uuid.uuid4())
    }


def utcnow() -> str:
    '''
    Description: Returns the current UTC time as a timezone-aware ISO 8601 string.

    Returns: str: ISO formatted UTC timestamp (e.g., "2025-02-22T15:30:00+00:00").

    Example:
        ts = utcnow()  # "2025-02-22T15:30:00.123456+00:00"
    '''
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


# This is the core implementation function. It is intentionally NOT decorated
# with @udf.function() to avoid coroutine wrapper issues when called from
# multiple decorated entry points. All public UDF functions delegate to this.
def call_agent(
    dataAgentUrl: str,
    prompt: str,
    bearertoken: str = "",
    history: list = None,
    includeDetails: bool = False,
    timeoutSeconds: int = DEFAULT_TIMEOUT
) -> dict:
    '''
    Description: Sends a prompt to a Fabric Data Agent using the OpenAI Assistants API pattern.

    Args:
    - dataAgentUrl (str): Published endpoint URL of the Fabric Data Agent.
    - prompt (str): Natural language question to send.
    - bearertoken (str): Pre-fetched bearer token from the notebook caller.
    - history (list): Optional previous messages for multi-turn conversations.
    - includeDetails (bool): If True, includes run steps and generated queries.
    - timeoutSeconds (int): Maximum seconds to wait for completion. Default: 120.

    Returns: dict: Keys "success", "answer", "timestamp", "error" (plus optional "steps",
        "generatedQueries", "conversationHistory").

    Example:
        result = call_agent(
            dataAgentUrl="https://api.fabric.microsoft.com/v1/workspaces/abc/dataagents/def/aiassistant/openai",
            prompt="What were total sales?",
            bearertoken="eyJ0eXAi..."
        )
        print(result["answer"])
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

        # Step 1: Create assistant (required by the API; model value is ignored by Fabric)
        r = requests.post(build_url(base, "/assistants"), headers=headers,
                          json={"model": "not used"}, timeout=30)
        r.raise_for_status()
        assistant_id = r.json().get("id")
        logging.info(f"Assistant: {assistant_id}")

        # Step 2: Create conversation thread
        r = requests.post(build_url(base, "/threads"), headers=headers, timeout=30)
        r.raise_for_status()
        thread_id = r.json().get("id")
        logging.info(f"Thread: {thread_id}")

        # Step 3a: Add conversation history (enables multi-turn follow-ups)
        if history:
            for msg in history:
                requests.post(build_url(base, f"/threads/{thread_id}/messages"),
                              headers=headers, json=msg, timeout=30)

        # Step 3b: Add the current user message
        r = requests.post(build_url(base, f"/threads/{thread_id}/messages"),
                          headers=headers,
                          json={"role": "user", "content": prompt}, timeout=30)
        r.raise_for_status()

        # Step 4: Create run (assistant_id is required)
        r = requests.post(build_url(base, f"/threads/{thread_id}/runs"),
                          headers=headers,
                          json={"assistant_id": assistant_id}, timeout=30)
        r.raise_for_status()
        run_id = r.json().get("id")
        logging.info(f"Run: {run_id}")

        # Step 5: Poll until a terminal state is reached
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

        # Step 6: Retrieve messages in ascending order
        r = requests.get(build_url(base, f"/threads/{thread_id}/messages?order=asc"),
                         headers=headers, timeout=30)
        r.raise_for_status()
        messages = r.json().get("data", [])

        # Extract last assistant message as the answer
        answer = ""
        for msg in reversed(messages):
            if msg.get("role") == "assistant":
                content = msg.get("content", [])
                if isinstance(content, list) and content:
                    answer = content[0].get("text", {}).get("value", "")
                break

        result = {
            "success": True,
            "answer": answer,
            "timestamp": utcnow(),
            "error": None
        }

        # Step 7 (optional): Retrieve run steps and generated queries
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

        # Include updated conversation history for multi-turn calls
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
        # Always clean up the thread to avoid resource leaks
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
# UDF DECORATOR RULES:
#   - @udf.function() must be DIRECTLY above the def line.
#   - Other decorators (@udf.context, @udf.connection) go above @udf.function().
#   - Parameter names CANNOT contain underscores (use "bearertoken" not "bearer_token").
#   - Never call a decorated function from another decorated function.
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
    Description: Sends a natural language prompt to a Fabric Data Agent and returns a structured response.

    Args:
    - dataAgentUrl (str): Published endpoint URL of the Fabric Data Agent.
    - prompt (str): Natural language question to ask the Data Agent.
    - bearertoken (str): Bearer token from notebook. Get via notebookutils.credentials.getToken("https://api.fabric.microsoft.com").
    - includeDetails (bool): If True, includes "steps" and "generatedQueries" in the response. Default: False.
    - timeoutSeconds (int): Maximum seconds to wait for completion. Default: 120.

    Returns: dict: Keys "success" (bool), "answer" (str), "timestamp" (str), "error" (str or None).

    Example:
        # In a Fabric notebook:
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")

        result = notebookutils.udf.run("DataAgentUDF", "ask_agent",
            dataAgentUrl="https://api.fabric.microsoft.com/v1/workspaces/abc-123/dataagents/def-456/aiassistant/openai",
            prompt="What were the total sales last quarter?",
            bearertoken=token,
            includeDetails=True)

        print(result["answer"])

        # When includeDetails=True, generated SQL/DAX/KQL queries are also returned:
        for q in result.get("generatedQueries", []):
            print(f"Generated query: {q['query']}")
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
    Description: Sends a prompt to a Fabric Data Agent and returns only the answer as a string.

    Args:
    - dataAgentUrl (str): Published endpoint URL of the Fabric Data Agent.
    - prompt (str): Natural language question to ask the Data Agent.
    - bearertoken (str): Bearer token from notebook. Get via notebookutils.credentials.getToken("https://api.fabric.microsoft.com").

    Returns: str: The Data Agent answer text, or "Error: ..." if the request failed.

    Example:
        # In a Fabric notebook:
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")

        answer = notebookutils.udf.run("DataAgentUDF", "ask_agent_simple",
            dataAgentUrl="https://api.fabric.microsoft.com/v1/workspaces/abc-123/dataagents/def-456/aiassistant/openai",
            prompt="How many customers placed orders last month?",
            bearertoken=token)

        print(answer)
        # "There were 1,247 customers who placed orders last month."
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
    Description: Sends a prompt with conversation history for multi-turn follow-up questions.

    Args:
    - dataAgentUrl (str): Published endpoint URL of the Fabric Data Agent.
    - prompt (str): Current natural language question to ask.
    - conversationHistory (list): Previous messages as [{"role": "user", "content": "..."}, ...]. Pass [] for first question.
    - bearertoken (str): Bearer token from notebook. Get via notebookutils.credentials.getToken("https://api.fabric.microsoft.com").

    Returns: dict: Keys "success" (bool), "answer" (str), "conversationHistory" (list), "timestamp" (str), "error" (str or None).

    Example:
        # In a Fabric notebook - multi-turn conversation:
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        url = "https://api.fabric.microsoft.com/v1/workspaces/abc-123/dataagents/def-456/aiassistant/openai"

        # First question:
        history = []
        result = notebookutils.udf.run("DataAgentUDF", "ask_agent_with_history",
            dataAgentUrl=url, prompt="What were total sales last quarter?",
            conversationHistory=history, bearertoken=token)
        print(result["answer"])
        history = result["conversationHistory"]

        # Follow-up (uses context from first answer):
        result = notebookutils.udf.run("DataAgentUDF", "ask_agent_with_history",
            dataAgentUrl=url, prompt="Break that down by region",
            conversationHistory=history, bearertoken=token)
        print(result["answer"])
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
    Description: Validates connectivity to a Fabric Data Agent without sending a query.

    Args:
    - dataAgentUrl (str): Published endpoint URL of the Fabric Data Agent.
    - bearertoken (str): Bearer token from notebook. Get via notebookutils.credentials.getToken("https://api.fabric.microsoft.com").

    Returns: dict: Keys "isValid" (bool), "workspaceId" (str), "artifactId" (str), "message" (str), "error" (str or None).

    Example:
        # In a Fabric notebook - test connection before querying:
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")

        check = notebookutils.udf.run("DataAgentUDF", "validate_agent",
            dataAgentUrl="https://api.fabric.microsoft.com/v1/workspaces/abc-123/dataagents/def-456/aiassistant/openai",
            bearertoken=token)

        if check["isValid"]:
            print(f"Connected to workspace {check['workspaceId']}")
        else:
            print(f"Failed: {check['error']}")
    '''
    import requests

    logging.info("Validating Data Agent connection...")

    try:
        url_parts = parse_agent_url(dataAgentUrl)
        base = url_parts["baseUrl"]
        access_token = get_access_token(bearertoken=bearertoken)
        headers = make_headers(access_token)

        # Test connectivity by creating and immediately deleting a thread
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
