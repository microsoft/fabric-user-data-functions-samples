import json

'''
Fabric User Data Function - Data Agent Integration
===================================================
This User Data Function connects to a published Microsoft Fabric Data Agent
and sends natural language prompts to retrieve data-driven insights.

Prerequisites:
- A published Fabric Data Agent with a valid endpoint URL
- User Data Functions tenant setting enabled
- Cross-geo processing/storing for AI enabled (if applicable)
- The invoking user must have access to the underlying data sources
- azure-identity package installed via Manage Libraries (pip: azure-identity)
'''

# =============================================================================
# Configuration Constants
# =============================================================================

# Default timeout for polling the Data Agent response (seconds)
DEFAULT_TIMEOUT_SECONDS = 120

# Polling interval between status checks (seconds)
POLLING_INTERVAL_SECONDS = 3

# Maximum retries for transient failures
MAX_RETRIES = 3


# =============================================================================
# Helper Functions
# =============================================================================

def get_access_token(bearertoken: str = "", resource: str = "https://api.fabric.microsoft.com") -> str:
    '''
    Returns a valid access token for the Fabric API.

    If bearertoken is provided (non-empty), it is returned directly.
    This is the primary path for UDF runtime where the calling notebook
    passes its token via notebookutils.credentials.getToken().

    If bearertoken is empty, attempts auto-detection (best-effort):
    1. notebookutils (Fabric notebooks)
    2. DefaultAzureCredential (azure-identity, if installed)
    3. Managed Identity IMDS endpoint (stdlib only)

    Args:
        bearertoken: A pre-fetched bearer token. If non-empty, used directly.
        resource: The target resource URI for auto-detection fallback.

    Returns:
        str: A valid access token.
    '''
    # If caller passed a token, use it directly
    if bearertoken and bearertoken.strip():
        logging.info("Using caller-provided bearer token")
        return bearertoken.strip()

    # Auto-detection fallback
    errors = []

    try:
        from notebookutils import mssparkutils
        token = mssparkutils.credentials.getToken(resource)
        logging.info("Authenticated via notebookutils")
        return token
    except Exception as e:
        errors.append(f"notebookutils: {e}")

    try:
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()
        token = credential.get_token(f"{resource}/.default")
        logging.info("Authenticated via DefaultAzureCredential")
        return token.token
    except Exception as e:
        errors.append(f"DefaultAzureCredential: {e}")

    try:
        import urllib.request
        imds_url = (
            "http://169.254.169.254/metadata/identity/oauth2/token"
            f"?api-version=2018-02-01&resource={resource}"
        )
        req = urllib.request.Request(imds_url, headers={"Metadata": "true"})
        resp = urllib.request.urlopen(req, timeout=5)
        token_data = json.loads(resp.read().decode("utf-8"))
        token = token_data.get("access_token")
        if token:
            logging.info("Authenticated via IMDS")
            return token
    except Exception as e:
        errors.append(f"IMDS: {e}")

    raise RuntimeError(
        f"No bearer token provided and auto-detection failed.\n"
        f"Details: {'; '.join(errors)}\n\n"
        f"Pass a token from the notebook caller:\n"
        f"  token = notebookutils.credentials.getToken('https://api.fabric.microsoft.com')\n"
        f"  notebookutils.udf.run('MyUDF', 'ask_data_agent_simple',\n"
        f"      dataAgentUrl='...', prompt='...', bearertoken=token)"
    )


def parse_data_agent_url(dataAgentUrl: str) -> dict:
    '''
    Parses a Data Agent URL to extract workspace ID, artifact ID, and the
    OpenAI-compatible API base URL used for thread/message/run operations.

    Supports two URL formats:

    1. REST API endpoint (preferred):
       https://api.fabric.microsoft.com/v1/workspaces/<workspace_id>/dataagents/<artifact_id>/aiassistant/openai

    2. Portal / published URL:
       https://<environment>.fabric.microsoft.com/groups/<workspace_id>/aiskills/<artifact_id>

    Args:
        dataAgentUrl: The Data Agent endpoint URL in either supported format.

    Returns:
        dict: Contains workspaceId, artifactId, and baseUrl (the OpenAI-
              compatible API root to which /threads, /messages, /runs are appended).
    '''
    import re

    # Format 1: REST API endpoint
    # Example: https://api.fabric.microsoft.com/v1/workspaces/{ws}/dataagents/{art}/aiassistant/openai
    api_pattern = r"https://[^/]+/v1/workspaces/([^/]+)/dataagents/([^/]+)/aiassistant/openai"
    match = re.search(api_pattern, dataAgentUrl, re.IGNORECASE)

    if match:
        workspace_id = match.group(1)
        artifact_id = match.group(2)
        # The matched portion IS the base URL — use it directly
        base_url = match.group(0).rstrip("/")
        return {
            "workspaceId": workspace_id,
            "artifactId": artifact_id,
            "baseUrl": base_url
        }

    # Format 2: Portal / published URL
    # Example: https://<env>.fabric.microsoft.com/groups/{ws}/aiskills/{art}
    portal_pattern = r"https://[^/]+/groups/([^/]+)/aiskills/([^/\?]+)"
    match = re.search(portal_pattern, dataAgentUrl)

    if match:
        workspace_id = match.group(1)
        artifact_id = match.group(2)
        # Construct the REST API base URL from the extracted IDs
        base_url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
            f"/dataagents/{artifact_id}/aiassistant/openai"
        )
        return {
            "workspaceId": workspace_id,
            "artifactId": artifact_id,
            "baseUrl": base_url
        }

    raise ValueError(
        f"Invalid Data Agent URL format. Expected one of:\n"
        f"  REST API:  https://api.fabric.microsoft.com/v1/workspaces/<workspace_id>/dataagents/<artifact_id>/aiassistant/openai\n"
        f"  Portal:    https://<env>.fabric.microsoft.com/groups/<workspace_id>/aiskills/<artifact_id>"
    )


def format_agent_response(response: dict, includeDetails: bool = False) -> dict:
    '''
    Formats the Data Agent response into a structured output.

    Args:
        response: The raw response from the Data Agent.
        includeDetails: Whether to include detailed step information.

    Returns:
        dict: Formatted response with answer and optional details.
    '''
    result = {
        "success": True,
        "answer": "",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "generatedQuery": None,
        "dataSource": None
    }

    if isinstance(response, dict):
        # Extract the answer from the response
        if "answer" in response:
            result["answer"] = response["answer"]
        elif "content" in response:
            result["answer"] = response["content"]
        elif "messages" in response:
            messages = response.get("messages", {}).get("data", [])
            assistant_msgs = [m for m in messages if m.get("role") == "assistant"]
            if assistant_msgs:
                last_msg = assistant_msgs[-1]
                if "content" in last_msg:
                    content = last_msg["content"]
                    if isinstance(content, list) and len(content) > 0:
                        result["answer"] = content[0].get("text", {}).get("value", "")
                    else:
                        result["answer"] = str(content)

        # Include additional details if requested
        if includeDetails:
            result["details"] = {
                "runId": response.get("run_id"),
                "threadId": response.get("thread_id"),
                "status": response.get("status"),
                "steps": response.get("run_steps", {}).get("data", [])
            }

            # Extract generated query if available
            steps = response.get("run_steps", {}).get("data", [])
            for step in steps:
                step_details = step.get("step_details", {})
                tool_calls = step_details.get("tool_calls", [])
                for tool_call in tool_calls:
                    if "function" in tool_call:
                        func_args = tool_call["function"].get("arguments", "{}")
                        try:
                            args_dict = json.loads(func_args)
                            if "query" in args_dict:
                                result["generatedQuery"] = args_dict["query"]
                            if "data_source" in args_dict:
                                result["dataSource"] = args_dict["data_source"]
                        except json.JSONDecodeError:
                            pass
    else:
        result["answer"] = str(response)

    return result


# =============================================================================
# Core Implementation (Private - not decorated with @udf.function)
# =============================================================================
# IMPORTANT: The @udf.function() decorator wraps functions for the Fabric
# runtime. Calling a decorated function directly from another decorated
# function returns a coroutine proxy, NOT the raw return value. To enable
# code reuse between UDF entry points, shared logic lives in this private
# helper that both ask_data_agent and ask_data_agent_simple can call safely.
# =============================================================================

def _ask_data_agent_impl(
    dataAgentUrl: str,
    prompt: str,
    bearertoken: str = "",
    includeDetails: bool = False,
    timeoutSeconds: int = 120
) -> dict:
    '''
    Internal implementation for sending a prompt to a Fabric Data Agent.

    Follows the official OpenAI Assistants API pattern required by Fabric:
    create assistant → create thread → add message → create run → poll → read messages → cleanup

    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.
        prompt: The natural language question to ask the Data Agent.
        bearertoken: A pre-fetched bearer token from the notebook caller.
        includeDetails: If True, includes detailed step information.
        timeoutSeconds: Maximum time to wait for a response.

    Returns:
        dict: Response containing success, answer, timestamp, and error fields.
    '''
    import requests
    import uuid

    API_VERSION = "2024-05-01-preview"
    logging.info(f"Processing Data Agent request - Prompt: {prompt[:100]}...")

    thread_id = None  # Track for cleanup

    try:
        # Validate and parse the Data Agent URL
        url_parts = parse_data_agent_url(dataAgentUrl)
        workspace_id = url_parts["workspaceId"]
        artifact_id = url_parts["artifactId"]
        base_url = url_parts["baseUrl"]

        logging.info(f"Connecting to Data Agent - Workspace: {workspace_id}, Artifact: {artifact_id}")
        logging.info(f"API base URL: {base_url}")

        # Get access token for Fabric API
        access_token = get_access_token(bearertoken=bearertoken)

        # Common headers matching official MS sample
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "ActivityId": str(uuid.uuid4())
        }

        def api_url(path: str) -> str:
            '''Build a full API URL with required api-version query parameter.'''
            separator = "&" if "?" in path else "?"
            return f"{base_url}{path}{separator}api-version={API_VERSION}"

        # Step 1: Create an assistant (required by Fabric Data Agent API)
        logging.info("Creating assistant...")
        assistant_response = requests.post(
            api_url("/assistants"),
            headers=headers,
            json={"model": "not used"},
            timeout=30
        )
        assistant_response.raise_for_status()
        assistant_id = assistant_response.json().get("id")
        logging.info(f"Assistant created: {assistant_id}")

        # Step 2: Create a new thread
        logging.info("Creating conversation thread...")
        thread_response = requests.post(
            api_url("/threads"),
            headers=headers,
            timeout=30
        )
        thread_response.raise_for_status()
        thread_id = thread_response.json().get("id")
        logging.info(f"Thread created: {thread_id}")

        # Step 3: Add the user's message to the thread
        logging.info("Sending prompt to Data Agent...")
        message_response = requests.post(
            api_url(f"/threads/{thread_id}/messages"),
            headers=headers,
            json={"role": "user", "content": prompt},
            timeout=30
        )
        message_response.raise_for_status()

        # Step 4: Create a run (with assistant_id)
        logging.info("Starting Data Agent run...")
        run_response = requests.post(
            api_url(f"/threads/{thread_id}/runs"),
            headers=headers,
            json={"assistant_id": assistant_id},
            timeout=30
        )
        run_response.raise_for_status()
        run_id = run_response.json().get("id")
        logging.info(f"Run started: {run_id}")

        # Step 5: Poll for completion
        terminal_states = {"completed", "failed", "cancelled", "requires_action"}
        start_time = time.time()
        status = "queued"

        while status not in terminal_states:
            if time.time() - start_time > timeoutSeconds:
                raise TimeoutError(f"Data Agent response timed out after {timeoutSeconds} seconds")

            time.sleep(POLLING_INTERVAL_SECONDS)

            status_response = requests.get(
                api_url(f"/threads/{thread_id}/runs/{run_id}"),
                headers=headers,
                timeout=30
            )
            status_response.raise_for_status()
            status_data = status_response.json()
            status = status_data.get("status", "unknown")
            logging.info(f"Run status: {status}")

        if status != "completed":
            error_msg = status_data.get("last_error", {}).get("message", f"Run finished with status: {status}")
            raise RuntimeError(error_msg)

        # Step 6: Retrieve the messages (in ascending order)
        logging.info("Retrieving Data Agent response...")
        messages_response = requests.get(
            api_url(f"/threads/{thread_id}/messages?order=asc"),
            headers=headers,
            timeout=30
        )
        messages_response.raise_for_status()

        response_data = {
            "messages": messages_response.json(),
            "run_id": run_id,
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "status": status
        }

        # Step 7: Get run steps if details requested
        if includeDetails:
            steps_response = requests.get(
                api_url(f"/threads/{thread_id}/runs/{run_id}/steps"),
                headers=headers,
                timeout=30
            )
            if steps_response.status_code == 200:
                response_data["run_steps"] = steps_response.json()

        # Format and return the response
        result = format_agent_response(response_data, includeDetails)
        logging.info("Data Agent request completed successfully")

        return result

    except ValueError as ve:
        logging.error(f"Validation error: {ve}")
        return {
            "success": False,
            "answer": None,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "error": str(ve)
        }
    except TimeoutError as te:
        logging.error(f"Timeout error: {te}")
        return {
            "success": False,
            "answer": None,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "error": str(te)
        }
    except Exception as e:
        logging.error(f"Data Agent request failed: {e}")
        return {
            "success": False,
            "answer": None,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "error": str(e)
        }
    finally:
        # Cleanup: delete the thread to avoid resource leaks
        if thread_id:
            try:
                requests.delete(
                    api_url(f"/threads/{thread_id}"),
                    headers=headers,
                    timeout=10
                )
                logging.info(f"Thread {thread_id} cleaned up")
            except Exception:
                logging.warning(f"Failed to clean up thread {thread_id}")


# =============================================================================
# Main User Data Functions (Public UDF Entry Points)
# =============================================================================

@udf.function()
def ask_data_agent(
    dataAgentUrl: str,
    prompt: str,
    bearertoken: str = "",
    includeDetails: bool = False,
    timeoutSeconds: int = 120
) -> dict:
    '''
    Sends a natural language prompt to a published Fabric Data Agent and returns the response.

    This function connects to a Fabric Data Agent using the OpenAI Assistants API pattern,
    creates a thread, sends the user's prompt, and retrieves the generated response.
    The Data Agent will analyze the prompt, determine the appropriate data source,
    generate and execute queries (SQL, DAX, or KQL), and return a natural language answer.

    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.
                      Supports both REST API and portal URL formats.
        prompt: The natural language question to ask the Data Agent.
        bearertoken: A pre-fetched bearer token from the notebook caller.
                     Get it via: notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        includeDetails: If True, includes detailed step information and generated queries.
        timeoutSeconds: Maximum time to wait for a response (default: 120 seconds).

    Returns:
        dict: Response containing:
            - success (bool): Whether the request completed successfully
            - answer (str): The Data Agent's natural language response
            - timestamp (str): ISO timestamp of the response
            - generatedQuery (str|None): The SQL/DAX/KQL query generated (if includeDetails=True)
            - dataSource (str|None): The data source used (if includeDetails=True)
            - details (dict|None): Full run details (if includeDetails=True)
            - error (str|None): Error message if the request failed

    Raises:
        ValueError: If the Data Agent URL format is invalid.
        RuntimeError: If authentication fails or the Data Agent is unreachable.

    Example:
        >>> token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        >>> result = ask_data_agent(
        ...     dataAgentUrl="https://api.fabric.microsoft.com/v1/workspaces/xxx/dataagents/yyy/aiassistant/openai",
        ...     prompt="What were the total sales last quarter?",
        ...     bearertoken=token,
        ...     includeDetails=True
        ... )
        >>> print(result["answer"])
    '''
    return _ask_data_agent_impl(
        dataAgentUrl=dataAgentUrl,
        prompt=prompt,
        bearertoken=bearertoken,
        includeDetails=includeDetails,
        timeoutSeconds=timeoutSeconds
    )


@udf.function()
def ask_data_agent_simple(dataAgentUrl: str, prompt: str, bearertoken: str = "") -> str:
    '''
    Simplified version that sends a prompt to a Data Agent and returns just the answer.

    This is a convenience function for scenarios where you only need the text response
    without additional metadata or error details.

    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.
        prompt: The natural language question to ask the Data Agent.
        bearertoken: A pre-fetched bearer token from the notebook caller.
                     Get it via: notebookutils.credentials.getToken("https://api.fabric.microsoft.com")

    Returns:
        str: The Data Agent's natural language response, or an error message if failed.

    Example:
        >>> token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        >>> answer = ask_data_agent_simple(
        ...     dataAgentUrl="https://api.fabric.microsoft.com/v1/workspaces/xxx/dataagents/yyy/aiassistant/openai",
        ...     prompt="How many customers do we have?",
        ...     bearertoken=token
        ... )
        >>> print(answer)
    '''
    # Call the private helper directly (NOT the @udf.function() decorated version)
    # to avoid the coroutine wrapper that the decorator applies.
    result = _ask_data_agent_impl(
        dataAgentUrl=dataAgentUrl,
        prompt=prompt,
        bearertoken=bearertoken,
        includeDetails=False,
        timeoutSeconds=DEFAULT_TIMEOUT_SECONDS
    )

    if result.get("success"):
        return result.get("answer", "No answer received from Data Agent")
    else:
        return f"Error: {result.get('error', 'Unknown error occurred')}"


@udf.function()
def ask_data_agent_with_context(
    dataAgentUrl: str,
    prompt: str,
    conversationHistory: list,
    bearertoken: str = ""
) -> dict:
    '''
    Sends a prompt to a Data Agent with conversation history for contextual responses.

    This function maintains conversation context by including previous messages,
    enabling follow-up questions and multi-turn conversations with the Data Agent.

    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.
        prompt: The current question to ask the Data Agent.
        conversationHistory: List of previous messages in the format:
                            [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]

    Returns:
        dict: Response containing the answer and updated conversation history.

    Example:
        >>> history = []
        >>> result = ask_data_agent_with_context(
        ...     dataAgentUrl="https://app.fabric.microsoft.com/groups/xxx/aiskills/yyy",
        ...     prompt="What were total sales last month?",
        ...     conversationHistory=history
        ... )
        >>> history = result["conversationHistory"]
        >>> # Follow-up question
        >>> result = ask_data_agent_with_context(
        ...     dataAgentUrl="https://app.fabric.microsoft.com/groups/xxx/aiskills/yyy",
        ...     prompt="How does that compare to the previous month?",
        ...     conversationHistory=history
        ... )
    '''
    logging.info(f"Processing contextual Data Agent request with {len(conversationHistory)} previous messages")

    import requests
    import uuid

    API_VERSION = "2024-05-01-preview"
    thread_id = None

    try:
        url_parts = parse_data_agent_url(dataAgentUrl)
        base_url = url_parts["baseUrl"]
        access_token = get_access_token(bearertoken=bearertoken)

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "ActivityId": str(uuid.uuid4())
        }

        def api_url(path: str) -> str:
            separator = "&" if "?" in path else "?"
            return f"{base_url}{path}{separator}api-version={API_VERSION}"

        # Create assistant
        assistant_response = requests.post(
            api_url("/assistants"),
            headers=headers,
            json={"model": "not used"},
            timeout=30
        )
        assistant_response.raise_for_status()
        assistant_id = assistant_response.json().get("id")

        # Create thread
        thread_response = requests.post(
            api_url("/threads"),
            headers=headers,
            timeout=30
        )
        thread_response.raise_for_status()
        thread_id = thread_response.json().get("id")

        # Add conversation history to the thread
        for message in conversationHistory:
            requests.post(
                api_url(f"/threads/{thread_id}/messages"),
                headers=headers,
                json=message,
                timeout=30
            )

        # Add the current prompt
        requests.post(
            api_url(f"/threads/{thread_id}/messages"),
            headers=headers,
            json={"role": "user", "content": prompt},
            timeout=30
        )

        # Create run with assistant_id
        run_response = requests.post(
            api_url(f"/threads/{thread_id}/runs"),
            headers=headers,
            json={"assistant_id": assistant_id},
            timeout=30
        )
        run_response.raise_for_status()
        run_id = run_response.json().get("id")

        # Poll for completion
        terminal_states = {"completed", "failed", "cancelled", "requires_action"}
        start_time = time.time()
        status = "queued"

        while status not in terminal_states:
            if time.time() - start_time > DEFAULT_TIMEOUT_SECONDS:
                raise TimeoutError("Data Agent response timed out")
            time.sleep(POLLING_INTERVAL_SECONDS)
            status_response = requests.get(
                api_url(f"/threads/{thread_id}/runs/{run_id}"),
                headers=headers,
                timeout=30
            )
            status_response.raise_for_status()
            status = status_response.json().get("status", "unknown")

        if status != "completed":
            raise RuntimeError(f"Run finished with status: {status}")

        # Get messages in ascending order
        messages_response = requests.get(
            api_url(f"/threads/{thread_id}/messages?order=asc"),
            headers=headers,
            timeout=30
        )
        messages_response.raise_for_status()
        messages_data = messages_response.json()

        # Extract the assistant's response
        messages_list = messages_data.get("data", [])
        assistant_msgs = [m for m in messages_list if m.get("role") == "assistant"]

        answer = ""
        if assistant_msgs:
            last_msg = assistant_msgs[-1]
            content = last_msg.get("content", [])
            if isinstance(content, list) and len(content) > 0:
                answer = content[0].get("text", {}).get("value", "")

        # Update conversation history
        updated_history = conversationHistory.copy()
        updated_history.append({"role": "user", "content": prompt})
        updated_history.append({"role": "assistant", "content": answer})

        return {
            "success": True,
            "answer": answer,
            "conversationHistory": updated_history,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

    except Exception as e:
        logging.error(f"Contextual Data Agent request failed: {e}")
        return {
            "success": False,
            "answer": None,
            "conversationHistory": conversationHistory,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "error": str(e)
        }
    finally:
        if thread_id:
            try:
                requests.delete(api_url(f"/threads/{thread_id}"), headers=headers, timeout=10)
            except Exception:
                pass


@udf.function()
def validate_data_agent_connection(dataAgentUrl: str, bearertoken: str = "") -> dict:
    '''
    Validates connectivity to a Fabric Data Agent without sending a query.

    Use this function to verify that the Data Agent is accessible and the user
    has appropriate permissions before sending actual queries.

    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.

    Returns:
        dict: Validation result containing:
            - isValid (bool): Whether the connection is valid
            - workspaceId (str): Extracted workspace ID
            - artifactId (str): Extracted artifact ID
            - message (str): Status message
            - error (str|None): Error details if validation failed

    Example:
        >>> result = validate_data_agent_connection(
        ...     dataAgentUrl="https://app.fabric.microsoft.com/groups/xxx/aiskills/yyy"
        ... )
        >>> if result["isValid"]:
        ...     print("Connection validated successfully")
    '''
    logging.info("Validating Data Agent connection...")

    try:
        # Parse and validate URL format
        url_parts = parse_data_agent_url(dataAgentUrl)

        # Test authentication
        access_token = get_access_token(bearertoken=bearertoken)

        import requests
        import uuid

        API_VERSION = "2024-05-01-preview"
        workspace_id = url_parts["workspaceId"]
        artifact_id = url_parts["artifactId"]
        base_url = url_parts["baseUrl"]

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "ActivityId": str(uuid.uuid4())
        }

        def api_url(path: str) -> str:
            separator = "&" if "?" in path else "?"
            return f"{base_url}{path}{separator}api-version={API_VERSION}"

        # Test the connection by creating and immediately deleting a thread
        test_response = requests.post(
            api_url("/threads"),
            headers=headers,
            timeout=30
        )

        if test_response.status_code == 200:
            thread_id = test_response.json().get("id")
            logging.info(f"Connection validated - Test thread: {thread_id}")

            # Clean up the test thread
            try:
                requests.delete(api_url(f"/threads/{thread_id}"), headers=headers, timeout=10)
            except Exception:
                pass

            return {
                "isValid": True,
                "workspaceId": workspace_id,
                "artifactId": artifact_id,
                "message": "Data Agent connection validated successfully",
                "error": None
            }
        else:
            return {
                "isValid": False,
                "workspaceId": workspace_id,
                "artifactId": artifact_id,
                "message": f"Connection failed with status code: {test_response.status_code}",
                "error": test_response.text
            }

    except ValueError as ve:
        return {
            "isValid": False,
            "workspaceId": None,
            "artifactId": None,
            "message": "Invalid Data Agent URL format",
            "error": str(ve)
        }
    except Exception as e:
        return {
            "isValid": False,
            "workspaceId": url_parts.get("workspaceId") if 'url_parts' in locals() else None,
            "artifactId": url_parts.get("artifactId") if 'url_parts' in locals() else None,
            "message": "Connection validation failed",
            "error": str(e)
        }
