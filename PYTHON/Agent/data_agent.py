import datetime
import fabric.functions as fn
import logging
import json
import time
from typing import Optional

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

Author: Microsoft Unified Data Platform Engineer
Version: 1.0.0
'''

# Initialize the User Data Functions execution context
udf = fn.UserDataFunctions()


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

def get_access_token(audience: str = "https://analysis.windows.net/powerbi/api") -> str:
    '''
    Retrieves an access token for the Fabric API using the executing user's identity.
    This helper function uses the notebookutils library available in Fabric runtime.
    
    Args:
        audience: The resource audience for the token request.
        
    Returns:
        str: The access token for API authentication.
    '''
    try:
        from notebookutils import mssparkutils
        token = mssparkutils.credentials.getToken(audience)
        return token
    except ImportError:
        logging.warning("notebookutils not available - using alternative auth method")
        raise RuntimeError("Authentication requires Fabric runtime environment")


def parse_data_agent_url(dataAgentUrl: str) -> dict:
    '''
    Parses the Data Agent published URL to extract workspace and artifact IDs.
    
    Expected URL format:
    https://<environment>.fabric.microsoft.com/groups/<workspace_id>/aiskills/<artifact_id>
    
    Args:
        dataAgentUrl: The published Data Agent endpoint URL.
        
    Returns:
        dict: Contains workspaceId and artifactId.
    '''
    import re
    
    pattern = r"https://[^/]+/groups/([^/]+)/aiskills/([^/\?]+)"
    match = re.search(pattern, dataAgentUrl)
    
    if not match:
        raise ValueError(
            f"Invalid Data Agent URL format. Expected: "
            f"https://<env>.fabric.microsoft.com/groups/<workspace_id>/aiskills/<artifact_id>"
        )
    
    return {
        "workspaceId": match.group(1),
        "artifactId": match.group(2)
    }


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
        "timestamp": datetime.datetime.utcnow().isoformat(),
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
# Main User Data Functions
# =============================================================================

@udf.function()
def ask_data_agent(
    dataAgentUrl: str,
    prompt: str,
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
                      Format: https://<env>.fabric.microsoft.com/groups/<workspace_id>/aiskills/<artifact_id>
        prompt: The natural language question to ask the Data Agent.
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
        >>> result = ask_data_agent(
        ...     dataAgentUrl="https://app.fabric.microsoft.com/groups/xxx/aiskills/yyy",
        ...     prompt="What were the total sales last quarter?",
        ...     includeDetails=True
        ... )
        >>> print(result["answer"])
    '''
    logging.info(f"Processing Data Agent request - Prompt: {prompt[:100]}...")
    
    try:
        # Validate and parse the Data Agent URL
        url_parts = parse_data_agent_url(dataAgentUrl)
        workspace_id = url_parts["workspaceId"]
        artifact_id = url_parts["artifactId"]
        
        logging.info(f"Connecting to Data Agent - Workspace: {workspace_id}, Artifact: {artifact_id}")
        
        # Get access token for Fabric API
        access_token = get_access_token()
        
        # Import required libraries
        import requests
        
        # Construct the API endpoint
        base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataAgents/{artifact_id}"
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Step 1: Create a new thread
        logging.info("Creating new conversation thread...")
        thread_response = requests.post(
            f"{base_url}/threads",
            headers=headers,
            timeout=30
        )
        thread_response.raise_for_status()
        thread_data = thread_response.json()
        thread_id = thread_data.get("id")
        
        logging.info(f"Thread created: {thread_id}")
        
        # Step 2: Add the user's message to the thread
        logging.info("Sending prompt to Data Agent...")
        message_payload = {
            "role": "user",
            "content": prompt
        }
        
        message_response = requests.post(
            f"{base_url}/threads/{thread_id}/messages",
            headers=headers,
            json=message_payload,
            timeout=30
        )
        message_response.raise_for_status()
        
        # Step 3: Create a run to process the message
        logging.info("Starting Data Agent run...")
        run_response = requests.post(
            f"{base_url}/threads/{thread_id}/runs",
            headers=headers,
            json={},
            timeout=30
        )
        run_response.raise_for_status()
        run_data = run_response.json()
        run_id = run_data.get("id")
        
        logging.info(f"Run started: {run_id}")
        
        # Step 4: Poll for completion
        start_time = time.time()
        status = "queued"
        
        while status in ["queued", "in_progress", "requires_action"]:
            if time.time() - start_time > timeoutSeconds:
                raise TimeoutError(f"Data Agent response timed out after {timeoutSeconds} seconds")
            
            time.sleep(POLLING_INTERVAL_SECONDS)
            
            status_response = requests.get(
                f"{base_url}/threads/{thread_id}/runs/{run_id}",
                headers=headers,
                timeout=30
            )
            status_response.raise_for_status()
            status_data = status_response.json()
            status = status_data.get("status", "unknown")
            
            logging.info(f"Run status: {status}")
        
        if status != "completed":
            error_msg = status_data.get("last_error", {}).get("message", f"Run failed with status: {status}")
            raise RuntimeError(error_msg)
        
        # Step 5: Retrieve the messages
        logging.info("Retrieving Data Agent response...")
        messages_response = requests.get(
            f"{base_url}/threads/{thread_id}/messages",
            headers=headers,
            timeout=30
        )
        messages_response.raise_for_status()
        
        response_data = {
            "messages": messages_response.json(),
            "run_id": run_id,
            "thread_id": thread_id,
            "status": status
        }
        
        # Step 6: Get run steps if details requested
        if includeDetails:
            steps_response = requests.get(
                f"{base_url}/threads/{thread_id}/runs/{run_id}/steps",
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
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": str(ve)
        }
    except TimeoutError as te:
        logging.error(f"Timeout error: {te}")
        return {
            "success": False,
            "answer": None,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": str(te)
        }
    except Exception as e:
        logging.error(f"Data Agent request failed: {e}")
        return {
            "success": False,
            "answer": None,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": str(e)
        }


@udf.function()
def ask_data_agent_simple(dataAgentUrl: str, prompt: str) -> str:
    '''
    Simplified version that sends a prompt to a Data Agent and returns just the answer.
    
    This is a convenience function for scenarios where you only need the text response
    without additional metadata or error details.
    
    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.
        prompt: The natural language question to ask the Data Agent.
        
    Returns:
        str: The Data Agent's natural language response, or an error message if failed.
        
    Example:
        >>> answer = ask_data_agent_simple(
        ...     dataAgentUrl="https://app.fabric.microsoft.com/groups/xxx/aiskills/yyy",
        ...     prompt="How many customers do we have?"
        ... )
        >>> print(answer)
    '''
    result = ask_data_agent(
        dataAgentUrl=dataAgentUrl,
        prompt=prompt,
        includeDetails=False,
        timeoutSeconds=120
    )
    
    if result.get("success"):
        return result.get("answer", "No answer received from Data Agent")
    else:
        return f"Error: {result.get('error', 'Unknown error occurred')}"


@udf.function()
def ask_data_agent_with_context(
    dataAgentUrl: str,
    prompt: str,
    conversationHistory: list
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
    
    try:
        # Validate and parse the Data Agent URL
        url_parts = parse_data_agent_url(dataAgentUrl)
        workspace_id = url_parts["workspaceId"]
        artifact_id = url_parts["artifactId"]
        
        # Get access token
        access_token = get_access_token()
        
        import requests
        
        base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataAgents/{artifact_id}"
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Create a new thread
        thread_response = requests.post(
            f"{base_url}/threads",
            headers=headers,
            timeout=30
        )
        thread_response.raise_for_status()
        thread_id = thread_response.json().get("id")
        
        # Add conversation history to the thread
        for message in conversationHistory:
            requests.post(
                f"{base_url}/threads/{thread_id}/messages",
                headers=headers,
                json=message,
                timeout=30
            )
        
        # Add the current prompt
        requests.post(
            f"{base_url}/threads/{thread_id}/messages",
            headers=headers,
            json={"role": "user", "content": prompt},
            timeout=30
        )
        
        # Create and run
        run_response = requests.post(
            f"{base_url}/threads/{thread_id}/runs",
            headers=headers,
            json={},
            timeout=30
        )
        run_response.raise_for_status()
        run_id = run_response.json().get("id")
        
        # Poll for completion
        start_time = time.time()
        status = "queued"
        
        while status in ["queued", "in_progress", "requires_action"]:
            if time.time() - start_time > DEFAULT_TIMEOUT_SECONDS:
                raise TimeoutError("Data Agent response timed out")
            
            time.sleep(POLLING_INTERVAL_SECONDS)
            
            status_response = requests.get(
                f"{base_url}/threads/{thread_id}/runs/{run_id}",
                headers=headers,
                timeout=30
            )
            status_response.raise_for_status()
            status = status_response.json().get("status", "unknown")
        
        if status != "completed":
            raise RuntimeError(f"Run failed with status: {status}")
        
        # Get messages
        messages_response = requests.get(
            f"{base_url}/threads/{thread_id}/messages",
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
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Contextual Data Agent request failed: {e}")
        return {
            "success": False,
            "answer": None,
            "conversationHistory": conversationHistory,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": str(e)
        }


@udf.function()
def validate_data_agent_connection(dataAgentUrl: str) -> dict:
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
        access_token = get_access_token()
        
        import requests
        
        workspace_id = url_parts["workspaceId"]
        artifact_id = url_parts["artifactId"]
        
        # Attempt to create a thread as a connectivity test
        base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataAgents/{artifact_id}"
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Test the connection with a simple API call
        test_response = requests.post(
            f"{base_url}/threads",
            headers=headers,
            timeout=30
        )
        
        if test_response.status_code == 200:
            # Clean up the test thread
            thread_id = test_response.json().get("id")
            logging.info(f"Connection validated - Test thread: {thread_id}")
            
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
