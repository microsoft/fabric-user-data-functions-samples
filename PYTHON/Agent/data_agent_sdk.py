import datetime
import fabric.functions as fn
import logging
from typing import Optional

'''
Fabric User Data Function - Data Agent Integration (SDK Version)
================================================================
This User Data Function uses the official fabric-data-agent-client SDK
to connect to a published Microsoft Fabric Data Agent.

Note: This version requires the fabric-data-agent-client package.
Install it using: pip install fabric-data-agent-client

Prerequisites:
- fabric-data-agent-client package installed
- A published Fabric Data Agent with a valid endpoint URL
- User Data Functions tenant setting enabled
- The invoking user must have access to the underlying data sources

'''

# Initialize the User Data Functions execution context
udf = fn.UserDataFunctions()


# =============================================================================
# SDK-Based Functions
# =============================================================================

@udf.function()
def ask_agent_sdk(dataAgentUrl: str, tenantId: str, prompt: str) -> dict:
    '''
    Sends a prompt to a Fabric Data Agent using the official SDK.
    
    This function uses the fabric-data-agent-client SDK which provides
    a simplified interface for interacting with Fabric Data Agents.
    
    IMPORTANT: This function requires the fabric-data-agent-client package.
    Add it to your User Data Functions library management:
        pip install fabric-data-agent-client
    
    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.
        tenantId: Your Azure Active Directory tenant ID.
        prompt: The natural language question to ask the Data Agent.
        
    Returns:
        dict: Response containing:
            - success (bool): Whether the request completed successfully
            - answer (str): The Data Agent's natural language response
            - timestamp (str): ISO timestamp of the response
            - error (str|None): Error message if the request failed
            
    Example:
        >>> result = ask_agent_sdk(
        ...     dataAgentUrl="https://app.fabric.microsoft.com/groups/xxx/aiskills/yyy",
        ...     tenantId="your-tenant-id",
        ...     prompt="What were the total sales last quarter?"
        ... )
        >>> print(result["answer"])
   '''
    logging.info(f"Processing SDK-based Data Agent request - Prompt: {prompt[:100]}...")
    
    try:
        # Import the SDK (must be installed via Library Management)
        from fabric_data_agent_client import FabricDataAgentClient
        from azure.identity import DefaultAzureCredential
        
        # Use DefaultAzureCredential which works in Fabric runtime
        # This will use the managed identity or user credentials
        credential = DefaultAzureCredential()
        
        # Create the client
        client = FabricDataAgentClient(
            credential=credential,
            tenant_id=tenantId,
            data_agent_url=dataAgentUrl
        )
        
        logging.info("Sending prompt to Data Agent via SDK...")
        
        # Ask the Data Agent
        response = client.ask(prompt)
        
        logging.info("Data Agent request completed successfully")
        
        return {
            "success": True,
            "answer": str(response),
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": None
        }
        
    except ImportError as ie:
        error_msg = (
            "fabric-data-agent-client package not installed. "
            "Please add it via Library Management: pip install fabric-data-agent-client"
        )
        logging.error(error_msg)
        return {
            "success": False,
            "answer": None,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": error_msg
        }
    except Exception as e:
        logging.error(f"SDK Data Agent request failed: {e}")
        return {
            "success": False,
            "answer": None,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": str(e)
        }


@udf.function()
def ask_agent_sdk_with_details(
    dataAgentUrl: str, 
    tenantId: str, 
    prompt: str
) -> dict:
    '''
    Sends a prompt to a Fabric Data Agent and returns detailed run information.
    
    This function provides additional transparency by including the steps
    the Data Agent took to arrive at the answer, including any generated queries.
    
    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.
        tenantId: Your Azure Active Directory tenant ID.
        prompt: The natural language question to ask the Data Agent.
        
    Returns:
        dict: Response containing:
            - success (bool): Whether the request completed successfully
            - answer (str): The Data Agent's natural language response
            - steps (list): List of steps the agent took
            - generatedQueries (list): Any SQL/DAX/KQL queries generated
            - timestamp (str): ISO timestamp of the response
            - error (str|None): Error message if the request failed
    '''
    logging.info(f"Processing SDK-based Data Agent request with details...")
    
    try:
        from fabric_data_agent_client import FabricDataAgentClient
        from azure.identity import DefaultAzureCredential
        
        credential = DefaultAzureCredential()
        
        client = FabricDataAgentClient(
            credential=credential,
            tenant_id=tenantId,
            data_agent_url=dataAgentUrl
        )
        
        # Get the response
        response = client.ask(prompt)
        
        # Get detailed run information
        run_details = client.get_run_details(prompt)
        
        # Extract messages
        messages = run_details.get('messages', {}).get('data', [])
        assistant_messages = [msg for msg in messages if msg.get('role') == 'assistant']
        
        # Extract the answer from the last assistant message
        answer = ""
        if assistant_messages:
            last_msg = assistant_messages[-1]
            content = last_msg.get('content', [])
            if isinstance(content, list) and len(content) > 0:
                answer = content[0].get('text', {}).get('value', str(response))
            else:
                answer = str(response)
        else:
            answer = str(response)
        
        # Extract steps and queries
        steps = []
        generated_queries = []
        
        run_steps = run_details.get('run_steps', {}).get('data', [])
        for step in run_steps:
            tool_name = "N/A"
            step_details = step.get('step_details', {})
            tool_calls = step_details.get('tool_calls', [])
            
            for tool_call in tool_calls:
                if 'function' in tool_call:
                    tool_name = tool_call['function'].get('name', 'N/A')
                    
                    # Extract any generated queries
                    try:
                        import json
                        args = json.loads(tool_call['function'].get('arguments', '{}'))
                        if 'query' in args:
                            generated_queries.append({
                                "tool": tool_name,
                                "query": args['query']
                            })
                    except:
                        pass
            
            steps.append({
                "stepId": step.get('id'),
                "type": step.get('type'),
                "status": step.get('status'),
                "toolName": tool_name,
                "error": step.get('error')
            })
        
        logging.info("Data Agent request with details completed successfully")
        
        return {
            "success": True,
            "answer": answer,
            "steps": steps,
            "generatedQueries": generated_queries,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": None
        }
        
    except ImportError:
        return {
            "success": False,
            "answer": None,
            "steps": [],
            "generatedQueries": [],
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": "fabric-data-agent-client package not installed"
        }
    except Exception as e:
        logging.error(f"SDK Data Agent request failed: {e}")
        return {
            "success": False,
            "answer": None,
            "steps": [],
            "generatedQueries": [],
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": str(e)
        }


# =============================================================================
# OpenAI API Pattern (Alternative Approach)
# =============================================================================

@udf.function()
def ask_agent_openai_pattern(
    dataAgentUrl: str,
    prompt: str,
    timeoutSeconds: int = 120
) -> dict:
    '''
    Sends a prompt to a Fabric Data Agent using the OpenAI Assistants API pattern.
    
    This function implements the same pattern used by the Azure OpenAI Assistant API,
    which is the underlying technology for Fabric Data Agents. It creates a thread,
    adds a message, runs the assistant, and retrieves the response.
    
    Args:
        dataAgentUrl: The published endpoint URL of the Fabric Data Agent.
        prompt: The natural language question to ask the Data Agent.
        timeoutSeconds: Maximum time to wait for a response (default: 120 seconds).
        
    Returns:
        dict: Response containing the answer and metadata.
        
    Example:
        >>> result = ask_agent_openai_pattern(
        ...     dataAgentUrl="https://app.fabric.microsoft.com/groups/xxx/aiskills/yyy",
        ...     prompt="Show me the top 10 products by revenue"
        ... )
    '''
    logging.info(f"Processing Data Agent request using OpenAI pattern...")
    
    try:
        import requests
        import time
        import re
        
        # Parse the Data Agent URL
        pattern = r"https://([^/]+)/groups/([^/]+)/aiskills/([^/\?]+)"
        match = re.search(pattern, dataAgentUrl)
        
        if not match:
            raise ValueError("Invalid Data Agent URL format")
        
        environment = match.group(1)
        workspace_id = match.group(2)
        artifact_id = match.group(3)
        
        # Get access token using notebookutils
        from notebookutils import mssparkutils
        access_token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
        
        # Construct base URL - using the Fabric REST API pattern
        base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataAgents/{artifact_id}"
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Step 1: Create thread
        thread_resp = requests.post(f"{base_url}/threads", headers=headers, timeout=30)
        thread_resp.raise_for_status()
        thread_id = thread_resp.json()["id"]
        
        # Step 2: Add message
        msg_payload = {"role": "user", "content": prompt}
        requests.post(f"{base_url}/threads/{thread_id}/messages", headers=headers, json=msg_payload, timeout=30)
        
        # Step 3: Create run
        run_resp = requests.post(f"{base_url}/threads/{thread_id}/runs", headers=headers, json={}, timeout=30)
        run_resp.raise_for_status()
        run_id = run_resp.json()["id"]
        
        # Step 4: Poll for completion
        start = time.time()
        while True:
            if time.time() - start > timeoutSeconds:
                raise TimeoutError(f"Request timed out after {timeoutSeconds} seconds")
            
            time.sleep(3)
            status_resp = requests.get(f"{base_url}/threads/{thread_id}/runs/{run_id}", headers=headers, timeout=30)
            status = status_resp.json().get("status")
            
            if status == "completed":
                break
            elif status in ["failed", "cancelled", "expired"]:
                raise RuntimeError(f"Run failed with status: {status}")
        
        # Step 5: Get messages
        msgs_resp = requests.get(f"{base_url}/threads/{thread_id}/messages", headers=headers, timeout=30)
        messages = msgs_resp.json().get("data", [])
        
        # Find the assistant's response
        answer = ""
        for msg in messages:
            if msg.get("role") == "assistant":
                content = msg.get("content", [])
                if isinstance(content, list) and content:
                    answer = content[0].get("text", {}).get("value", "")
                break
        
        return {
            "success": True,
            "answer": answer,
            "threadId": thread_id,
            "runId": run_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": None
        }
        
    except Exception as e:
        logging.error(f"OpenAI pattern request failed: {e}")
        return {
            "success": False,
            "answer": None,
            "threadId": None,
            "runId": None,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": str(e)
        }
