# Select 'Manage connections' and add a connection to a Variable Library
# Replace the alias "<My Variable Library Alias>" with your connection alias.
from openai import AzureOpenAI
from azure.keyvault.secrets import SecretClient

udf = fn.UserDataFunctions()

@udf.generic_connection(argName="keyVaultClient", audienceType="KeyVault")
@udf.connection(argName="varLib", alias="<My Variable Library Alias>")
@udf.function()
def chat_request(prompt: str, keyVaultClient: fn.FabricItem, varLib: fn.FabricVariablesClient) -> str:   
    '''
    Description: Sends a chat completion request to an Azure OpenAI model using configuration values 
    retrieved from a Fabric Variable Library and Azure Key Vault.
    
        Pre-requisites: 
                * Create an Azure OpenAI endpoint in Azure Portal
                * Create an Azure Key Vault and store your Azure OpenAI API key as a secret
                * Grant your Fabric User Data Functions item owner's identity access to read secrets (Access policies or RBAC). Guidance: https://learn.microsoft.com/en-us/fabric/data-factory/azure-key-vault-reference-overview
                * Create a Variable Library in Fabric and add variables for:
                    - KEY_VAULT_URL: Your Azure Key Vault URL (e.g., "https://your-keyvault.vault.azure.net/")
                    - API_KEY_SECRET_NAME: Name of the secret in Key Vault containing the API key
                    - ENDPOINT: Your Azure OpenAI endpoint URL
                    - MODEL: Your deployed model name
                * Add the openai and azure-keyvault-secrets libraries to your function dependencies
                * Ensure fabric-user-data-functions library is using the latest version
    
    Args:
        prompt (str): The user input or query to be processed by the model.
        varLib (fn.FabricVariablesClient): A client instance to access stored variables in Variable Library
            for Key Vault URL, secret name, endpoint, and model name.

    Returns:
        str: The generated response from the Azure OpenAI model.

    Workflow:
        1. Fetch Key Vault URL, secret name, endpoint, and model details from Variable Library.
        2. Use the generic Key Vault connection to obtain an access token (managed by Fabric).
        3. Retrieve the API key securely from Azure Key Vault using SecretClient.
        4. Initialize the AzureOpenAI client with the retrieved configuration.
        5. Send a chat completion request with the given prompt and system instructions.
        6. Return the content of the first message in the response.
        
    Example:
        Assumes Variable Library contains:
        - KEY_VAULT_URL = "https://my-keyvault.vault.azure.net/"
        - API_KEY_SECRET_NAME = "openai-api-key"
        - ENDPOINT = "https://your-resource.openai.azure.com/"
        - MODEL = "gpt-4"
        
        chat_request("What is Microsoft Fabric?", varLib) returns AI-generated response
    '''

    # Retrieve configuration from Variable Library
    variables = varLib.getVariables()
    key_vault_url = variables["KEY_VAULT_URL"]
    api_key_secret_name = variables["API_KEY_SECRET_NAME"]
    endpoint = variables["ENDPOINT"]
    model_name = variables["MODEL"]

    # Obtain a credential from the generic Key Vault connection (Fabric-managed identity)
    credential = keyVaultClient.get_access_token()
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    key = secret_client.get_secret(api_key_secret_name).value

    api_version = "2024-12-01-preview"

    client = AzureOpenAI(
        api_version=api_version,
        azure_endpoint=endpoint,
        api_key=key,
    )

    response = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant.",
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        max_completion_tokens=13107,
        temperature=1.0,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0,
        model=model_name
    )

    return (response.choices[0].message.content)
