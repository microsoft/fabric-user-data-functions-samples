# Select 'Manage connections' and add a connection to a Variable Library
# Replace the alias "<My Variable Library Alias>" with your connection alias.
@udf.connection(argName="varLib", alias="<My Variable Library Alias>")
@udf.function()
def get_variables_from_library(varLib: fn.FabricVariablesClient) -> str:
    '''
    Description: Retrieve API key and configuration values from Fabric Variable Library and construct a readable message.
    
    Args:
        varLib (fn.FabricVariablesClient): Fabric Variable Library connection.
    
    Returns:
        str: Message with API key, environment mode, and region.
        
    Example:
        Returns "Variable Library's API key is 'api-key-value' in 'environment-mode-value' mode of 'region-value' region"
    '''
    # Retrieve all variables from the Variable Library
    variables = varLib.getVariables()
    
    # Access stored configuration values
    api_key = variables["API_KEY"]
    environment_mode = variables["ENVIRONMENT_MODE"]
    region = variables["REGION"]
    
    # Return a human-readable configuration string
    return f"Variable Library's API key is '{api_key}' in '{environment_mode}' mode of '{region}' region"
