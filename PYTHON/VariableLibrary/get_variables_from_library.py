# Select 'Manage connections' and add a connection to a Variable Library
# Replace the alias "<My Variable Library Alias>" with your connection alias.
@udf.connection(argName="varLib", alias="<My Variable Library Alias>")
@udf.function()
def get_variables_from_library(varLib: fn.FabricVariablesClient) -> str:
    '''
    Description: Retrieve configuration values from Fabric Variable Library. Replace variable names with your own stored values.
    
    Args:
        varLib (fn.FabricVariablesClient): Fabric Variable Library connection.
    
    Returns:
        str: Message with retrieved variable values.
        
    Example:
        Assumes Variable Library contains: API_KEY, ENVIRONMENT_MODE, REGION
        Returns "Variable Library's API key is 'api-key-value' in 'environment-mode-value' mode of 'region-value' region"
    '''
    # Retrieve all variables from the Variable Library
    variables = varLib.getVariables()
    
    # Access stored configuration values - replace these variable names with your own
    api_key = variables["API_KEY"]  # Replace "API_KEY" with your variable name
    environment_mode = variables["ENVIRONMENT_MODE"]  # Replace "ENVIRONMENT_MODE" with your variable name
    region = variables["REGION"]  # Replace "REGION" with your variable name
    
    # Return a configuration string containing variable library values
    return f"Variable Library's API key is '{api_key}' in '{environment_mode}' mode of '{region}' region"
