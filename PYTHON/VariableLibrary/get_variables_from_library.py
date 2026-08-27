# Select 'Manage connections' and add a connection to a Variable Library
# Replace the alias "<My Variable Library Alias>" with your connection alias.
from datetime import datetime
@udf.connection(argName="varLib", alias="<My Variable Library Alias>")
@udf.function()
def standardize_date(rawDate: str, varLib: fn.FabricVariablesClient) -> str:
    '''
    Description: Standardize date format using configuration from Variable Library before data ingestion.
    
    Args:
        rawDate (str): Raw date string in desired format.
        varLib (fn.FabricVariablesClient): Fabric Variable Library connection.
    
    Returns:
        str: Standardized date in the format specified in Variable Library.
        
    Example:
        Assumes Variable Library contains: DATE_FORMAT = "%Y-%m-%d"
        standardize_date("15/10/2025", varLib) returns "Standardized Date: 2025-10-15"
    '''
    # Retrieve all variables from the Variable Library
    variables = varLib.getVariables()

    # Get desired format from environment or use default
    date_format = variables["DATE_FORMAT"]

    try:
        # Assume input is DD/MM/YYYY
        parsed_date = datetime.strptime(rawDate, "%d/%m/%Y")
        # Convert to standardized format from Variable Library
        standardized_date = parsed_date.strftime(date_format)
    except ValueError:
        standardized_date = "Invalid Date"

    return f"Standardized Date: {standardized_date}"

