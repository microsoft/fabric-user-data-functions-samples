import datetime

@udf.context(argName="udfContext")
@udf.function()
def get_function_invocation_details(udfContext: fn.UserDataFunctionContext) -> str:
    '''
    Description: Get function invocation details including user info and invocation ID.
    
    Args:
        udfContext (fn.UserDataFunctionContext): Context containing invocation metadata.
    
    Returns:
        str: Welcome message with username, timestamp, and invocation ID.
        
    Example:
       Returns "Welcome to Fabric Functions, user@example.com, at 2025-07-01 10:30:00! Invocation ID: abc123"
    '''
    invocation_id = udfContext.invocation_id
    invoking_users_username = udfContext.executing_user['PreferredUsername']
    # Other executing_user keys include: 'Oid', 'TenantId'
 
    return f"Welcome to Fabric Functions, {invoking_users_username}, at {datetime.datetime.now()}! Invocation ID: {invocation_id}"
