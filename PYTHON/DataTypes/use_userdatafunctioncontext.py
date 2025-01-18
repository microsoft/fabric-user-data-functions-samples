# This sample uses a parameter with data type UserDataFunctionContext,
# which is a parameter that contains certain metadata about the function
# invocation, such as the invocation id and some properties of the token
# used to invoke the function.

import datetime

@udf.context(argName="udfContext")
@udf.function()
def hello_fabric_with_userdatafunctioncontext(udfContext: fn.UserDataFunctionContext)-> str:
    invocation_id = udfContext.invocation_id
    invoking_users_username = udfContext.executing_user['PreferredUsername']
    # Other executing_user keys include: 'Oid', 'TenantId'
 
    return f"Welcome to Fabric Functions, {invoking_users_username}, at {datetime.datetime.now()}! Invocation ID: {invocation_id}"