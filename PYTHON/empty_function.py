# This is an empty function definition
# Provide input and output data types. <link to public doc on data types supported>
# Example usage as shown below

@app.function("")
# replace <function-name> with a new function name 
def <function-name>(name: str) -> str:
    # use logging.info() to log any information
    logging.info('Python UDF trigger function processed a request.')

    # Add your custom code here

    return f"Python UDF was executed successfully!"
