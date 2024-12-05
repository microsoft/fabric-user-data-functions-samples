# This is an empty function definition which takes a text string input and returns a string output
# Provide input and output data types. <link to public doc on data types supported>
# Example usage as shown below

@udf.function()
# replace <function-name> with a new function name 
def <function-name>(text: str) -> str:
    # use logging.info() to log any information
    logging.info('Python UDF trigger function processed a request.')

    # Add your custom code here

    return f"Python UDF was executed successfully!"
