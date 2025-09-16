
# This sample reads a CSV file from a lakehouse using pandas. Function takes file name as an input parameter
# Complete these steps before testing this funtion 
#   1. Select Manage connections to connect to Lakehouse. 
#   2. Select Library Management and add pandas library 

import pandas as pd 

# Replace the alias "<My Lakehouse alias>" with your connection alias.
@udf.connection(argName="myLakehouse", alias="<My Lakehouse alias>")
@udf.function()
async def read_csv_from_lakehouse(myLakehouse: fn.FabricLakehouseClient, csvFileName: str) -> str:
    '''
    Description: Read CSV file from lakehouse and return data as formatted string.
    
    Args:
        myLakehouse (fn.FabricLakehouseClient): Fabric lakehouse connection.
        csvFileName (str): CSV file name in the lakehouse Files folder.
    
    Returns: 
        str: Confirmation message with formatted CSV data rows.
    '''  

    # Connect to the Lakehouse
    connection = myLakehouse.connectToFilesAsync()   

    # Download the CSV file from the Lakehouse
    csvFile = connection.get_file_client(csvFileName)

    downloadFile = await csvFile.download_file()
    csvData = await downloadFile.readall()
    
    # Read the CSV data into a pandas DataFrame
    from io import StringIO
    df = pd.read_csv(StringIO(csvData.decode('utf-8')))

    # Display the DataFrame    
    result="" 
    for index, row in df.iterrows():
        result=result + "["+ (",".join([str(item) for item in row]))+"]"
    
    # Close the connection
    csvFile.close()
    connection.close()

    return f"CSV file read successfully.{result}"
