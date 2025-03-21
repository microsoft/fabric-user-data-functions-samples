
# This sample reads a CSV file from a lakehouse using pandas. Function takes file name as an input parameter
# Complete these steps before testing this funtion 
#   1. Select 'Manage connections' and add a connection to a Lakehouse
#   2. Select 'Library management' and add pandas library 

import pandas as pd 

# Replace the alias "<My Lakehouse alias>" with your connection alias.
@udf.connection(argName="myLakehouse", alias="<My Lakehouse alias>")
@udf.function()
def read_csv_from_lakehouse(myLakehouse: fn.FabricLakehouseClient, csvFileName: str) -> str:

    # Connect to the Lakehouse
    connection = myLakehouse.connectToFiles()   

    # Download the CSV file from the Lakehouse
    csvFile = connection.get_file_client(csvFileName)
    downloadFile=csvFile.download_file()
    csvData = downloadFile.readall()
    
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
