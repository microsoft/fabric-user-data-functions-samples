
# This sample reads a CSV file from a lakehouse  using pandas. Function takes file name as an input parameter
# Complete these steps before testing this funtion 
#   1. Select Manage connections to connect to Lakehouse. 
#   2. Select Library Management and add pandas library 

import pandas as pd 

#Replace the alias "<My Lakehouse alias>" with your connection alias.
@app.fabric_item_input(argName="mylakehouse", alias="<My Lakehouse alias>")
@app.function("read_csv_from_lakehouse")
def read_csv_from_lakehouse(myLakehouse: udf.FabricLakehouseClient, csvFileName: str) -> str:

    # Connect to the Lakehouse
    connection = myLakehouse.connectToFiles()   

    # Download the CSV file from the Lakehouse
    csvFile = connection.get_file_client(csvFileName)
    downloadFile=csvFile.download_file()
    csvData = downloadFile.readall()
    
    # Read the CSV data into a pandas DataFrame
    from io import StringIO
    df = pd.read_csv(StringIO(csvData.decode('utf-8')))

    # Display the DataFrame in debug console
    print(df)

    # Close the connection
    csvFile.close()
    connection.close()

    return "CSV file read successfully."
