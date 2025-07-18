import pandas as pd 
# Select 'Manage connections' and add a connection to a Lakehouse which has a CSV file
# Replace the alias "<My Lakehouse alias>" with your connection alias.
@udf.connection(argName="myLakehouse", alias="<My Lakehouse alias>")
@udf.function()
def read_csv_from_lakehouse(myLakehouse: fn.FabricLakehouseClient, csvFileName: str) -> str:
    '''
    Description: Read CSV file from lakehouse and return data as formatted string.
    
    Args:
        myLakehouse (fn.FabricLakehouseClient): Fabric lakehouse connection.
        csvFileName (str): CSV file name in the lakehouse Files folder.
    
    Returns: 
        str: Confirmation message with formatted CSV data rows.
    '''
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
