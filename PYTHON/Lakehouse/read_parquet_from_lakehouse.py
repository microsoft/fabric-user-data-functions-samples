# This sample reads a parquet file from a lakehouse
# Complete these steps before testing this function
# 1. Select 'Manage connections' and add a connection to a Lakehouse which has a parquet file
# 2. Select 'Library management' and add the pyarrow, pandas libraries
# 3. Replace the alias "<My Lakehouse alias>" with your connection alias.

import pandas as pd
from io import BytesIO 
import pyarrow.parquet as pq # This is engine needed to read parquet files

# Replace the alias "<My Lakehouse alias>" with your connection alias.
@udf.connection(argName="myLakehouse", alias="<Lakehouse-alias>")
@udf.function()
def read_parquet_from_lakehouse(myLakehouse: fn.FabricLakehouseClient, parquetFileName: str) -> str:
    '''
    Description: Read parquet file from lakehouse and return data as formatted string.
    
    Args:
        myLakehouse (fn.FabricLakehouseClient): Fabric lakehouse connection.
        parquetFileName (str): Parquet file name or path relative to Files folder.
    
    Returns:
        str: Confirmation message with formatted parquet data rows.
        
    Example:
        parquetFileName = "data.parquet" or "Folder1/data.parquet"
        Returns formatted rows from the parquet file
    '''
        
    # Connect to the Lakehouse
    connection = myLakehouse.connectToFiles()   

    # Download the Parquet file from the Lakehouse
    # If relative path is "Files/myfile.parquet , then parquetFileName = "myfile.parquet"
    # If relative path is "Files/Folder1/myfile.parquet , then parquetFileName = "Folder1/myfile.parquet"
    parquetFile = connection.get_file_client(parquetFileName)
    downloadFile = parquetFile.download_file()
    parquetData = downloadFile.readall()
    
    # Read the Parquet data into a pandas DataFrame
    df = pd.read_parquet(BytesIO(parquetData))

    # Display the DataFrame    
    rows = [] 
    for index, row in df.iterrows():
        rows.append("[" + (",".join([str(item) for item in row])) + "]")
    result = "".join(rows)
    
    # Close the connection
    parquetFile.close()
    connection.close()

    return f"Parquet file read successfully.{result}"
