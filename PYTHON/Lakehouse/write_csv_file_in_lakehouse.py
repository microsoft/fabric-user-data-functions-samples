
# This sample writes a CSV file from a lakehouse  using pandas 
# Complete these steps before testing this funtion 
#   1. Select Manage connections to connect to Lakehouse. 
#   2. Select Library Management and add pandas library 

import pandas as pd 
import datetime

#Replace the alias "<My Lakehouse alias>" with your connection alias.
@udf.connection(argName="mylakehouse", alias="<My Lakehouse alias>")
@udf.function()
def write_csv_file_in_lakehouse(mylakehouse: fn.FabricLakehouseClient)-> str:
    data = [(1,"John Smith", 31), (2,"Kayla Jones", 33)]
    csvFileName = "Employees" + str(round(datetime.datetime.now().timestamp())) + ".csv"
       
    # Convert the data to a DataFrame
    df = pd.DataFrame(data, columns=['ID','EmpName', 'DepID'])
    # Write the DataFrame to a CSV file
    csv_string = df.to_csv(index=False)
       
    # Upload the CSV file to the Lakehouse
    connection = mylakehouse.connectToFiles()
    csvFile = connection.get_file_client(csvFileName)  
    
    csvFile.upload_data(csv_string, overwrite=True)

    csvFile.close()
    connection.close()
    return f"File {csvFileName} was written to the Lakehouse. Open the Lakehouse in https://app.fabric.microsoft.com to view the files"
