
# This sample writes a CSV file from a lakehouse using pandas 
# Complete these steps before testing this funtion 
#   1. Select 'Manage connections' and add a connection to a Lakehouse 
#   2. Select 'Library management' and add pandas library 
#   3. Sample input for employees: 
#   [[1,"John Smith", 31], [2,"Kayla Jones", 33]]

import pandas as pd 
import datetime

#Replace the alias "<My Lakehouse alias>" with your connection alias.
@udf.connection(argName="myLakehouse", alias="<My Lakehouse alias>")
@udf.function()
def write_csv_file_in_lakehouse(myLakehouse: fn.FabricLakehouseClient, employees: list)-> str:
    '''
    Description: Write employee data to lakehouse as timestamped CSV file using pandas.
    
    Args:
        myLakehouse (fn.FabricLakehouseClient): Fabric lakehouse connection.
        employees (list): List of employee records as [ID, Name, DeptID] arrays.
    
    Returns:
        str: Confirmation message with filename and viewing instructions.
        
    Example:
        employees = [[1,"John Smith", 31], [2,"Kayla Jones", 33]]
        Creates "Employees1672531200.csv" in lakehouse
    '''
    
    csvFileName = "Employees" + str(round(datetime.datetime.now().timestamp())) + ".csv"
       
    # Convert the data to a DataFrame
    df = pd.DataFrame(employees, columns=['ID','EmpName', 'DepID'])
    # Write the DataFrame to a CSV file
    csv_string = df.to_csv(index=False)
       
    # Upload the CSV file to the Lakehouse
    connection = myLakehouse.connectToFiles()
    csvFile = connection.get_file_client(csvFileName)  
    
    csvFile.upload_data(csv_string, overwrite=True)

    csvFile.close()
    connection.close()
    return f"File {csvFileName} was written to the Lakehouse. Open the Lakehouse in https://app.fabric.microsoft.com to view the files"
