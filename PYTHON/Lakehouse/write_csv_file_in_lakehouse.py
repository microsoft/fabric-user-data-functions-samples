
# This sample writes a CSV file from a lakehouse 


import pandas as pd 

@app.fabric_item_input(argName="mylakehouse", alias="<My Lakehouse alias>")
@app.function("write_csv_file_from_lakehouse")
def write_csv_file_in_lakehouse(mylakehouse: fabric.functions.FabricSqlConnection)-> str:
    data = [(1,"John Smith", 31), (2,"Kayla Jones", 33)]
    connection = mylakehouse.connectToFiles()
    csvFileName = "Employees" + str(round(datetime.datetime.now().timestamp())) + ".csv"
    csvFile = connection.get_file_client(csvFileName)
    # Convert the data to a DataFrame
    df = pd.DataFrame(data, columns=['ID','EmpName', 'DepID'])
    # Write the DataFrame to a CSV file
    df.to_csv(csvFile, index=False)
    # csvFile.upload_data('\n'.join(csvRows), overwrite=True)
  
    csvFile.close()
    connection.close()
    return resultsJSON
