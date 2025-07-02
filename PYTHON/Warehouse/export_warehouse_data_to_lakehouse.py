import datetime  

@udf.connection(argName="myWarehouse", alias="<My Warehouse Alias>")
@udf.connection(argName="myLakehouse", alias="<My Lakehouse Alias>")
@udf.function()
def export_warehouse_data_to_lakehouse(myWarehouse: fn.FabricSqlConnection, myLakehouse: fn.FabricLakehouseClient) -> dict:
    '''
    Description: Export employee data from warehouse to lakehouse as timestamped CSV file.
    
    Args:
        myWarehouse (fn.FabricSqlConnection): Fabric warehouse connection.
        myLakehouse (fn.FabricLakehouseClient): Fabric lakehouse connection.
    
    Returns:
        dict: Contains confirmation message and employee data as JSON objects.
        
    Example:
        Creates "Employees1672531200.csv" with sample employee records.
    '''

    whSqlConnection = myWarehouse.connect()

    cursor = whSqlConnection.cursor()
    cursor.execute(f"SELECT * FROM (VALUES ('John Smith',  31) , ('Kayla Jones', 33)) AS Employee(EmpName, DepID);")

    rows = [x for x in cursor]
    columnNames = [x[0] for x in cursor.description]
    csvRows = []
    csvRows.append(','.join(columnNames))

    # Turn the rows into comma separated values, and then upload it to Employees<timestamp>.csv
    for row in rows:
        csvRows.append(','.join(map(str, row)))

    lhFileConnection = myLakehouse.connectToFiles()
    csvFileName = "Employees" + str(round(datetime.datetime.now().timestamp())) + ".csv"
    csvFile = lhFileConnection.get_file_client(csvFileName)
    csvFile.upload_data('\n'.join(csvRows), overwrite=True)

    # Turn the rows into a json object
    values = []

    for row in rows:
        item = {}
        for prop, val in zip(columnNames, row):
            if isinstance(val, (datetime.datetime, datetime.date)):
                val = val.isoformat()
            item[prop] = val
        values.append(item)

    cursor.close()
    whSqlConnection.close()
    csvFile.close()
    lhFileConnection.close()

    return {"message": "File {} is written to {} Lakehouse. You can delete it from the Lakehouse after trying this sample.".format(csvFileName, myLakehouse.alias_name),
                        "values": values}
