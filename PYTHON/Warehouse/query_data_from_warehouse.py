# Example of using a connection to query a Warehouse
# Complete these steps before testing this funtion 
#   1. Select 'Manage connections' to connect to a Warehouse
#   2. Copy the Alias name and replace below

import datetime

@udf.connection(argName="myWarehouse", alias="<My Warehouse Alias>")
@udf.function()
def query_data_from_warehouse(myWarehouse: fn.FabricSqlConnection) -> list:

    whSqlConnection = myWarehouse.connect()
    # Use connection to execute a query
    cursor = whSqlConnection.cursor()
    cursor.execute(f"SELECT * FROM (VALUES ('John Smith',  31) , ('Kayla Jones', 33)) AS Employee(EmpName, DepID);")
    
    rows = [x for x in cursor]
    columnNames = [x[0] for x in cursor.description]
    # Turn the rows into a json object
    values = []
    for row in rows:
        item = {}
        for prop, val in zip(columnNames, row):
            if isinstance(val, (datetime.date, datetime.datetime)):
                val = val.isoformat()
            item[prop] = val
        values.append(item)
    
    cursor.close()
    whSqlConnection.close()

    return values
