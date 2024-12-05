
# This sample reads data from a table in a lakehouse 
# Complete these steps before testing this function
# 1. Select Manage connections and add a connection to a Lakehouse 

import json
import datetime

# Replace the alias "<My Lakehouse alias>" with your connection alias.
@udf.connection(argName="myLakehouse", alias="<My Lakehouse alias>")
@udf.function()
def query_data_from_tables(myLakehouse: fn.FabricLakehouseClient) -> list:
    # Connect to the Lakehouse SQL Endpoint
    connection = myLakehouse.connectToSql()
    
    # Use connection to execute a query
    cursor = connection.cursor()
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

    # Close the connection
    cursor.close()
    connection.close()

    return values

