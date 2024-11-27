# This sample reads data from a table in a lakehouse 
# Complete these steps before testing this function
# 1. Select Manage connections and add a connection to a Lakehouse 


import json

@udf.connection(argName="mylakehouse", alias="<My Lakehouse alias>")
@udf.function()
def query_data_from_tables(mylakehouse: fn.FabricSqlConnection) -> str:

    connection = mylakehouse.connect()
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
    
    valJSON = json.dumps({"values": values})
    cursor.close()
    connection.close()

    return valJSON

