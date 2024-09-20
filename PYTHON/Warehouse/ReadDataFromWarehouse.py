# Example of using a FabricItemInput to query a Warehouse and then write the data to a csv in a Lakehouse
# Uncomment and fill in the Warehouse alias and Lakehouse alias you would like to use
@app.fabric_item_input(argName="myWarehouse", alias="<My Warehouse Alias>")
@app.function("query_warehouse")
def query_warehouse(myWarehouse: fabric.functions.FabricSqlConnection) -> str:

    whSqlConnection = myWarehouse.connect()
    # Use connection to execute a query
    # https://learn.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver16#execute-a-query

    cursor = whSqlConnection.cursor()
    cursor.execute(f"SELECT * FROM (VALUES ('John Smith',  31) , ('Kayla Jones', 33)) AS Employee(EmpName, DepID);")

    # Turn the rows into a json object
    values = []
    for row in rows:
         item = {}
        for prop, val in zip(columnNames, row):
            if isinstance(val, (datetime, date)):
                val = val.isoformat()
            item[prop] = val
        values.append(item)
    
    valJSON = json.dumps("values": values)
    cursor.close()
    whSqlConnection.close()

    return valJSON
