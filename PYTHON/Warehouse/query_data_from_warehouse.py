import datetime
#Select 'Manage connections' to connect to a Warehouse
#Replace the alias "<My Warehouse Alias>" with your connection alias.
@udf.connection(argName="myWarehouse", alias="<My Warehouse Alias>")
@udf.function()
def query_data_from_warehouse(myWarehouse: fn.FabricSqlConnection) -> list:
    '''
    Description: Query employee data from a Fabric warehouse and return as JSON objects.
    
    Args:
        myWarehouse (fn.FabricSqlConnection): Fabric warehouse connection.
    
    Returns:
        list: Employee records as dictionaries with EmpName and DepID fields.
        
    Example:
        Returns [{'EmpName': 'John Smith', 'DepID': 31}, {'EmpName': 'Kayla Jones', 'DepID': 33}]
    '''
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
