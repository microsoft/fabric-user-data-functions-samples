# This sample allows you to read data from a Fabric SQL Database 
# Complete these steps before testing this function 
#   1. Select Manage connections to connect to a Fabric SQL Database
#   2. Copy the Alias name and replace it below inside the @udf.connection() decorator.

@udf.connection(argName="sqlDB",alias="<alias for sql database>")
@udf.function()
def read_from_sql_db(sqlDB: fn.FabricSqlConnection)-> str:
    # Replace with the query you want to run
    query = "SELECT * FROM (VALUES ('John Smith', 31), ('Kayla Jones', 33)) AS Employee(EmpName, DepID);"

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()

    # Execute the query
    cursor.execute(query)

    # Fetch all results
    results = []
    for row in cursor.fetchall():
        results.append(row)

    # Close the connection
    cursor.close()
    connection.close()
        
    return results


