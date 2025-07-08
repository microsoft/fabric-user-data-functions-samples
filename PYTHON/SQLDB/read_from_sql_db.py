# Select 'Manage connections' and add a connection to a Fabric SQL Database 
# Replace the alias "<alias for sql database>" with your connection alias.
@udf.connection(argName="sqlDB",alias="<alias for sql database>")
@udf.function()
def read_from_sql_db(sqlDB: fn.FabricSqlConnection)-> list:
    '''
    Description: Read employee data from SQL database using sample query.
    
    Args:
        sqlDB (fn.FabricSqlConnection): Fabric SQL database connection.
    
    Returns:
        list: Employee records as tuples with name and department ID.
        
    Example:
        Returns [('John Smith', 31), ('Kayla Jones', 33)]
    '''
    
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


