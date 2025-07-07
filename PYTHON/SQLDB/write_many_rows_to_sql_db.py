# Select 'Manage connections' and add a connection to a Fabric SQL Database 
# Replace the alias "<alias for sql database>" with your connection alias.
@udf.connection(argName="sqlDB",alias="<alias for sql database>")
@udf.function()
def write_many_to_sql_db(sqlDB: fn.FabricSqlConnection) -> str:
    '''
    Description: Insert multiple employee records into SQL database, creating table if needed.
    
    Args:
        sqlDB (fn.FabricSqlConnection): Fabric SQL database connection.
    
    Returns:
        str: Confirmation message about table creation and data insertion.
        
    Example:
        Inserts sample employee records: John Smith, Kayla Jones, Edward Harris
    '''
    
    # Replace with the data you want to insert
    data = [(1,"John Smith", 31), (2,"Kayla Jones", 33),(3,"Edward Harris", 33)]

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
  
    # Create the table if it doesn't exist
    create_table_query = '''
        IF OBJECT_ID(N'dbo.Employee', N'U') IS NULL
        CREATE TABLE dbo.Employee (
            EmpID INT PRIMARY KEY,
            EmpName nvarchar(50),
            DepID INT
            );
    '''
    cursor.execute(create_table_query)
 
    # Insert data into the table
    insert_query = "INSERT INTO Employee (EmpID, EmpName, DepID) VALUES (?, ?, ?);"
    cursor.executemany(insert_query, data)

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()               
    return "Employee table was created (if necessary) and data was added to this table"
