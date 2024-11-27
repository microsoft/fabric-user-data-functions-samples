# This sample allows you to write data into a Fabric SQL Database 
# Complete these steps before testing this function: 
#   1. Select 'Manage connections' to connect to a Fabric SQL Database 
#   2. Copy the Alias name and replace it inside the @udf.connection() decorator. 

@udf.connection(argName="sqlDB",alias="<alias for sql database>")
@udf.function()
def write_to_sql_db(sqlDB: fn.FabricSqlConnection):
    # Replace with the data you want to insert
    data = [(1,"John Smith", 31), (2,"Kayla Jones", 33),(3,"Edward Harris", 33)]

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
  
    # Create the table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Employee (
        EmpID INT PRIMARY KEY,
        EmpName NVARCHAR(50),
        DepID INT
    );
    """
    cursor.execute(create_table_query)

    # Insert data into the table
    insert_query = "INSERT INTO Employee (EmpName, DepID) VALUES (?, ?);"
    cursor.executemany(insert_query, data)

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()               
    return "Employee table was created and data was added to this table"
