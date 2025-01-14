# Write one row of data into a table in SQL database 
# This sample allows you to write one row of data into a Fabric SQL Database 
# Complete these steps before testing this function: 
#   1. Select 'Manage connections' to connect to a Fabric SQL Database 
#   2. Copy the Alias name and replace it inside the @udf.connection() decorator. 

@udf.connection(argName="sqlDB",alias="sqldb")
@udf.function()
def write_one_to_sql_db(sqlDB: fn.FabricSqlConnection, employeeId: int, employeeName: str, deptId: int) -> str:
    # Replace with the data you want to insert
    data = [employeeId, employeeName, deptId]

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
  
    # Create the table if it doesn't exist
    create_table_query = """
        IF OBJECT_ID(N'dbo.Employee', N'U') IS NULL
        CREATE TABLE dbo.Employee (
            EmpID INT PRIMARY KEY,
            EmpName nvarchar(50),
            DepID INT
            );
    """
    cursor.execute(create_table_query)
 
    # Insert data into the table
    insert_query = "INSERT INTO Employee (EmpID, EmpName, DepID) VALUES (?, ?, ?);"
    cursor.execute(insert_query, data)

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()               
    return "Employee table was created (if necessary) and data was added to this table"