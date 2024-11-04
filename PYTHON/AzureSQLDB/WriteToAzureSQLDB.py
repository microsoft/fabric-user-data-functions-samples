# This sample allows you to read data from Azure SQL database 
# Complete these steps before testing this funtion 
#   1. Select Manage connections to connect to Azure SQL database 
#   2. Copy the Alias name and replace it in line 6 

@app.connection("sqlDB",alias="<alias for azure sql database>")
@app.function("write_to_azure_sql_db")
def write_to_azure_sql_db(sqlDB: udf.FabricSqlConnection):
    # Replace with the data you want to insert
    data = [(1,"John Smith", 31), (2,"Kayla Jones", 33),(3,"Edward Harris", 33)]

    # Establish a connection to the SQL database
    connection = sqlDB.connect(connection_string)
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
    conn.commit()

    # Close the connection
    conn.close()
    # Close the connection
    cursor.close()
    connection.close()               
    return "Employee table was created and data was added to this table".
