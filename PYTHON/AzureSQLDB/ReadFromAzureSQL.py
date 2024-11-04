# This sample allows you to read data from Azure SQL database 
# Complete these steps before testing this funtion 
#   1. Select Manage connections to connect to Azure SQL database 


@app.connection("sqlDB")
@app.function("readfromazuresqldatabase")
def readfromazuresqldatabase(connection_string)->str:
    # Replace with the query you want to run
      query = "SELECT * FROM (VALUES ('John Smith', 31), ('Kayla Jones', 33)) AS Employee(EmpName, DepID);"
  
      # Establish a connection to the SQL database
      connection = sqlDB.connect(connection_string)
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
      resultsJSON = json.dumps({"values": results})          
      return results
  

