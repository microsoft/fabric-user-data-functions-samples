
# This sample writes a CSV file from a lakehouse  using pandas 
# Complete these steps before testing this funtion 
#   1. Select Manage connections to connect to Lakehouse. 
#   2. Select Library Management and add pandas library 

import pandas as pd 

#Replace the alias "<My Lakehouse alias>" with your connection alias.
@app.fabric_item_input(argName="mylakehouse", alias="<My Lakehouse alias>")
@app.function("read_csv_file_from_lakehouse")
def read_csv_file_from_lakehouse(mylakehouse: fabric.functions.FabricSqlConnection)-> str:
             
    return f"File {csvFileName} was read from the Lakehouse."
