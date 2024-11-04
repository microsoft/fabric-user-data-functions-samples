
# This sample writes a CSV file from a lakehouse 


import pandas as pd 
@app.fabric_item_input(argName="mylakehouse", alias="<My Lakehouse alias>")
@app.function("write_csv_file_from_lakehouse")
def write_csv_file_in_lakehouse(mylakehouse: fabric.functions.FabricSqlConnection)-> str:
             
    return resultsJSON
