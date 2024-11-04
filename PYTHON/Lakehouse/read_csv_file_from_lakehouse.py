# This sample reads a CSV file from a lakehouse 


import pandas as pd 
@app.fabric_item_input(argName="mylakehouse", alias="<My Lakehouse alias>")
@app.function("read_csv_file_from_lakehouse")
def read_csv_file_from_lakehouse(mylakehouse: fabric.functions.FabricSqlConnection)-> str:
             
    return resultsJSON
