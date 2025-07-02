# This sample uses pandas to manipulate data for a given age group 
# Complete these steps before testing this function
# 1. Select library management and add pandas library
# Pass input as a list of objects, an example to use for this sample
# [
#  {
#   "Name": "John",
#   "Age": 22,
#   "Gender": "male"
#  }
# ]

import pandas as pd 

@udf.function()
def manipulate_data(data: list) -> list:
    '''
    Description: Manipulate data using pandas to group by age categories and calculate mean ages.

    Args:
    - data (list): List of dictionaries containing Name, Age, and Gender fields

    Returns: list: JSON records with AgeGroup (Adult/Minor) and mean Age values
    '''
    # Convert the data dictionary to a DataFrame
    df = pd.DataFrame(data)
        # Perform basic data manipulation
    # Example: Add a new column 'AgeGroup' based on the 'Age' column    
    df['AgeGroup'] = df['Age'].apply(lambda x: 'Adult' if x >= 18 else 'Minor')
    
    # Example: Filter rows where 'Age' is greater than 30
    # df_filtered = df[df["Age"] > 30]

    # Example: Group by 'AgeGroup' and calculate the mean age
    df_grouped = df.groupby("AgeGroup")["Age"].mean().reset_index()

    return df_grouped.to_json(orient='records')
