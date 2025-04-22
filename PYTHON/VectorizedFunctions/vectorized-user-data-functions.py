 """
    Filters the dataframe based on quality conditions.

    Parameters:
    df (pd.DataFrame): The input dataframe to be filtered.
    qualityConditions (dict): A dictionary where keys are column names and values are conditions to be met.

    Returns:
    pd.DataFrame: The filtered dataframe.
    
    Sample data 
    df = {
       "name": ["Alice", "Bob", "Charlie", "Diana"],
       "age": [25, 32, 37, 29],
       "score": [85.5, 90.0, 67.0, 88.5]
     }

    # Example quality conditions: age > 30 and score >= 70
    qualityConditions = {
       "age": "> 30",
       "score": ">= 70"
    }
    """

import pandas as pd 

@udf.function()
def filter_dataframe(df: pd.DataFrame, qualityConditions:dict) -> pd.DataFrame:  
    return filter_dataframe(df, qualityConditions)

# Helper function to filter dataframe 
def filter_dataframe(df: pd.DataFrame, qualityConditions:dict) -> pd.DataFrame:
    """
    Filters the dataframe based on quality conditions.

    Parameters:
    df (pd.DataFrame): The input dataframe to be filtered.
    qualityConditions (dict): A dictionary where keys are column names and values are conditions to be met.

    Returns:
    pd.DataFrame: The filtered dataframe.
    """
    # Apply each condition to the dataframe
    for column, condition in qualityConditions.items():
        row_count = df.shape[0]
        logging.info(f"Filtering dataframe based on condition: {column} {condition}")
        df = df.query(f"{column} {condition}")
        logging.info(f"Number of rows after filtered: {row_count - df.shape[0]}")
    
    return df
