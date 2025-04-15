import pandas as pd 


@udf.function()
def filter_dataframe(df: pd.DataFrame, qualityConditions:dict) -> pd.DataFrame:
    """
    Filters the dataframe based on quality conditions.

    Parameters:
    df (pd.DataFrame): The input dataframe to be filtered.
    qualityConditions (dict): A dictionary where keys are column names and values are conditions to be met.

    Returns:
    pd.DataFrame: The filtered dataframe.
    """
    return filter_dataframe(df, qualityConditions)

@udf.function()
def filter_dict(dfDict: dict, qualityConditions:dict) -> dict:
    """
    Filters the dict based on quality conditions.

    Parameters:
    df (pd.DataFrame): The input dataframe to be filtered.
    qualityConditions (dict): A dictionary where keys are column names and values are conditions to be met.

    Returns:
    pd.DataFrame: The filtered dataframe.
    """
    df = pd.DataFrame.from_dict(dfDict)
    logging.info(f"Shape of data: {df.shape}")
    return filter_dataframe(df, qualityConditions).to_dict()

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
