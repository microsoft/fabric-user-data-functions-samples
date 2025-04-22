"""
    Aggregates the input trip data to compute the average fare per country.

    Parameters:
    - df (pd.DataFrame): DataFrame containing trip data with columns:
        ['trip_id', 'customer_id', 'trip_fare', 'tip', 'country', 'date']

    Returns:
    - pd.DataFrame: DataFrame with columns ['country', 'average_fare']

    # Sample input data to test 
    data = {
    "trip_id": [101, 102, 103, 104, 105],
    "customer_id": [1, 2, 1, 3, 2],
    "trip_fare": [20.0, 25.0, 15.0, 30.0, 22.0],
    "tip": [2.0, 3.0, 1.0, 5.0, 2.5],
    "country": ["USA", "Canada", "USA", "UK", "Canada"],
    "date": pd.to_datetime(["2024-01-01", "2024-01-03", "2024-01-05", "2024-01-07", "2024-01-08"])
    }

"""

import pandas as pd
   
@udf.function()
def average_fare_by_country(df: pd.DataFrame) -> pd.DataFrame:
    # Group by country and calculate average fare
    result_df = df.groupby("country", as_index=False)["trip_fare"].mean()
    
    # Rename column for clarity
    result_df.rename(columns={"trip_fare": "average_fare"}, inplace=True)
    
    return result_df
