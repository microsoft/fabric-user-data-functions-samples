"""
    Filters the input DataFrame to include only rows where 'country' matches the specified value.

    Parameters:
    - df (pd.DataFrame): Input DataFrame with person data.
    - country (str): Country to filter by.

    Returns:
    - pd.DataFrame: Filtered DataFrame.


# Sample data
data = {
    "first_name": ["Alice", "Bob", "Charlie", "Diana"],
    "last_name": ["Smith", "Jones", "Brown", "Taylor"],
    "age": [30, 25, 35, 28],
    "country": ["USA", "Canada", "USA", "UK"],
    "city": ["New York", "Toronto", "Los Angeles", "London"],
    "email": ["alice@example.com", "bob@example.ca", "charlie@example.com", "diana@example.co.uk"]
}
"""

import pandas as pd
@udf.function()
def filter_by_country(df: pd.DataFrame, country: str) -> pd.DataFrame:
    filtered_df = df[df["country"] == country].reset_index(drop=True)
    return filtered_df
