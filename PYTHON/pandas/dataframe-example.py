import pandas as pd
import logging

def filter_customers_by_country_df(df: pd.DataFrame, countryname: str) -> list:
    """
    Filters customer and order information by country name from a Pandas DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing customer and order information
          Example:  [     {"CustomerID": 1, "Name": "Alice", "Country": "USA", "OrderID": 101},     {"CustomerID": 2, "Name": "Bob", "Country": "Canada", "OrderID": 102},     {"CustomerID": 3, "Name": "Charlie",           "Country": "USA", "OrderID": 103},     {"CustomerID": 4, "Name": "Diana", "Country": "Mexico", "OrderID": 104} ]
        countryname (str): Name of the country to filter by 
          Example: USA
        
    Returns:
        list: Filtered list of customers from the specified country
    """
    logging.info(f'Filtering customers by country: {countryname}')
    
    try:
        # Check if the DataFrame is empty
        if df.empty:
            logging.warning('No data provided')
            return []
        
        # Check if 'Country' column exists (case-insensitive)
        country_column = None
        for col in df.columns:
            if col.lower() in ['country', 'country_name', 'countryname']:
                country_column = col
                break
        
        if country_column is None:
            logging.error('Country column not found in data')
            return []
        
        # Filter by country (case-insensitive)
        filtered_df = df[df[country_column].str.lower() == countryname.lower()]
        
        # Convert back to list of dictionaries
        result = filtered_df.to_dict('records')
        
        logging.info(f'Found {len(result)} customers from {countryname}')
        return result
        
    except Exception as e:
        logging.error(f'Error filtering customers by country: {str(e)}')
        return []
