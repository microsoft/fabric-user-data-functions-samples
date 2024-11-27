# This samples converts the input 2D list to a numpy array. The output is normalized to the range [0, 1] and we calculate the mean of each column.
# Complete these steps before testing this function
# 1. Select library management and add numpy library

import numpy as np

@udf.function()
def transform_data(data: dict )-> str:
    # Extract the items from the input data
    items = data['data']['items']
    # Convert the 2D list to a numpy array
    np_data = np.array(items)

    # Normalize the data (scale values to range [0, 1])
    min_vals = np.min(np_data, axis=0)
    max_vals = np.max(np_data, axis=0)
    normalized_data = (np_data - min_vals) / (max_vals - min_vals)

    # Calculate the mean of each column
    column_means = np.mean(np_data, axis=0)

    return f"Normalized Data: {normalized_data} and Column Means: {column_means}"
