# This samples converts the input 2D list to a numpy array. The output is normalized to the range [0, 1] and we calculate the mean of each column.
# Complete these steps before testing this function
# 1. Select library management and add numpy library
# 2. Pass input as a list of lists, an example to use for this sample:
# [1, 2, 3, 4, 5]

import numpy as np
import json 

@udf.function()
def transform_data(data: list)-> dict:

    # Convert the 2D list to a numpy array
    np_data = np.array(data)

    # Normalize the data (scale values to range [0, 1])
    min_vals = np.min(np_data, axis=0)
    max_vals = np.max(np_data, axis=0)
    normalized_data = (np_data - min_vals) / (max_vals - min_vals)
    # Calculate the mean of each column
    column_means = np.mean(np_data, axis=0)
    norm = np.array(normalized_data)

    return { "NormalizedData": norm.tolist(), "Mean": float(column_means) }