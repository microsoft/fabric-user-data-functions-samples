import pandas as pd 

@udf.function()
def summarize_age_distribution(ageSeries: pd.Series) -> str:
    """
    Summarizes the distribution of ages in a Pandas Series.

    Args:
        ageSeries (pd.Series): Series containing age values.
        Example: [23, 45, 31, 35, 29, 41, 38, 27]
    Returns:
        str: Summary string describing the distribution.
    """
    if ageSeries.empty:
        return "No age data provided."

    summary = (
        f"Age Summary:\n"
        f"- Count: {ageSeries.count()}\n"
        f"- Mean: {ageSeries.mean():.2f}\n"
        f"- Median: {ageSeries.median():.2f}\n"
        f"- Min: {ageSeries.min()}\n"
        f"- Max: {ageSeries.max()}\n"
        f"- Std Dev: {ageSeries.std():.2f}"
    )
    return summary

