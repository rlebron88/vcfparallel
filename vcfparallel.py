# vcfparallel.py

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

def _apply_func_to_row(row, func, *args, **kwargs):
    """Applies a function to a DataFrame row."""
    return func(row, *args, **kwargs)

def parallel_apply(df, func, num_threads=4, *args, **kwargs):
    """Applies a function to each row of a DataFrame in parallel.
    
    Args:
        df (pd.DataFrame): The DataFrame to process.
        func (callable): The function to apply to each row.
        num_threads (int, optional): Number of threads to use.
        *args, **kwargs: Additional arguments for the `func` function.
    
    Returns:
        pd.DataFrame: The resulting DataFrame.
    """
    # Split the DataFrame into chunks for parallel processing
    rows = [row for _, row in df.iterrows()]
    results = []
    
    # Use ThreadPoolExecutor to parallelize the function application
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_row = {executor.submit(_apply_func_to_row, row, func, *args, **kwargs): row for row in rows}
        for future in as_completed(future_to_row):
            results.append(future.result())
    
    # Create a new DataFrame from the results
    result_df = pd.DataFrame(results)
    
    # Sort the resulting DataFrame by CHROM and POS if present
    if 'CHROM' in result_df.columns and 'POS' in result_df.columns:
        result_df = result_df.sort_values(by=['CHROM', 'POS'])
    
    return result_df
