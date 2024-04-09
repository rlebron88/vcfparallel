# vcfparallel.py

import pandas as pd
from multiprocessing import Pool, cpu_count

def _apply_func_to_row(args):
    """Unwrapper function for applying a function to a row."""
    func, row = args[:2]
    return func(row, *args[2:])

def parallel_apply(df, func, num_processes=None, *args, **kwargs):
    """Applies a function to each row of a DataFrame in parallel using multiprocessing.
    
    Args:
        df (pd.DataFrame): The DataFrame to process.
        func (callable): The function to apply to each row.
        num_processes (int, optional): Number of processes to use. Defaults to the number of CPU cores.
        *args, **kwargs: Additional arguments for the `func` function.
    
    Returns:
        pd.DataFrame: The resulting DataFrame.
    """
    if num_processes is None:
        num_processes = cpu_count()
    
    # Prepare data for multiprocessing
    rows = df.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
    pool_data = [(func, row, *args) for row in rows]
    
    # Use multiprocessing Pool to parallelize the operation
    with Pool(processes=num_processes) as pool:
        results = pool.map(_apply_func_to_row, pool_data)
    
    # Convert results back to DataFrame
    result_df = pd.DataFrame(results)
    
    # Sort the resulting DataFrame by CHROM and POS if present
    if 'CHROM' in result_df.columns and 'POS' in result_df.columns:
        result_df = result_df.sort_values(by=['CHROM', 'POS'])
    
    return result_df
