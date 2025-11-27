import pandas as pd
from utils import is_static_column, fuzzy_match_activity


def process_single_file(file, mapping_df, sheet_name, header_row, static_columns):
    """
    Process a single Excel file through the full pipeline:
    - Read and unpivot
    - Clean data
    - Match activities with fuzzy matching
    - Merge with mapping table
    
    Returns: DataFrame ready for transformation, or None if no valid data
    """
    
    # Read the Excel file
    df = pd.read_excel(
        file,
        sheet_name=sheet_name,
        skiprows=header_row-1,
        dtype=str
    )
    
    # Identify activity columns using fuzzy matching
    activity_cols = [col for col in df.columns if not is_static_column(col, static_columns)]
    
    # Get the static columns that actually exist in this file
    existing_static_cols = [col for col in df.columns if is_static_column(col, static_columns)]
    
    # Unpivot the activity columns
    melted_df = pd.melt(
        df,
        id_vars=existing_static_cols,
        value_vars=activity_cols,
        var_name='RawItemName',
        value_name='Count'
    )
    
    # Clean data - remove null and zero counts
    melted_df = melted_df[melted_df['Count'].notna()]
    melted_df = melted_df[melted_df['Count'] != '0']
    melted_df = melted_df[melted_df['Count'] != 0]
    
    if melted_df.empty:
        return None
    
    # Fuzzy match activities to handle typos and variations
    melted_df['MatchedActivity'] = melted_df['RawItemName'].apply(
        lambda x: fuzzy_match_activity(x, mapping_df) or x
    )
    
    # Merge with mapping table
    melted_df = melted_df.merge(
        mapping_df,
        left_on='MatchedActivity',
        right_on='RawItemName',
        how='left'
    )
    
    # Keep only successfully mapped rows
    mapped_df = melted_df[melted_df['Sector'].notna()].copy()
    
    if mapped_df.empty:
        return None
    
    return mapped_df