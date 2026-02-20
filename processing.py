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
    
    try:
        df = pd.read_excel(
            file,
            sheet_name=sheet_name,
            skiprows=header_row-1,
            dtype=str
        )

        # STANDARDIZE COLUMN NAMES - remove extra spaces, strip whitespace
        df.columns = df.columns.str.strip().str.replace(r'\s+', ' ', regex=True)

        # Clean numeric columns - remove commas and convert
        numeric_cols = ['COST', 'Quantity', 'People_Per_Beneficiary']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '').replace('', None)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
    except ValueError as e:
        # Sheet name not found
        if "Worksheet named" in str(e):
            raise ValueError(
                f"❌ Sheet '{sheet_name}' not found in {file.name}. "
                f"Please check the sheet name or ask the uploader to use the correct template."
            )
        else:
            raise e
    except Exception as e:
        raise Exception(f"❌ Error reading {file.name}: {str(e)}. File may be corrupted or in wrong format.")
    
    # BEFORE unpivoting, preserve original row number
    df['_original_row'] = df.index + header_row + 1
    
    # Identify activity columns using fuzzy matching (exclude our temp column)
    activity_cols = [col for col in df.columns if not is_static_column(col, static_columns) and col != '_original_row']
    
    # Get the static columns that actually exist in this file
    existing_static_cols = [col for col in df.columns if is_static_column(col, static_columns)]
    
    # Add _original_row to static cols so it survives the unpivot
    existing_static_cols.append('_original_row')
    
    # Unpivot the activity columns
    melted_df = pd.melt(
        df,
        id_vars=existing_static_cols,
        value_vars=activity_cols,
        var_name='RawItemName',
        value_name='Count'
    )

    # Add source tracking using preserved row numbers
    melted_df['Source_Filename'] = file.name if hasattr(file, 'name') else 'Unknown'
    melted_df['Source_Row_Number'] = melted_df['_original_row']
    
    # Drop temp column
    melted_df = melted_df.drop(columns=['_original_row'])
    
    # Clean - convert to numeric (blanks become NaN)
    melted_df['Count'] = pd.to_numeric(melted_df['Count'], errors='coerce')

    # Remove NaN and zeros
    melted_df = melted_df[melted_df['Count'].notna()]
    melted_df = melted_df[melted_df['Count'] > 0]
    
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
    
    # Keep ALL rows, including unmapped ones
    if melted_df.empty:
        return None

    return melted_df