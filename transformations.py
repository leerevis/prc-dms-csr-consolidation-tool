import pandas as pd
import os
from thefuzz import process

# Load PCode reference data
script_dir = os.path.dirname(__file__)
pcode_path = os.path.join(script_dir, 'data', 'phl_adminareas_fixed.csv')
pcode_df = pd.read_csv(pcode_path)

from thefuzz import process

def add_pcodes(output_df):
    """
    Add Philippine administrative codes (PCodes) by matching location names.
    Uses fuzzy matching for Province and Municipality.
    """
    
    # Only process rows with Province data
    has_province = output_df['Province'].notna() & (output_df['Province'] != '')
    
    if not has_province.any():
        return output_df
    
    # Get unique provinces and municipalities from PCode file
    province_list = pcode_df['adm2_clean'].dropna().unique().tolist()
    
    # Fuzzy match Province
    def match_province(prov):
        if pd.isna(prov) or prov == '':
            return None, None, None
        
        cleaned = str(prov).strip().lower()
        match = process.extractOne(cleaned, province_list, score_cutoff=85)
        
        if match:
            matched_name = match[0]
            # Get the PCode and Region for this province
            pcode_row = pcode_df[pcode_df['adm2_clean'] == matched_name].iloc[0]
            return pcode_row['ADM2_new'], pcode_row['ADM1_EN'], matched_name
        return None, None, None
    
    # Apply province matching
    matched = output_df[has_province]['Province'].apply(match_province)
    output_df.loc[has_province, 'Prov_CODE'] = matched.apply(lambda x: x[0])
    output_df.loc[has_province, 'Region'] = matched.apply(lambda x: x[1])
    
    # Fuzzy match Municipality (filter by province for accuracy)
    def match_municipality(row):
        if pd.isna(row['Municipality/City']) or row['Municipality/City'] == '':
            return None
        if pd.isna(row['Prov_CODE']):
            return None
        
        # Get municipalities for this province only
        mun_list = pcode_df[pcode_df['ADM2_new'] == row['Prov_CODE']]['adm3_clean'].dropna().unique().tolist()
        
        cleaned = str(row['Municipality/City']).strip().lower()
        match = process.extractOne(cleaned, mun_list, score_cutoff=85)
        
        if match:
            matched_name = match[0]
            pcode_row = pcode_df[(pcode_df['ADM2_new'] == row['Prov_CODE']) & 
                                (pcode_df['adm3_clean'] == matched_name)].iloc[0]
            return pcode_row['ADM3_new']
        return None
    
    output_df.loc[has_province, 'Mun_Code'] = output_df[has_province].apply(match_municipality, axis=1)
    
    return output_df

def calculate_beneficiary_units(row):
    """
    Calculate number of beneficiary units (e.g., families, households)
    Formula: Count / Quantity
    """
    count = pd.to_numeric(row.get('Count', 0), errors='coerce') or 0
    quantity = pd.to_numeric(row.get('Quantity'), errors='coerce')
    unit = str(row.get('Unit', '')).strip().upper()
    
    # Flag cash for manual review
    if unit in ['PESOS', 'PHP', 'CASH', 'PESO']:
        return None
    
    # If Quantity is missing/NA/0, can't calculate
    if pd.isna(quantity) or quantity == 0:
        return None
    
    # Calculate beneficiary units
    return count / quantity


def calculate_individuals(row):
    """
    Calculate number of individuals served
    Formula: (Count / Quantity) × People_Per_Beneficiary
    """
    count = pd.to_numeric(row.get('Count', 0), errors='coerce') or 0
    quantity = pd.to_numeric(row.get('Quantity'), errors='coerce')
    people_per_beneficiary = pd.to_numeric(row.get('People_Per_Beneficiary'), errors='coerce')
    unit = str(row.get('Unit', '')).strip().upper()
    
    # Flag cash for manual review
    if unit in ['PESOS', 'PHP', 'CASH', 'PESO']:
        return None
    
    # If any required field is missing, can't calculate
    if pd.isna(quantity) or quantity == 0:
        return None
    if pd.isna(people_per_beneficiary) or people_per_beneficiary == 0:
        return None
    
    # Calculate individuals
    beneficiary_units = count / quantity
    return beneficiary_units * people_per_beneficiary


def transform_to_output_schema(df):
    """
    Transform the processed DataFrame to the final output schema.
    Takes merged data with mapping columns and creates standardized output.
    
    Returns: DataFrame with final output columns
    """
    
    output_df = df.copy()

    # Add validation flag for unmapped activities
    # Check for null/blank Sector OR placeholder values
    output_df['Validation Status'] = output_df.apply(
        lambda row: 'Taxonomy Error' if (
            pd.isna(row.get('Sector')) or 
            str(row.get('Sector', '')).strip() == '' or
            str(row.get('Activity', '')).strip().upper() == 'NEEDS MAPPING' or
            str(row.get('Sector', '')).strip().upper() == 'NEEDS MAPPING'
        ) else 'For Validation',
        axis=1
    )

    # For unmapped rows, use the same expanded mask
    unmapped_mask = (
        (output_df['Sector'].isna()) | 
        (output_df['Sector'] == '') | 
        (output_df['Sector'].str.strip() == '') |
        (output_df['Activity'].str.strip().str.upper() == 'NEEDS MAPPING') |
        (output_df['Sector'].str.strip().str.upper() == 'NEEDS MAPPING')
    )

    # Use RawItemName_x (the original activity name from the raw data)
    if 'RawItemName_x' in output_df.columns:
        output_df.loc[unmapped_mask, 'Materials/Service Provided'] = output_df.loc[unmapped_mask, 'RawItemName_x']
    elif 'RawItemName' in output_df.columns:
        output_df.loc[unmapped_mask, 'Materials/Service Provided'] = output_df.loc[unmapped_mask, 'RawItemName']

    # Static values
    output_df['Organisation'] = 'Philippine Red Cross'
    # Handle both single and double space variations
    output_df['Implementing Partner/Supported By'] = output_df.get('Relief  Donor', output_df.get('Relief Donor', None))
    output_df['Phase'] = None
    
    # Mapping table columns
    output_df['Sector/Cluster'] = output_df['Sector']
    output_df['Sub Sector'] = output_df.get('Sub - Sector', output_df.get('Sub Sector', None))
    output_df['Activity'] = output_df.get('Activity', None)

    # Only use mapping table value for MAPPED rows (don't overwrite unmapped)
    mapped_mask = ~unmapped_mask  # Inverse of unmapped
    output_df.loc[mapped_mask, 'Materials/Service Provided'] = output_df.loc[mapped_mask, 'Assistance? Materials/service']

    output_df['Unit'] = output_df.get('Unit', None)
    # Calculate beneficiaries using the new logic
    output_df['# of Beneficiaries Served'] = output_df.apply(calculate_beneficiary_units, axis=1)
    output_df['Number of Individuals'] = output_df.apply(calculate_individuals, axis=1)

    
    def determine_validation_status(row):
        has_mapping_error = (
            pd.isna(row.get('Sector')) or 
            str(row.get('Sector', '')).strip() == '' or
            str(row.get('Activity', '')).strip().upper() == 'NEEDS MAPPING' or
            str(row.get('Sector', '')).strip().upper() == 'NEEDS MAPPING'
        )
        # Check BOTH beneficiary columns
        has_beneficiary_error = (
            pd.isna(row.get('# of Beneficiaries Served')) and 
            pd.isna(row.get('Number of Individuals'))
        )
        has_duplicate_mapping = row.get('Duplicate_Mapping_Flag') == 'DUPLICATE MAPPING'
        
        if has_duplicate_mapping:
            return 'Check - Duplicate Mapping'
        elif has_mapping_error and has_beneficiary_error:
            return 'Check'
        elif has_mapping_error:
            return 'Check Mapping'
        elif has_beneficiary_error:
            return 'Check Beneficiaries'
        else:
            return 'For Validation'

    output_df['Validation Status'] = output_df.apply(determine_validation_status, axis=1)

    
    # Location columns (initialize as None, will be filled by PCodes)
    output_df['Region'] = None
    output_df['Province'] = output_df.get('Province', None)
    output_df['Prov_CODE'] = None
    output_df['Municipality/City'] = output_df.get('Municipality/City', None)
    output_df['Mun_Code'] = None
    output_df['Barangay'] = output_df.get('Barangay', None)
    output_df['Place Name'] = output_df.get('Location Notes/Place/Evacuation Center', 
                                            output_df.get('Location Notes/Place /Evacuation Center', None))

    # Add PCodes
    output_df = add_pcodes(output_df)
    
    # Operational columns
    output_df['DSR Intervention Team'] = None
    output_df['DSR Unit'] = None
    output_df['Status'] = None
    
    # Date handling
    date_columns = ['Date of Activity']
    date_col = None
    for col in date_columns:
        if col in output_df.columns:
            date_col = col
            break
    
    if date_col:
        output_df['Start Date'] = pd.to_datetime(output_df[date_col], errors='coerce')
        output_df['Month'] = output_df['Start Date'].dt.strftime('%B')
    else:
        output_df['Start Date'] = None
        output_df['Month'] = None
    
    output_df['End Date'] = None
    
    # Documentation
    output_df['Source'] = 'Chapter Statistical Report'
    output_df['Signature'] = None
    output_df['Weather System'] = None
    output_df['Remarks'] = output_df.get('Additional Comments', None)
    output_df['Date Modified'] = None
    
# Financial calculations
    output_df['Count'] = pd.to_numeric(output_df['Count'], errors='coerce').fillna(0)
    output_df['ACTIVITY COSTING'] = pd.to_numeric(output_df.get('COST', 0), errors='coerce').fillna(0)

    def calculate_total_cost(row):
        unit = str(row.get('Unit', '')).strip().upper()
        count = row.get('Count', 0)
        activity_cost = row.get('ACTIVITY COSTING', 0)
        beneficiaries = row.get('# of Beneficiaries Served', 0)
        
        # If unit is cash/pesos, multiply beneficiaries × cost per household
        if unit in ['PESOS', 'PHP', 'CASH', 'PESO']:
            # Use beneficiaries if available, otherwise fall back to count
            num_recipients = beneficiaries if pd.notna(beneficiaries) and beneficiaries > 0 else count
            return num_recipients * activity_cost
        else:
            # For items, it's count × unit cost
            return activity_cost * count

    output_df['Total Cost'] = output_df.apply(calculate_total_cost, axis=1)
    output_df['QTY'] = output_df['Count']

    # Final safety filter - remove any zeros that shouldn't be there
    output_df = output_df[output_df['Count'] > 0]
    
    # Select and order final columns
    final_columns = [
        "Organisation", "Implementing Partner/Supported By", "Phase", "Sector/Cluster",
        "Sub Sector", "Region", "Province", "Prov_CODE", "Municipality/City", "Mun_Code",
        "Barangay", "Place Name", "Activity", "Materials/Service Provided",
        "DSR Intervention Team", "Count", "Unit", "# of Beneficiaries Served",
        "Primary Beneficiary Served", "DSR Unit", "Status", "Start Date", "End Date",
        "Source", "Signature", "Weather System", "Remarks", "Date Modified",
        "ACTIVITY COSTING", "Total Cost", "Month", "Validation Status", "Number of Individuals",
        "Source_Filename", "Source_Row_Number" 
    ]
    
    # Only include columns that exist
    available_columns = [col for col in final_columns if col in output_df.columns]
    
    return output_df[available_columns]

def transform_to_opcen_format(df):
    """
    Transform processed data to OpCen_DSR_DA format.
    
    Returns: DataFrame with OpCen columns
    """
    
    output_df = df.copy()
    
    # Add validation flag for unmapped activities
    output_df['Validation Status'] = output_df['Sector'].apply(
        lambda x: 'FOR VALIDATION' if (pd.isna(x) or x == '' or str(x).strip() == '') else 'Validated'
    )

    # For unmapped rows, populate with raw activity name
    unmapped_mask = (output_df['Sector'].isna()) | (output_df['Sector'] == '') | (output_df['Sector'].str.strip() == '')
    output_df.loc[unmapped_mask, 'Sector/Cluster'] = 'REQUIRES MAPPING'

    # Use RawItemName_x (the original activity name from the raw data)
    if 'RawItemName_x' in output_df.columns:
        output_df.loc[unmapped_mask, 'INTERVENTION_TYPE'] = output_df.loc[unmapped_mask, 'RawItemName_x']
    elif 'RawItemName' in output_df.columns:
        output_df.loc[unmapped_mask, 'INTERVENTION_TYPE'] = output_df.loc[unmapped_mask, 'RawItemName']
    
    # OpCen column mappings
    output_df['DATE'] = pd.to_datetime(output_df.get('Date of Activity'), format='mixed', errors='coerce')
    output_df['REGION'] = None  # Will be added via PCodes later
    output_df['PROVINCE'] = output_df.get('Province', None)
    output_df['CHAPTER'] = output_df.get('Chapter', None)
    output_df['MUNICIPALITY'] = output_df.get('Municipality/City', None)
    output_df['BARANGAY'] = output_df.get('Barangay', None)
    output_df['EXACT LOCATION'] = output_df.get('Location Notes/Place/Evacuation Center', 
                                                  output_df.get('Location Notes/Place /Evacuation Center', None))
    output_df['SERVICE'] = None  # To be added later
    # Only use mapping table value for MAPPED rows
    mapped_mask = ~unmapped_mask
    output_df.loc[mapped_mask, 'INTERVENTION_TYPE'] = output_df.loc[mapped_mask, 'Activity']
        
    # Calculate QTY and beneficiaries
    output_df['QTY'] = pd.to_numeric(output_df.get('Count', 0), errors='coerce').fillna(0)
    output_df['UNIT'] = output_df.get('Unit', None)
    output_df['MENU'] = output_df.get('Additional Comments', None)
    output_df['MEALS'] = None
    output_df['PARTNERS'] = output_df.get('Relief Donor', None)
    output_df['PLATE NUMBER'] = None
    output_df['VEHICLE'] = None
    output_df['LATITUDE'] = None  # Will be added via PCodes later
    output_df['LONGITUDE'] = None  # Will be added via PCodes later
    output_df['PHOTO LINK'] = None
    
    # Calculate beneficiaries using the same logic as DMS 5W
    output_df['BENEFICIARIES'] = output_df.apply(calculate_beneficiary_units, axis=1)
    
    # Select final columns in correct order
    opcen_columns = [
        'DATE', 'REGION', 'PROVINCE', 'CHAPTER', 'MUNICIPALITY', 'BARANGAY',
        'EXACT LOCATION', 'SERVICE', 'INTERVENTION_TYPE', 'QTY', 'UNIT',
        'MENU', 'MEALS', 'PARTNERS', 'PLATE NUMBER', 'VEHICLE',
        'LATITUDE', 'LONGITUDE', 'PHOTO LINK', 'BENEFICIARIES'
    ]

    # Final safety filter - remove any zeros that shouldn't be there
    output_df = output_df[output_df['QTY'] > 0]

    return output_df[opcen_columns]