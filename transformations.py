import pandas as pd

def calculate_beneficiaries(row):
    try:
        count = pd.to_numeric(row.get('Count', 0), errors='coerce')
        if pd.isna(count):
            count = 0
        
        beneficiary_type = row.get('Beneficiary Served', '')
        
        if beneficiary_type == 'Families':
            return count * 5
        elif beneficiary_type == 'Individuals':
            return count
        else:
            # Try to use value from mapping table
            mapped_value = row.get('# of beneficiaries served', 0)
            return pd.to_numeric(mapped_value, errors='coerce') or 0
    except:
        return 0


def transform_to_output_schema(df):
    """
    Transform the processed DataFrame to the final output schema.
    Takes merged data with mapping columns and creates standardized output.
    
    Returns: DataFrame with final output columns
    """
    
    output_df = df.copy()

    # Add validation flag - treat blank Sector as unmapped
    output_df['Validation Status'] = output_df['Sector'].apply(
        lambda x: 'FOR VALIDATION' if (pd.isna(x) or x == '' or str(x).strip() == '') else 'Validated'
    )

    # For unmapped rows (null OR blank Sector), populate with raw activity name
    unmapped_mask = (output_df['Sector'].isna()) | (output_df['Sector'] == '') | (output_df['Sector'].str.strip() == '')
    output_df.loc[unmapped_mask, 'Sector/Cluster'] = 'REQUIRES MAPPING'
    if 'RawItemName' in output_df.columns:
        output_df.loc[unmapped_mask, 'Materials/Service Provided'] = output_df.loc[unmapped_mask, 'RawItemName']

    # Static values
    output_df['Organisation'] = 'Philippine Red Cross'
    output_df['Implementing Partner/Supported By'] = output_df.get('Relief Donor', None)
    output_df['Phase'] = None
    
    # Mapping table columns
    output_df['Sector/Cluster'] = output_df['Sector']
    output_df['Sub Sector'] = output_df.get('Sub - Sector', output_df.get('Sub Sector', None))
    output_df['Activity'] = output_df.get('Activity', None)
    output_df['Materials/Service Provided'] = output_df.get('Assistance? Materials/service', None)
    output_df['Unit'] = output_df.get('Unit', None)
    output_df['# of Beneficiaries Served'] = output_df.get('# of beneficiaries served', None)
    output_df['Primary Beneficiary Served'] = output_df.get('Beneficiary Served', None)
    
    # Location columns
    output_df['Region'] = None
    output_df['Province'] = output_df.get('Province', None)
    output_df['Prov_CODE'] = None
    output_df['Municipality/City'] = output_df.get('Municipality/City', None)
    output_df['Mun_Code'] = None
    output_df['Barangay'] = output_df.get('Barangay', None)
    output_df['Place Name'] = output_df.get('Location Notes/Place/Evacuation Center', 
                                             output_df.get('Location Notes/Place /Evacuation Center', None))
    
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
    output_df['Total Cost'] = output_df['ACTIVITY COSTING'] * output_df['Count']
    
    # Select and order final columns
    final_columns = [
        "Organisation", "Implementing Partner/Supported By", "Phase", "Sector/Cluster",
        "Sub Sector", "Region", "Province", "Prov_CODE", "Municipality/City", "Mun_Code",
        "Barangay", "Place Name", "Activity", "Materials/Service Provided",
        "DSR Intervention Team", "Count", "Unit", "# of Beneficiaries Served",
        "Primary Beneficiary Served", "DSR Unit", "Status", "Start Date", "End Date",
        "Source", "Signature", "Weather System", "Remarks", "Date Modified",
        "ACTIVITY COSTING", "Total Cost", "Month", "Validation Status"
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
    output_df['INTERVENTION_TYPE'] = output_df.get('Activity', None)
    
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
    
    # Keep raw beneficiary type - no calculation
    output_df['BENEFICIARIES'] = output_df.get('Beneficiary Served', None)
    
    # Select final columns in correct order
    opcen_columns = [
        'DATE', 'REGION', 'PROVINCE', 'CHAPTER', 'MUNICIPALITY', 'BARANGAY',
        'EXACT LOCATION', 'SERVICE', 'INTERVENTION_TYPE', 'QTY', 'UNIT',
        'MENU', 'MEALS', 'PARTNERS', 'PLATE NUMBER', 'VEHICLE',
        'LATITUDE', 'LONGITUDE', 'PHOTO LINK', 'BENEFICIARIES'
    ]
    
    return output_df[opcen_columns]