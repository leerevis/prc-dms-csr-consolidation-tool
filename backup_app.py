import streamlit as st
import pandas as pd
import io
from datetime import datetime
import re
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from config import STATIC_COLUMNS, DEFAULT_SHEET_NAME, DEFAULT_HEADER_ROW, DEFAULT_MAPPING_SHEET_ID

import json
with open('google_credentials.json', 'r') as f:
    credentials_dict = json.load(f)

st.set_page_config(page_title="PRC: Chapter Statistical Report - Data Consolidation", layout="wide")

st.title("🔄 PRC: Chapter Statistical Report - Data Consolidation Tool")
st.markdown("Transform and consolidate multiple Chapter Statistical Reports into one standardized output.")

# Configuration inputs
col1, col2 = st.columns(2)

with col1:
    sheet_name = st.text_input(
        "What is the Sheet Name?",
        value=DEFAULT_SHEET_NAME,
        help="Leave as default if the sheet name hasn't changed"
    )

with col2:
    header_row = st.number_input(
        "What Row are the Headers on?",
        value=DEFAULT_HEADER_ROW,
        min_value=1,
        step=1,
        help="Leave as default if unchanged"
    )

st.divider()


# Input method selection
input_method = st.radio(
    "Choose how to provide raw data files:",
    ["Google Drive Folder", "Upload Files Manually"],
    help="Select your preferred input method"
)

if input_method == "Google Drive Folder":
    gdrive_folder_url = st.text_input(
        "Google Drive Folder URL",
        placeholder="https://drive.google.com/drive/folders/...",
        help="Paste the shareable link to your Google Drive folder containing raw files"
    )
    
    if not gdrive_folder_url:
        st.warning("⚠️ Please provide a Google Drive folder URL")
        st.stop()
        
elif input_method == "Upload Files Manually":
    uploaded_files = st.file_uploader(
        "Upload raw DSR files (.xlsx)",
        type=['xlsx'],
        accept_multiple_files=True,
        help="Select one or more Excel files to process"
    )

st.divider()

# Mapping table loader
use_default_mapping = st.checkbox(
    "Use default activity mapping table", 
    value=True,
    help="Uncheck to upload a custom mapping table"
)

if use_default_mapping:
    # Load from Google Sheets using service account
    from google.oauth2.service_account import Credentials
    import gspread
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    sheets_client = gspread.authorize(creds)
    
    # Open the sheet and read data
    sheet = sheets_client.open_by_key(DEFAULT_MAPPING_SHEET_ID)
    worksheet = sheet.get_worksheet(0)  # First sheet
    data = worksheet.get_all_records()
    mapping_df = pd.DataFrame(data)
    
    st.success(f"✅ Using default mapping table with {len(mapping_df)} activities")
else:
    # Custom upload stays the same
    mapping_file = st.file_uploader(
        "Upload Custom Activity Mapping Table",
        type=['xlsx', 'csv'],
        help="Upload your activity mapping table"
    )
    
    if mapping_file is None:
        st.warning("⚠️ Please upload a mapping table to continue")
        st.stop()
    
    if mapping_file.name.endswith('.csv'):
        mapping_df = pd.read_csv(mapping_file)
    else:
        mapping_df = pd.read_excel(mapping_file)
    
    st.success(f"✅ Loaded custom mapping table with {len(mapping_df)} activities")

with st.expander("📋 View Mapping Table"):
    st.dataframe(mapping_df.head(10))

st.divider()

# Raw data file uploader
uploaded_file = st.file_uploader(
    "Upload ONE raw Chapter Statistical Report (.xlsx)",
    type=['xlsx'],
    help="Upload a single Excel file to test"
)

if uploaded_file:
    st.success(f"✅ File uploaded: {uploaded_file.name}")
    
    # Read the Excel file using user-specified settings
    df = pd.read_excel(
        uploaded_file, 
        sheet_name=sheet_name, 
        skiprows=header_row-1,
        dtype=str
    )

    st.subheader("📊 Raw Data Preview")
    st.write(f"**Shape:** {df.shape[0]} rows × {df.shape[1]} columns")
    
    st.write("**Column Names:**")
    st.write(list(df.columns))
    
    st.dataframe(df.head(10))

    # Identify activity columns
    activity_cols = [col for col in df.columns if col not in STATIC_COLUMNS]

    st.subheader("🔍 Column Analysis")
    col1, col2 = st.columns(2)

    with col1:
        st.write("**Static Columns (location/date info):**")
        for col in df.columns:
            if col in STATIC_COLUMNS:
                st.text(f"✓ {col}")

    with col2:
        st.write("**Activity Columns (will be unpivoted):**")
        for col in activity_cols:
            st.text(f"→ {col}")

    st.info(f"Found {len(activity_cols)} activity columns to process")

    st.subheader("🔄 Unpivoting Data")

    # Unpivot the activity columns
    melted_df = pd.melt(
        df,
        id_vars=[col for col in STATIC_COLUMNS if col in df.columns],
        value_vars=activity_cols,
        var_name='RawItemName',
        value_name='Count'
    )

    st.write(f"**Original shape:** {df.shape[0]} rows × {df.shape[1]} columns")
    st.write(f"**After unpivot:** {melted_df.shape[0]} rows × {melted_df.shape[1]} columns")

    st.dataframe(melted_df.head(20))

    # Clean data
    st.subheader("🧹 Cleaning Data")

    original_rows = len(melted_df)
    melted_df = melted_df[melted_df['Count'].notna()]
    melted_df = melted_df[melted_df['Count'] != '0']
    melted_df = melted_df[melted_df['Count'] != 0]

    rows_removed = original_rows - len(melted_df)

    st.write(f"**Rows before cleaning:** {original_rows}")
    st.write(f"**Rows after removing null/zero counts:** {len(melted_df)}")
    st.success(f"✅ Removed {rows_removed} empty activity rows") 

    st.subheader("🔗 Mapping Activities")

    # Merge with mapping table
    melted_df = melted_df.merge(
        mapping_df,
        on='RawItemName',
        how='left'
    )

    # Check for unmapped activities
    unmapped = melted_df[melted_df['Sector'].isna()]['RawItemName'].unique()

    if len(unmapped) > 0:
        st.warning(f"⚠️ Found {len(unmapped)} unmapped activities:")
        for activity in unmapped:
            st.text(f"  • {activity}")
    else:
        st.success("✅ All activities successfully mapped")

    st.write(f"**Rows with mapping:** {len(melted_df[melted_df['Sector'].notna()])}")
    st.dataframe(melted_df.head(20))

    st.subheader("📝 Creating Output Format")

    # Keep only successfully mapped rows
    output_df = melted_df[melted_df['Sector'].notna()].copy()

    # Create output columns
    output_df['Organisation'] = 'Philippine Red Cross'
    output_df['Implementing Partner/Supported By'] = output_df.get('Relief Donor', None)
    output_df['Phase'] = None
    output_df['Sector/Cluster'] = output_df['Sector']
    output_df['Sub Sector'] = output_df.get('Sub - Sector', output_df.get('Sub Sector', None))
    output_df['Region'] = None
    output_df['Province'] = output_df.get('Province', None)
    output_df['Prov_CODE'] = None
    output_df['Municipality/City'] = output_df.get('Municipality/City', None)
    output_df['Mun_Code'] = None
    output_df['Barangay'] = output_df.get('Barangay', None)
    output_df['Place Name'] = output_df.get('Location Notes/Place/Evacuation Center', None)
    output_df['Activity'] = output_df.get('Activity', None)
    output_df['Materials/Service Provided'] = output_df.get('Assistance? Materials/service', None)
    output_df['DSR Intervention Team'] = None
    # Count column already exists
    output_df['Unit'] = output_df.get('Unit', None)
    output_df['# of Beneficiaries Served'] = output_df.get('# of beneficiaries served', None)
    output_df['Primary Beneficiary Served'] = output_df.get('Beneficiary Served', None)
    output_df['DSR Unit'] = None
    output_df['Status'] = None

    # Handle dates
    if 'Date of Activity' in output_df.columns:
        output_df['Start Date'] = pd.to_datetime(output_df['Date of Activity'], errors='coerce')
        output_df['Month'] = output_df['Start Date'].dt.strftime('%B')
    else:
        output_df['Start Date'] = None
        output_df['Month'] = None

    output_df['End Date'] = None
    output_df['Source'] = 'Chapter Statistical Report'
    output_df['Signature'] = None
    output_df['Weather System'] = None
    output_df['Remarks'] = output_df.get('Additional Comments', None)
    output_df['Date Modified'] = None

    # Cost calculations
    output_df['ACTIVITY COSTING'] = pd.to_numeric(output_df.get('COST', 0), errors='coerce').fillna(0)
    output_df['Count'] = pd.to_numeric(output_df['Count'], errors='coerce').fillna(0)
    output_df['Total Cost'] = output_df['ACTIVITY COSTING'] * output_df['Count']

    st.success(f"✅ Created output with {len(output_df)} rows")
    st.dataframe(output_df.head(10))

    st.subheader("📋 Final Output")

    # Define final column order
    OUTPUT_COLUMNS = [
        "Organisation", "Implementing Partner/Supported By", "Phase", "Sector/Cluster",
        "Sub Sector", "Region", "Province", "Prov_CODE", "Municipality/City", "Mun_Code",
        "Barangay", "Place Name", "Activity", "Materials/Service Provided",
        "DSR Intervention Team", "Count", "Unit", "# of Beneficiaries Served",
        "Primary Beneficiary Served", "DSR Unit", "Status", "Start Date", "End Date",
        "Source", "Signature", "Weather System", "Remarks", "Date Modified",
        "ACTIVITY COSTING", "Total Cost", "Month"
    ]

    # Select only the output columns
    final_df = output_df[OUTPUT_COLUMNS]

    st.write(f"**Final output:** {final_df.shape[0]} rows × {final_df.shape[1]} columns")
    st.dataframe(final_df)

    # Summary stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Activities", len(final_df))
    with col2:
        st.metric("Total Beneficiaries", int(final_df['Count'].sum()))
    with col3:
        st.metric("Total Cost", f"₱{final_df['Total Cost'].sum():,.2f}")

    st.subheader("📥 Download")

    # Create Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Consolidated Data')

    output.seek(0)

    # Center the subheader and download button
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<h3 style='text-align: center;'>📥 Download</h3>", unsafe_allow_html=True)
        
    # Create Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Consolidated Data')

    output.seek(0)

    # Download button in the same centered column
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.download_button(
            label="📥 Download Consolidated Report",
            data=output,
            file_name=f"DSR_Consolidated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True
        )