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
from utils import extract_folder_id, download_files_from_drive, is_static_column, fuzzy_match_activity
from processing import process_single_file
from transformations import transform_to_output_schema, transform_to_opcen_format

import json
credentials_dict = dict(st.secrets["gcp_service_account"])



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

# Output format selection
output_format = st.radio(
    "Select output format:",
    ["DMS_5W", "OpCen_DSR_DA"],
    help="Choose which format to transform the data into"
)

# Raw data file uploader
#uploaded_file = st.file_uploader(
    #"Upload ONE raw Chapter Statistical Report (.xlsx)",
    #type=['xlsx'],
    #help="Upload a single Excel file to test"
#)

# Get files based on input method
files_to_process = []

if input_method == "Google Drive Folder":
    st.info("📁 Downloading files from Google Drive...")
    folder_id = extract_folder_id(gdrive_folder_url)
    files_to_process = download_files_from_drive(folder_id, credentials_dict)
    
    if not files_to_process:
        st.error("❌ No Excel files found in the folder")
        st.stop()
    
    st.success(f"✅ Downloaded {len(files_to_process)} files from Google Drive")
    
elif input_method == "Upload Files Manually":
    if uploaded_files:
        files_to_process = uploaded_files
    else:
        st.warning("⚠️ Please upload files to continue")
        st.stop()

# Process all files
# Process all files
if files_to_process:
    st.info(f"🔄 Processing {len(files_to_process)} file(s)...")
    
    all_outputs = []
    progress_bar = st.progress(0)
    
    for idx, file in enumerate(files_to_process):
        st.write(f"**Processing file:** {file.name}")
        
        try:
            # Process the file
            processed_df = process_single_file(file, mapping_df, sheet_name, header_row, STATIC_COLUMNS)
            
            if processed_df is not None:
                st.write(f"✅ Processed {len(processed_df)} rows")
                # Transform to output schema
                # Transform based on selected format
                if output_format == "DMS_5W":
                    output_df = transform_to_output_schema(processed_df)
                elif output_format == "OpCen_DSR_DA":
                    output_df = transform_to_opcen_format(processed_df)
                all_outputs.append(output_df)
                st.write(f"✅ Transformed to {len(output_df)} output rows")
            else:
                st.warning(f"⚠️ No valid data found in {file.name}")
        
        except Exception as e:
            st.error(f"❌ Error processing {file.name}: {str(e)}")
            st.exception(e)
        
        # Update progress
        progress_bar.progress((idx + 1) / len(files_to_process))
    
    # Check if we got any valid data
    if not all_outputs:
        st.error("❌ No valid data found in the uploaded files")
        st.stop()
    
    # Concatenate all outputs
    final_df = pd.concat(all_outputs, ignore_index=True)
    
    st.success(f"✅ Successfully processed {len(files_to_process)} file(s)!")
    
# Summary statistics
    st.subheader("📊 Summary")
    st.metric("Total Records", len(final_df))
    
    # Show preview
    st.subheader("📋 Final Output Preview")
    st.dataframe(final_df.head(20))
    
    # Download section
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<h3 style='text-align: center;'>📥 Download</h3>", unsafe_allow_html=True)
    
    # Create Excel fileF
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Consolidated Data')
    
    output.seek(0)
    
    # Download button
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
