import streamlit as st
import pandas as pd
import io
from datetime import datetime
import re
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import tempfile
import os

st.set_page_config(page_title="DSR Data Consolidation", layout="wide")

st.title("🔄 DSR Data Consolidation Tool")
st.markdown("Transform and consolidate multiple DSR activity reports into one standardized output.")

# Sidebar for inputs
with st.sidebar:
    st.header("📥 Input Method")
    
    input_method = st.radio(
        "Choose input method:",
        ["Upload Files", "Google Drive Folder"],
        help="Select how you want to provide the raw data files"
    )
    
    if input_method == "Upload Files":
        # File upload for raw data files
        uploaded_files = st.file_uploader(
            "Upload Raw DSR Files (.xlsx)",
            type=['xlsx'],
            accept_multiple_files=True,
            help="Select all Excel files you want to consolidate"
        )
        gdrive_folder_url = None
    else:
        # Google Drive folder URL
        gdrive_folder_url = st.text_input(
            "Google Drive Folder URL",
            placeholder="https://drive.google.com/drive/folders/...",
            help="Paste the shareable link to your Google Drive folder"
        )
        uploaded_files = None
    
    st.divider()
    
    # Activity mapping input
    mapping_method = st.radio(
        "Activity Mapping Source:",
        ["Upload File", "Google Sheet URL"],
        help="How to provide the activity mapping table"
    )
    
    if mapping_method == "Upload File":
        mapping_file = st.file_uploader(
            "Upload Activity Mapping Table",
            type=['xlsx', 'csv'],
            help="Upload your tblActivityMap file"
        )
        mapping_sheet_url = None
    else:
        mapping_sheet_url = st.text_input(
            "Google Sheet URL",
            placeholder="https://docs.google.com/spreadsheets/d/...",
            help="Paste the shareable link to your mapping Google Sheet"
        )
        mapping_file = None
    
    st.divider()
    
    # Google credentials
    with st.expander("🔑 Google API Setup (Required for Drive/Sheets)"):
        st.markdown("""
        To use Google Drive or Google Sheets, you need to:
        1. Create a Google Cloud Project
        2. Enable Drive API and Sheets API
        3. Create a Service Account
        4. Download the credentials JSON
        
        [Setup Guide](https://cloud.google.com/docs/authentication/getting-started)
        """)
        
        credentials_file = st.file_uploader(
            "Upload Service Account Credentials (JSON)",
            type=['json'],
            help="Upload your Google service account credentials"
        )
    
    process_button = st.button("🚀 Process Files", type="primary", use_container_width=True)

# Static columns that should not be unpivoted
STATIC_COLUMNS = [
    "Date of Activity",
    "Location Notes/Place/Evacuation Center",
    "Barangay",
    "Municipality/City",
    "Province",
    "Chapter",
    "Relief Donor",
    "Additional Comments"
]

# Output schema
OUTPUT_COLUMNS = [
    "Organisation", "Implementing Partner/Supported By", "Phase", "Sector/Cluster",
    "Sub Sector", "Region", "Province", "Prov_CODE", "Municipality/City", "Mun_Code",
    "Barangay", "Place Name", "Activity", "Materials/Service Provided",
    "DSR Intervention Team", "Count", "Unit", "# of Beneficiaries Served",
    "Primary Beneficiary Served", "DSR Unit", "Status", "Start Date", "End Date",
    "Source", "Signature", "Weather System", "Remarks", "Date Modified",
    "ACTIVITY COSTING", "Total Cost", "Month"
]

@st.cache_resource
def get_google_services(credentials_dict):
    """Initialize Google API services"""
    SCOPES = [
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/spreadsheets.readonly'
    ]
    
    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_client = gspread.authorize(creds)
    
    return drive_service, sheets_client

def extract_folder_id(url):
    """Extract folder ID from Google Drive URL"""
    patterns = [
        r'/folders/([a-zA-Z0-9-_]+)',
        r'id=([a-zA-Z0-9-_]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return url  # Assume it's already an ID

def extract_sheet_id(url):
    """Extract spreadsheet ID from Google Sheets URL"""
    patterns = [
        r'/d/([a-zA-Z0-9-_]+)',
        r'spreadsheets/d/([a-zA-Z0-9-_]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return url  # Assume it's already an ID

def download_files_from_drive(drive_service, folder_id):
    """Download all Excel files from a Google Drive folder"""
    files = []
    
    # Query for Excel files in the folder
    query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    
    results = drive_service.files().list(
        q=query,
        fields="files(id, name, mimeType)"
    ).execute()
    
    items = results.get('files', [])
    
    if not items:
        return []
    
    for item in items:
        # Download file
        request = drive_service.files().get_media(fileId=item['id'])
        
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        file_content.seek(0)
        file_content.name = item['name']
        files.append(file_content)
    
    return files

def load_mapping_from_gsheet(sheets_client, sheet_id):
    """Load mapping table from Google Sheets"""
    sheet = sheets_client.open_by_key(sheet_id)
    worksheet = sheet.get_worksheet(0)  # First sheet
    
    data = worksheet.get_all_records()
    mapping_df = pd.DataFrame(data)
    
    # Standardize column names
    mapping_df.columns = mapping_df.columns.str.strip()
    return mapping_df

def load_mapping_table(mapping_file=None, mapping_sheet_url=None, sheets_client=None):
    """Load the activity mapping table from file or Google Sheets"""
    if mapping_file:
        if mapping_file.name.endswith('.csv'):
            mapping_df = pd.read_csv(mapping_file)
        else:
            mapping_df = pd.read_excel(mapping_file)
    elif mapping_sheet_url and sheets_client:
        sheet_id = extract_sheet_id(mapping_sheet_url)
        mapping_df = load_mapping_from_gsheet(sheets_client, sheet_id)
    else:
        return None
    
    # Standardize column names
    mapping_df.columns = mapping_df.columns.str.strip()
    return mapping_df

def process_raw_file(file, mapping_df):
    """Process a single raw DSR file"""
    # Read the Excel file
    df = pd.read_excel(file)
    
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    # Identify which columns exist in this file
    existing_static_cols = [col for col in STATIC_COLUMNS if col in df.columns]
    
    # Get activity columns (everything that's not a static column)
    activity_cols = [col for col in df.columns if col not in existing_static_cols]
    
    # Remove empty activity columns
    activity_cols = [col for col in activity_cols if df[col].notna().any()]
    
    if not activity_cols:
        return None, []
    
    # Unpivot (melt) the activity columns
    id_vars = existing_static_cols
    melted_df = pd.melt(
        df,
        id_vars=id_vars,
        value_vars=activity_cols,
        var_name='RawItemName',
        value_name='Count'
    )
    
    # Remove rows with null or zero counts
    melted_df = melted_df[melted_df['Count'].notna()]
    melted_df = melted_df[melted_df['Count'] != 0]
    
    if melted_df.empty:
        return None, []
    
    # Merge with mapping table
    melted_df = melted_df.merge(
        mapping_df,
        on='RawItemName',
        how='left'
    )
    
    # Identify unmapped activities
    unmapped = melted_df[melted_df['Sector'].isna()]['RawItemName'].unique().tolist()
    
    # Keep only mapped rows for main output
    mapped_df = melted_df[melted_df['Sector'].notna()].copy()
    
    if mapped_df.empty:
        return None, unmapped
    
    # Transform to output schema
    output_df = pd.DataFrame()
    
    # Static values
    output_df['Organisation'] = 'Philippine Red Cross'
    output_df['Implementing Partner/Supported By'] = mapped_df.get('Relief Donor', None)
    output_df['Phase'] = None
    output_df['Sector/Cluster'] = mapped_df['Sector']
    output_df['Sub Sector'] = mapped_df.get('Sub - Sector', mapped_df.get('Sub Sector', None))
    output_df['Region'] = None
    output_df['Province'] = mapped_df.get('Province', None)
    output_df['Prov_CODE'] = None
    output_df['Municipality/City'] = mapped_df.get('Municipality/City', None)
    output_df['Mun_Code'] = None
    output_df['Barangay'] = mapped_df.get('Barangay', None)
    output_df['Place Name'] = mapped_df.get('Location Notes/Place/Evacuation Center', None)
    output_df['Activity'] = mapped_df.get('Activity', None)
    output_df['Materials/Service Provided'] = mapped_df.get('Assistance? Materials/service', None)
    output_df['DSR Intervention Team'] = None
    output_df['Count'] = mapped_df['Count']
    output_df['Unit'] = mapped_df.get('Unit', None)
    output_df['# of Beneficiaries Served'] = mapped_df.get('# of beneficiaries served', None)
    output_df['Primary Beneficiary Served'] = mapped_df.get('Beneficiary Served', None)
    output_df['DSR Unit'] = None
    output_df['Status'] = None
    
    # Date handling
    date_col = mapped_df.get('Date of Activity', None)
    if date_col is not None:
        output_df['Start Date'] = pd.to_datetime(date_col, errors='coerce')
        # Extract month name - handle the mm/dd/yyyy format correctly
        output_df['Month'] = output_df['Start Date'].apply(
            lambda x: x.strftime('%B') if pd.notna(x) else None
        )
    else:
        output_df['Start Date'] = None
        output_df['Month'] = None
    
    output_df['End Date'] = None
    output_df['Source'] = 'Chapter Statistical Report'
    output_df['Signature'] = None
    output_df['Weather System'] = None
    output_df['Remarks'] = mapped_df.get('Additional Comments', None)
    output_df['Date Modified'] = None
    
    # Cost calculations
    output_df['ACTIVITY COSTING'] = mapped_df.get('COST', 0)
    output_df['Total Cost'] = output_df['ACTIVITY COSTING'] * output_df['Count']
    
    return output_df, unmapped

# Main processing
if process_button:
    # Validation
    if input_method == "Upload Files" and not uploaded_files:
        st.error("⚠️ Please upload at least one raw DSR file.")
    elif input_method == "Google Drive Folder" and not gdrive_folder_url:
        st.error("⚠️ Please provide a Google Drive folder URL.")
    elif mapping_method == "Upload File" and not mapping_file:
        st.error("⚠️ Please upload the activity mapping table.")
    elif mapping_method == "Google Sheet URL" and not mapping_sheet_url:
        st.error("⚠️ Please provide the Google Sheet URL for the mapping table.")
    elif (input_method == "Google Drive Folder" or mapping_method == "Google Sheet URL") and not credentials_file:
        st.error("⚠️ Please upload Google API credentials to use Drive or Sheets integration.")
    else:
        with st.spinner("Processing files..."):
            try:
                # Initialize Google services if needed
                drive_service = None
                sheets_client = None
                
                if credentials_file:
                    import json
                    credentials_dict = json.load(credentials_file)
                    drive_service, sheets_client = get_google_services(credentials_dict)
                    st.success("✅ Connected to Google services")
                
                # Get files
                files_to_process = []
                
                if input_method == "Upload Files":
                    files_to_process = uploaded_files
                else:
                    folder_id = extract_folder_id(gdrive_folder_url)
                    st.info(f"📁 Downloading files from folder: {folder_id}")
                    files_to_process = download_files_from_drive(drive_service, folder_id)
                    st.success(f"✅ Downloaded {len(files_to_process)} files from Google Drive")
                
                # Load mapping table
                mapping_df = load_mapping_table(mapping_file, mapping_sheet_url, sheets_client)
                
                if mapping_df is None:
                    st.error("❌ Failed to load mapping table")
                else:
                    st.success(f"✅ Loaded mapping table with {len(mapping_df)} activities")
                    
                    # Process all files
                    all_outputs = []
                    all_unmapped = set()
                    
                    progress_bar = st.progress(0)
                    for idx, file in enumerate(files_to_process):
                        output_df, unmapped = process_raw_file(file, mapping_df)
                        
                        if output_df is not None:
                            all_outputs.append(output_df)
                        
                        all_unmapped.update(unmapped)
                        progress_bar.progress((idx + 1) / len(files_to_process))
                    
                    # Consolidate all outputs
                    if all_outputs:
                        consolidated_df = pd.concat(all_outputs, ignore_index=True)
                        
                        # Reorder columns to match output schema
                        final_columns = [col for col in OUTPUT_COLUMNS if col in consolidated_df.columns]
                        consolidated_df = consolidated_df[final_columns]
                        
                        st.success(f"✅ Successfully processed {len(files_to_process)} files!")
                        st.metric("Total Activities", len(consolidated_df))
                        
                        # Display summary
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Unique Locations", consolidated_df['Barangay'].nunique())
                        with col2:
                            st.metric("Total Beneficiaries", consolidated_df['Count'].sum())
                        with col3:
                            st.metric("Total Cost", f"₱{consolidated_df['Total Cost'].sum():,.2f}")
                        
                        # Show preview
                        st.subheader("📊 Data Preview")
                        st.dataframe(consolidated_df.head(20), use_container_width=True)
                        
                        # Download button
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            consolidated_df.to_excel(writer, index=False, sheet_name='Consolidated Data')
                            
                            # Add unmapped activities sheet if any
                            if all_unmapped:
                                unmapped_df = pd.DataFrame({
                                    'Unmapped Activity': list(all_unmapped),
                                    'Action Required': 'Add to mapping table'
                                })
                                unmapped_df.to_excel(writer, index=False, sheet_name='Unmapped Activities')
                        
                        output.seek(0)
                        
                        st.download_button(
                            label="📥 Download Consolidated Report",
                            data=output,
                            file_name=f"DSR_Consolidated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary",
                            use_container_width=True
                        )
                        
                        # Show unmapped activities warning
                        if all_unmapped:
                            st.warning(f"⚠️ Found {len(all_unmapped)} unmapped activities. Check the 'Unmapped Activities' sheet in the downloaded file.")
                            with st.expander("View Unmapped Activities"):
                                for activity in sorted(all_unmapped):
                                    st.text(f"• {activity}")
                    else:
                        st.error("❌ No valid data found in the uploaded files.")
                        
            except Exception as e:
                st.error(f"❌ Error processing files: {str(e)}")
                st.exception(e)

# Instructions
with st.expander("📖 How to Use This Tool"):
    st.markdown("""
    ### Step-by-Step Instructions:
    
    #### Option 1: Upload Files Directly
    1. Select "Upload Files" method
    2. Upload your raw DSR Excel files
    3. Upload or link to your Activity Mapping Table
    4. Click "Process Files"
    
    #### Option 2: Google Drive Integration
    1. **Set up Google API credentials** (one-time setup):
       - Create a Google Cloud Project
       - Enable Drive API and Sheets API
       - Create a Service Account
       - Download credentials JSON
       - Share your Drive folder/Sheet with the service account email
    
    2. **Process files**:
       - Select "Google Drive Folder" method
       - Paste the shareable link to your folder
       - Choose mapping source (file upload or Google Sheet)
       - Upload credentials JSON
       - Click "Process Files"
    
    3. **Download Results**:
       - Review the preview
       - Download consolidated report
       - Check for unmapped activities
    
    ### Required Mapping Table Columns:
    - RawItemName
    - Sector
    - Sub Sector (or "Sub - Sector")
    - Activity
    - Assistance? Materials/service
    - Beneficiary Served
    - # of beneficiaries served
    - Quantity
    - Unit
    - COST
    
    ### Tips:
    - Keep your mapping Google Sheet updated with new activities
    - Files in Drive folder must be .xlsx format
    - Unmapped activities will appear in a separate sheet
    """)