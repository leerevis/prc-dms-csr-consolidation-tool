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
st.markdown("Transform and consolidate the Chapter Relief data from multiple Chapter Statistical Reports into either a Disaster Management Services 5W or an OpCen Disaster Statistical Report Daily Activities format.")

tab1, tab2 = st.tabs(["📖 How to Use", "📊 Tool"])

with tab1:
    st.markdown("""## How to Use This Tool

This tool consolidates the Chapter Relief data from multiple Chapter Statistical Report activity files into a single standardised output format.

### Quick Start Guide

```
📁 Prepare Files → ⚙️ Configure Settings → 📥 Upload/Link Data → 🔄 Process → 💾 Download
```

---

### Step-by-Step Instructions

**1. Configure Your Settings** ⚙️
- **Sheet Name:** Enter the name of the sheet in your Excel files (default: "Chapter Relief")
- **Header Row:** Specify which row contains column headers (default: 9)
- **Output Format:** Choose between:
  - **DMS 5W** - For external audiences and returning
  - **OpCen DSR DA** - For the daily assistance sheet of the OpCen Disaster Statistical Report

**2. Choose Your Data Source** 📁

You have two options:

- **Google Drive Folder** (Recommended for multiple files):
  1. Upload all your Google files to a Google Drive folder
  2. Set both file and folder sharing to "Anyone with the link can view"
  3. Copy the folder URL
  4. Paste it into the tool

- **Manual Upload**:
  1. Click "Upload Files Manually"
  2. Select one or more Excel files (.xlsx) or Google Sheets

**3. Process Your Data** 🔄

Click the upload button or paste your Drive URL. The tool will:
- Download/read all Excel files
- Identify activity columns automatically
- Match activities with the mapping table (with fuzzy matching for typos)
- Transform data to your chosen output format
- Consolidate everything into one file

**4. Download Results** 💾

- Review the preview of consolidated data
- Check the "Total Records" summary
- Click **"Download Consolidated Report"** to save the Excel file
- Filename includes timestamp: `DSR_Consolidated_YYYYMMDD_HHMMSS.xlsx`

---

### Output Files

Your download will contain:
- **Mapped Activities Sheet** - All successfully processed activities
- **Unmapped Activities Sheet** (if any) - Activities not found in the mapping table

---

### Tips for Best Results

- **Ensure consistent file structure** - All files should use the Chapter Statistical Report template
- **Check mapping table** - New activity types should be added to the Google Sheet Activity Taxonomy, with permission from DMS
- **Use public folders** - Google Drive folders must be set to "Anyone with link" for access
- **Verify dates** - Dates should be in mm/dd/yyyy format in source files
- **Review unmapped activities** - Add any unmapped items to the mapping table for future runs, with permission from DMS

---

### Troubleshooting

**Problem:** Activities marked as "unmapped"
- **Solution:** Add the activity to the Google Sheets mapping table with proper categorization

**Problem:** Wrong columns appearing in output
- **Solution:** Verify sheet name and header row number are correct

**Problem:** Google Drive files not downloading
- **Solution:** Ensure folder is shared as "Anyone with the link can view"

**Problem:** Date formatting issues
- **Solution:** Check that dates in source files are in mm/dd/yyyy format

""")

with tab2:
    col1, col2, col3 = st.columns(3)

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

    with col3:
        output_format = st.selectbox(
            "Output Format:",
            ["DMS 5W", "OpCen DSR Daily Assistance"],
            help="Choose which format to transform the data into"
        )

    st.divider()


    # Input method selection
    input_method = st.radio(
        "Choose how to provide raw data files:",
        ["Google Link (Folder or Sheet)", "Upload Files Manually"],
        help="Paste a Google Drive folder or Google Sheet link, or upload Excel files"
    )

    if input_method == "Google Link (Folder or Sheet)":
        google_url = st.text_input(
            "Google Drive Folder or Sheet URL",
            placeholder="https://drive.google.com/... or https://docs.google.com/spreadsheets/...",
            help="Paste a link to a Google Drive folder (multiple files) or a single Google Sheet"
        )
        
        if not google_url:
            st.warning("⚠️ Please provide a Google link")
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

    # Get files based on input method
    files_to_process = []

    if input_method == "Google Link (Folder or Sheet)":
        from utils import detect_google_link_type, extract_folder_id, extract_sheet_id, read_google_sheet, download_files_from_drive
        
        link_type = detect_google_link_type(google_url)
        
        if link_type == 'folder':
            st.info("📁 Detected: Google Drive Folder - Downloading files...")
            folder_id = extract_folder_id(google_url)
            files_to_process = download_files_from_drive(folder_id, credentials_dict)
            
            if not files_to_process:
                st.error("❌ No Excel files found in the folder")
                st.stop()
            
            st.success(f"✅ Downloaded {len(files_to_process)} files from Google Drive")
            
        elif link_type == 'sheet':
            st.info("📊 Detected: Google Sheet - Reading data...")
            sheet_id = extract_sheet_id(google_url)
            
            if not sheet_id:
                st.error("❌ Could not extract Sheet ID from URL")
                st.stop()
            
            try:
                # First, try to read as native Google Sheet
                df = read_google_sheet(sheet_id, credentials_dict, sheet_name, header_row)
                
                # Convert DataFrame to file-like object
                import io
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=header_row-1)
                buffer.seek(0)
                
                class MemoryFile:
                    def __init__(self, buffer, name):
                        self.buffer = buffer
                        self.name = name
                        
                    def read(self, size=-1):
                        return self.buffer.read(size)
                        
                    def seek(self, pos, whence=0):  # ← Add whence parameter with default 0
                        return self.buffer.seek(pos, whence)
                    
                    def tell(self):  # ← Add this
                        return self.buffer.tell()
                    
                memory_file = MemoryFile(buffer, "GoogleSheet.xlsx")
                files_to_process = [memory_file]
                st.success(f"✅ Successfully loaded Google Sheet")
                
            except Exception as sheets_error:
                # If Sheets API fails, try downloading as Excel file via Drive API
                st.info("📥 Not a native Google Sheet - downloading as Excel file...")
                try:
                    from googleapiclient.discovery import build
                    from googleapiclient.http import MediaIoBaseDownload
                    from google.oauth2.service_account import Credentials
                    
                    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
                    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
                    service = build('drive', 'v3', credentials=creds)
                    
                    # Download the file
                    request = service.files().get_media(fileId=sheet_id)
                    file_buffer = io.BytesIO()
                    downloader = MediaIoBaseDownload(file_buffer, request)
                    
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                    
                    file_buffer.seek(0)
                    
                    class MemoryFile:
                        def __init__(self, buffer, name):
                            self.buffer = buffer
                            self.name = name
                            
                        def read(self, size=-1):
                            return self.buffer.read(size)
                            
                        def seek(self, pos, whence=0):  # ← Add whence parameter with default 0
                            return self.buffer.seek(pos, whence)
                        
                        def tell(self):  # ← Add this
                            return self.buffer.tell()
                    
                    memory_file = MemoryFile(file_buffer, "DriveExcel.xlsx")
                    files_to_process = [memory_file]
                    st.success(f"✅ Successfully downloaded Excel file from Drive")
                    
                except Exception as drive_error:
                    st.error(f"❌ Could not read as Google Sheet or download as Excel")
                    st.write("**Sheets API Error:**", str(sheets_error))
                    st.write("**Drive API Error:**", str(drive_error))
                    st.write("**Troubleshooting:**")
                    st.write("1. Ensure the file is shared as 'Anyone with the link can view'")
                    st.write(f"2. Verify the sheet name is '{sheet_name}' (if Google Sheet)")
                    st.write(f"3. Verify headers are on row {header_row}")
                    st.stop()
        
        else:
            st.error("❌ Invalid Google link. Please provide a Google Drive folder or Google Sheets URL.")
            st.stop()
            
    elif input_method == "Upload Files Manually":
        if uploaded_files:
            files_to_process = uploaded_files
        else:
            st.warning("⚠️ Please upload files to continue")
            st.stop()

    # Process all files (rest of your code stays the same from here)
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
                    if output_format == "DMS 5W":
                        output_df = transform_to_output_schema(processed_df)
                    elif output_format == "OpCen DSR Daily Assistance":
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
        # Create Excel file with multiple sheets
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Sheet 1: Successfully mapped activities
            final_df.to_excel(writer, index=False, sheet_name='Mapped Activities')
            
            # Sheet 2: Unmapped activities (if any exist)
            # We need to collect these during processing...

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
