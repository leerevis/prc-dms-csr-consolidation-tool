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

This tool consolidates Chapter Relief data from multiple Chapter Statistical Reports into a single standardized output format.

### Quick Start Guide
```
📁 Prepare Files → ⚙️ Configure Settings → 📥 Upload/Link Data → 🔄 Process → 💾 Download
```

---

### Step-by-Step Instructions

**1. Configure Your Settings** ⚙️

- **Sheet Name:** Name of the worksheet in your Excel files (default: "Chapter Relief")
  - Only change if your files use a different sheet name
  - Must match exactly (case-sensitive)

- **Header Row:** Row number where column headers are located (default: 9)
  - Count from the top of the sheet
  - Only change if using a modified template

- **Output Format:** Choose your report format:
  - **DMS 5W** - Full humanitarian reporting format for external partners
  - **OpCen DSR Daily Assistance** - Simplified format for OpCen daily reports

---

**2. Prepare Your Data** 📁

**Google Drive Option (Recommended):**

*For Google Drive Folders:*
1. Create a folder in Google Drive
2. Add all your Chapter Statistical Report files to this folder
3. Right-click the folder → Share → Change to "Anyone with the link"
4. Set permission to "Viewer"
5. Click "Copy link"
6. Paste the link into the tool

*For Individual Google Sheets:*
1. Open the Google Sheet
2. Click Share → Change to "Anyone with the link"
3. Set permission to "Viewer"
4. Click "Copy link"
5. Paste the link into the tool

**Manual Upload Option:**
- Click "Upload Files Manually"
- Select one or more Excel files (.xlsx)
- Files can be Excel downloads or Google Sheets saved as .xlsx

**Important:** The tool works with:
- ✅ Google Drive folder links (multiple files)
- ✅ Individual Google Sheet links
- ✅ Excel files uploaded directly (.xlsx format only)

---

**3. Activity Mapping Table** 📋

The tool uses a standardized activity mapping table to categorize your activities.

- **Default mapping:** Leave "Use default activity mapping table" checked
- **Custom mapping:** Uncheck to upload your own mapping file

If activities in your files don't match the mapping table, they'll be flagged for review in the output.

---

**4. Process Your Data** 🔄

After configuring settings and providing your data source, the tool will:
1. Download/read all files
2. Identify activity columns automatically using fuzzy matching (handles typos)
3. Match activities against the mapping table
4. Calculate beneficiaries served using standardized formulas
5. Apply validation flags to identify issues
6. Transform data to your chosen output format
7. Consolidate everything into one file

Processing time depends on file size and number of files (typically 10-30 seconds per file).

---

**5. Review & Download Results** 💾

**Preview the Output:**
- Check the "Total Records" count
- Review the data preview table
- Look for validation flags in the "Validation Status" column

**Validation Status Meanings:**
- **For Validation:** Activity mapped successfully, ready for manual review
- **Check Mapping:** Activity not found in mapping table or has incomplete mapping data
- **Check Beneficiaries:** Beneficiary calculation failed (missing Quantity or People_Per_Beneficiary)
- **Check:** Both mapping and beneficiary issues present

**Download:**
- Click "Download Consolidated Report"
- File saved as: `DSR_Consolidated_YYYYMMDD_HHMMSS.xlsx`
- Contains one sheet with all consolidated data

---

### Common Errors & Solutions

**"Sheet 'Chapter Relief' not found"**
- **Cause:** The sheet name in your file doesn't match the configured name
- **Solution:** Check the sheet name in your Excel file (look at the tab at the bottom) and update the "Sheet Name" setting to match exactly

**"No Excel files found in the folder"**
- **Cause:** Google Drive folder is empty or contains no .xlsx files
- **Solution:** Verify you've uploaded .xlsx files to the folder, or that Google Sheets in the folder are being detected

**"Could not read as Google Sheet or download as Excel"**
- **Cause:** File sharing permissions incorrect or file is corrupted
- **Solution:** 
  1. Verify the file is shared as "Anyone with the link can view"
  2. Try downloading the file manually to check if it opens
  3. If it's a Google Sheet, try downloading as Excel and uploading directly

**"APIError: This operation is not supported for this document"**
- **Cause:** File is not a native Google Sheet (uploaded Excel file being viewed in Sheets)
- **Solution:** The tool will automatically try to download as Excel - ensure sharing permissions are set to "Anyone with the link can view"

**Activities showing "Check Mapping" status**
- **Cause:** Activity name not found in the mapping table
- **Solution:** Contact DMS to add the activity to the Activity Mapping Table, or check for typos in the activity name

**Activities showing "Check Beneficiaries" status**
- **Cause:** Missing "Quantity" or "People_Per_Beneficiary" data in the mapping table for this activity
- **Solution:** Contact DMS to complete the mapping table entry for this activity

**Beneficiaries showing as blank**
- **Cause:** Cash/monetary activities or activities that can't be converted to people (e.g., water in liters, stations)
- **Solution:** These require manual beneficiary entry - fill in manually after export

**Date columns showing strange values**
- **Cause:** Date format in source file not recognized
- **Solution:** Ensure dates in source files are in mm/dd/yyyy format or Excel date format

---

### Tips for Best Results

✅ **Use the official Chapter Statistical Report template** - Don't add or remove columns

✅ **Consistent naming** - Use the same activity names across all your files

✅ **Share correctly** - Always set Google links to "Anyone with the link can view"

✅ **Check your mapping** - If you see many "Check Mapping" flags, verify your activities match the taxonomy

✅ **Review validation flags** - Always check flagged rows before finalizing your report

✅ **Date formatting** - Use Excel date format or mm/dd/yyyy

❌ **Don't modify templates** - Adding custom columns may cause processing errors

❌ **Don't use restricted sharing** - "Restricted" or "People at your organization" sharing won't work

---

### Need Help?

If you encounter issues not covered here:
1. Check the validation status column in your output for specific error flags
2. Verify your source files use the correct template
3. Confirm Google sharing permissions are set correctly
4. Contact DMS for mapping table updates

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
                        
                    def seek(self, pos, whence=0):
                        return self.buffer.seek(pos, whence)
                    
                    def tell(self):
                        return self.buffer.tell()
                    
                    def seekable(self):  # ← Add this
                        return True
                    
                    def readable(self):  # ← And this while we're at it
                        return True
                    
                    def writable(self):  # ← And this
                        return False
                    
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
                            
                        def seek(self, pos, whence=0):
                            return self.buffer.seek(pos, whence)
                        
                        def tell(self):
                            return self.buffer.tell()
                        
                        def seekable(self):  # ← Add this
                            return True
                        
                        def readable(self):  # ← And this while we're at it
                            return True
                        
                        def writable(self):  # ← And this
                            return False
                    
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

        import os
        if os.path.exists('/tmp/debug_fuzzy.txt'):
            os.remove('/tmp/debug_fuzzy.txt')
        
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

        # Upload to BigQuery
        st.info("📤 Uploading to BigQuery data lake...")
        try:
            from bigquery_utils import upload_to_bigquery
            
            total_affected, new_count, updated_count = upload_to_bigquery(
                final_df, 
                credentials_dict, 
                uploaded_by="Streamlit User"  # TODO: Add user tracking
            )
            
            st.success(f"✅ BigQuery upload complete: {new_count} new records, {updated_count} updated records")
        except Exception as e:
            st.warning(f"⚠️ BigQuery upload failed: {str(e)}")
            st.write("Data is still available for download below.")
        
    # Summary statistics
        # Summary statistics
        st.subheader("📊 Summary")
        st.metric("Total Records", len(final_df))

        # ADD THIS HERE - Debug file download
        import os
        if os.path.exists('/tmp/debug_fuzzy.txt'):
            with open('/tmp/debug_fuzzy.txt', 'r') as f:
                debug_content = f.read()
            
            st.download_button(
                label="📥 Download Debug Log",
                data=debug_content,
                file_name="fuzzy_debug.txt",
                mime="text/plain"
            )
        
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
