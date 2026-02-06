import io
import re
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from thefuzz import fuzz

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


def download_files_from_drive(folder_id, credentials_dict):
    """Download all Excel files from a Google Drive folder"""
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    
    # Query for Excel files in the folder
    query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel') and trashed=false"
    
    results = drive_service.files().list(
        q=query,
        fields="files(id, name)"
    ).execute()
    
    files = results.get('files', [])
    
    if not files:
        return []
    
    downloaded_files = []
    
    for file in files:
        # Download file
        request = drive_service.files().get_media(fileId=file['id'])
        
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        file_content.seek(0)
        file_content.name = file['name']
        downloaded_files.append(file_content)
    
    return downloaded_files

from difflib import SequenceMatcher

def is_static_column(column_name, static_columns, threshold=90):
    """
    Check if a column name matches any static column using fuzzy matching.
    """
    from thefuzz import fuzz
    
    # DEBUG - write to file
    with open('/tmp/debug_fuzzy.txt', 'a') as f:
        best_match = max(static_columns, key=lambda x: fuzz.ratio(column_name.lower(), x.lower()))
        best_score = fuzz.ratio(column_name.lower(), best_match.lower())
        
        if best_score < threshold:
            f.write(f"REJECTED: '{column_name}' -> best match '{best_match}' = {best_score}%\n")
        else:
            f.write(f"MATCHED: '{column_name}' -> '{best_match}' = {best_score}%\n")
    
    for static_col in static_columns:
        similarity = fuzz.ratio(column_name.lower(), static_col.lower())
        if similarity >= threshold:
            return True
    return False
def fuzzy_match_activity(activity_name, mapping_df, threshold=0.90):
    """
    Try to find a matching activity using fuzzy matching
    Returns the matched RawItemName or None
    """
    activity_clean = str(activity_name).strip().lower()
    
    best_match = None
    best_score = 0
    
    for mapped_activity in mapping_df['RawItemName']:
        mapped_clean = str(mapped_activity).strip().lower()
        
        # Exact match
        if activity_clean == mapped_clean:
            return mapped_activity
        
        # Fuzzy match
        similarity = SequenceMatcher(None, activity_clean, mapped_clean).ratio()
        if similarity > best_score and similarity >= threshold:
            best_score = similarity
            best_match = mapped_activity
    
    return best_match

def detect_google_link_type(url):
    """
    Detect if a Google URL is a Drive folder or a Google Sheet
    
    Args:
        url: Google Drive or Google Sheets URL
        
    Returns: 
        'folder' if Drive folder
        'sheet' if Google Sheets
        None if neither
    """
    if 'drive.google.com/drive/folders/' in url:
        return 'folder'
    elif 'docs.google.com/spreadsheets/d/' in url:
        return 'sheet'
    else:
        return None


def extract_sheet_id(url):
    """
    Extract Google Sheet ID from URL
    
    Args:
        url: Google Sheets URL
        
    Returns: Sheet ID string or None
    """
    import re
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
    if match:
        return match.group(1)
    return None


def read_google_sheet(sheet_id, credentials_dict, sheet_name='Chapter Relief', header_row=9):
    """
    Read a Google Sheet directly using gspread API
    
    Args:
        sheet_id: Google Sheet ID
        credentials_dict: Service account credentials
        sheet_name: Name of the worksheet to read (default: 'Chapter Relief')
        header_row: Row number containing headers (1-indexed, default: 9)
        
    Returns: pandas DataFrame
    """
    from google.oauth2.service_account import Credentials
    import gspread
    import pandas as pd
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    sheets_client = gspread.authorize(creds)
    
    try:
        # Open the spreadsheet
        spreadsheet = sheets_client.open_by_key(sheet_id)
        
        # Try to get the worksheet by name
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            # If sheet name not found, list available sheets
            available_sheets = [ws.title for ws in spreadsheet.worksheets()]
            raise ValueError(f"Sheet '{sheet_name}' not found. Available sheets: {available_sheets}")
        
        # Get all data as list of lists
        data = worksheet.get_all_values()
        
        if not data or len(data) < header_row:
            raise ValueError(f"Sheet has insufficient rows (found {len(data)}, need at least {header_row})")
        
        # Convert to DataFrame, using header_row (convert to 0-indexed)
        header_idx = header_row - 1
        df = pd.DataFrame(data[header_idx+1:], columns=data[header_idx])
        
        return df
        
    except gspread.exceptions.APIError as e:
        raise Exception(f"Google Sheets API Error: {str(e)}. Check that the sheet is shared and accessible.")
    except Exception as e:
        raise