import io
import re
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

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

def is_static_column(column_name, static_columns, threshold=0.85):
    """
    Check if a column is static using fuzzy matching
    threshold: 0.0 to 1.0, where 1.0 is exact match
    """
    column_clean = column_name.strip().lower()
    
    for static_col in static_columns:
        static_clean = static_col.strip().lower()
        
        # Exact match after cleaning
        if column_clean == static_clean:
            return True
        
        # Fuzzy match
        similarity = SequenceMatcher(None, column_clean, static_clean).ratio()
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