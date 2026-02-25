# CSR Consolidation Tool - Technical Documentation

## Overview

This tool consolidates Philippine Red Cross Chapter Statistical Reports (Excel/Google Sheets) into standardized humanitarian reporting formats (DMS 5W or OpCen DSR).

**Stack:**
- Streamlit (web UI)
- Pandas (data processing)
- Google Drive/Sheets APIs (file access)
- Google BigQuery (optional data lake)
- Deployed on: Streamlit Cloud
- Repository: https://github.com/leerevis/prc-dms-csr-consolidation-tool

---

## Project Structure
```
├── app.py                       # Main Streamlit UI and orchestration
├── config.py                    # Static settings (column names, defaults)
├── utils.py                     # Helper functions (fuzzy matching, Google APIs)
├── processing.py                # Data processing pipeline (unpivot, clean, merge)
├── transformations.py           # Output schema transformation (DMS 5W / OpCen)
├── bigquery_utils.py            # BigQuery integration (optional)
├── requirements.txt             # Python dependencies
├── .streamlit/secrets.toml      # Google service account credentials (not in git)
├── data/
│   └── phl_adminareas_fixed.csv # PCode reference data (Philippine admin areas)
├── bigquery_setup/              # One-time BigQuery setup scripts
│   └──bigquery_setup.py
```

---

## How It Works (Data Flow)

### 1. Input Stage (app.py)

**Accepts three input methods:**
- Google Drive folder URL → downloads all .xlsx files via Drive API
- Individual Google Sheet URL → reads directly via Sheets API OR downloads as Excel
- Manual file upload → standard Streamlit file uploader

**Key code:** Lines 330-437 in app.py

### 2. Processing Stage (processing.py)

**Function:** process_single_file()

**Steps:**
1. Read Excel file starting from configurable header row (default: row 9)
2. Standardize column names (remove extra spaces)
3. Preserve original row numbers before unpivoting
4. Identify static vs activity columns using fuzzy matching
5. Unpivot activity columns (melt from wide to long format)
6. Add source tracking (filename + row number)
7. Clean data (remove nulls, zeros, blanks)
8. Fuzzy match activity names against mapping table (90% threshold)
9. Merge with mapping table to get sector, cost, beneficiary calculation data

**Output:** DataFrame with one row per activity instance

### 3. Transformation Stage (transformations.py)

**Two output formats:**

**DMS 5W Format** (transform_to_output_schema())
- Full humanitarian reporting standard
- Includes: organization, sector, location, beneficiaries, costs, validation status
- Calculates two beneficiary columns:
  - # of Beneficiaries Served = Count / Quantity (beneficiary units, e.g., families)
  - Number of Individuals = (Count / Quantity) × People_Per_Beneficiary (actual people)
- Special cost handling for cash grants: beneficiaries × cost (not count × cost)

**OpCen DSR Format** (transform_to_opcen_format())
- Simplified daily operations format
- Maps to OpCen column structure
- Same beneficiary calculations

**Validation statuses:**
- "For Validation" - Mapped successfully
- "Check Mapping" - Activity not in mapping table
- "Check Beneficiaries" - Missing calculation data
- "Check" - Both issues
- "Check - Duplicate Mapping" - Activity appears twice in mapping table

---

## Key Functions Explained

### Fuzzy Matching (utils.py)

is_static_column(column_name, static_columns, threshold=90)
- Uses thefuzz library (Levenshtein distance)
- Matches column names with 90% similarity
- Handles typos and spacing variations in source files

fuzzy_match_activity(activity_name, mapping_df, threshold=0.90)
- Matches activity names against mapping table
- Uses SequenceMatcher for similarity scoring
- Returns best match above threshold or None

### Beneficiary Calculations (transformations.py)

calculate_beneficiary_units(row)
- Returns: Count / Quantity
- Example: 10 food packs / 1 pack per family = 10 families

calculate_individuals(row)
- Returns: (Count / Quantity) × People_Per_Beneficiary
- Example: 10 families × 5 people per family = 50 people

**Special cases:**
- Cash/Pesos units → returns None (requires manual entry)
- Missing Quantity or People_Per_Beneficiary → returns None

### Cost Calculations (transformations.py)

calculate_total_cost(row)

**Logic:**
- For items: Total Cost = Activity Costing × Count
- For cash: Total Cost = # of Beneficiaries Served × Activity Costing
  - Uses beneficiary count (families/households), not raw count
  - Prevents incorrect calculations like 8000 pesos × 8000 = 64 million

### Source Tracking (processing.py)

Preserves original row numbers through unpivoting:

# BEFORE unpivoting
df['_original_row'] = df.index + header_row + 1

# After unpivot, assign to melted data
melted_df['Source_Row_Number'] = melted_df['_original_row']

Enables validators to trace back to source file and row.

### PCode Integration
# What are PCodes?
Philippine Standard Geographic Codes (PSGCodes or PCodes) are unique identifiers for administrative areas. The tool automatically assigns these codes by matching location names.

# How it works:

1. Reference file: phl_adminareas_fixed.csv contains pre-cleaned admin area names and their PCodes

2. Name cleaning: Uses get_clean_names() function to standardize location names:

- Removes "City of", "Brgy.", "Province of", etc.
- Converts Roman numerals to Arabic (Region I → Region 1)
- Handles diacritics (Ñ → N)
- Standardizes st./sta. to san/santa

3. Fuzzy matching: Uses thefuzz library (80% threshold) to match cleaned names

4. Hierarchical matching: Matches Province first (ADM2), then Municipality (ADM3) filtered by Province

# Output columns:

- Region - Region name (e.g., "Region I (Ilocos Region)")
- Prov_CODE - Province PCode (ADM2_new from reference file)
- Mun_Code - Municipality PCode (ADM3_new from reference file)

# Limitations:

- Only works for rows with Province data
- Requires 80%+ similarity for match
- Barangay-level codes (ADM4) not implemented
- OpCen format does not include PCodes (Region field left blank)
- Does not include the Negros Island Region (NIR) as a region

# Updating the PCode reference file:
Replace phl_adminareas_fixed.csv and redeploy. The file must have these columns:

- adm2_clean - Cleaned province names
- adm3_clean - Cleaned municipality names
- ADM2_new - Province PCodes
- ADM3_new - Municipality PCodes
- ADM1_EN - Region names

---

## Google APIs Integration

### Service Account Setup

Credentials stored in .streamlit/secrets.toml:

[gcp_service_account]
type = "service_account"
project_id = "prc-automatic-data-parsing"
private_key_id = "..."
private_key = "..."
client_email = "..."

### Permissions Required
- Google Drive API: Read-only access
- Google Sheets API: Read-only access
- BigQuery API: Data Editor + Job User (if using BigQuery)

### API Usage

**Drive API (utils.py):**
download_files_from_drive(folder_id, credentials_dict)
- Queries for .xlsx files in folder
- Downloads as BytesIO objects
- Returns list of MemoryFile objects

**Sheets API (utils.py):**
read_google_sheet(sheet_id, credentials_dict, sheet_name, header_row)
- Reads native Google Sheets
- Extracts headers from specified row
- Returns pandas DataFrame

---

## BigQuery Integration (Optional)

**Setup (bigquery_setup/bigquery_setup.py):**
- Creates table: prc_5w_lake.consolidated_activities
- Schema includes all DMS 5W columns + metadata
- One-time setup script (not in main app)

**Upload (bigquery_utils.py):**
upload_to_bigquery(df, credentials_dict, uploaded_by)
- Generates record hash for deduplication
- Compares against existing hashes
- Updates existing records or inserts new ones
- Currently commented out in production

---

## Common Issues & Solutions

### Issue: "Sheet not found" error
**Cause:** Sheet name doesn't match (case-sensitive)
**Fix:** User must specify correct sheet name in UI

### Issue: Thousands of blank rows in output
**Cause:** Wrong header row number
**Fix:** User adjusts header_row setting

### Issue: Native Google Sheets fail to read
**Cause:** Row indexing off by one in some sheets
**Fix:** Tool attempts fallback to Drive API download
**Known limitation:** Some native Google Sheets require manual download

### Issue: Duplicate mappings
**Cause:** Same activity appears multiple times in mapping table
**Fix:** Check for duplicates in mapping table, flag in validation status

---

## Configuration Files

### config.py

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

DEFAULT_SHEET_NAME = "Chapter Relief"
DEFAULT_HEADER_ROW = 9
DEFAULT_MAPPING_SHEET_ID = "..." # Google Sheet with activity mappings

---

## Deployment

**Platform:** Streamlit Cloud

**Secrets Management:**
- .streamlit/secrets.toml (local dev)
- Streamlit Cloud Secrets (production) - set via web UI

**Deployment Process:**
1. Push to GitHub main branch
2. Streamlit Cloud auto-deploys
3. No manual intervention needed

**URL:** https://prc-dms-csr-consolidation-tool.streamlit.app/

---

## Dependencies (requirements.txt)

streamlit
pandas
openpyxl
google-auth
google-auth-oauthlib
google-auth-httplib2
google-api-python-client
gspread
thefuzz
python-Levenshtein
google-cloud-bigquery

---

## Maintenance Tasks

### Updating the Activity Mapping Table

1. Open Google Sheet: [link in DEFAULT_MAPPING_SHEET_ID]
2. Add new activities with:
   - RawItemName (exact activity name from CSRs)
   - Sector, Sub-Sector, Activity (taxonomy)
   - Assistance? Materials/service (output name)
   - Unit, Quantity, COST, People_Per_Beneficiary
3. Remove duplicates (tool flags these automatically)
4. Tool reads mapping table on each run (no deployment needed)

### Adding New Static Columns

If CSR template changes to add new columns:

1. Update STATIC_COLUMNS in config.py
2. Update final_columns list in transformations.py
3. Map new column in transformation functions
4. Commit and push to GitHub

### Changing Output Formats

To modify DMS 5W or OpCen output schemas:

1. Edit transform_to_output_schema() or transform_to_opcen_format() in transformations.py
2. Update final_columns list
3. Test locally before deploying

---

## Known Limitations

1. **Native Google Sheets reading:** Some sheets fail due to row indexing issues (fallback to Excel download works)
2. **No PCodes:** Location codes not integrated (would require Philippine admin boundary database)
3. **Manual beneficiary entry required for:** Cash grants, water (liters), stations, some health activities
4. **No user authentication:** Anyone with link can access (by design, for field use)
5. **Validation is manual:** Tool flags issues but humans must verify

---

## Future Enhancements (Not Implemented)

- PCodes integration for automated location codes
- Multi-user validation workflow
- Admin UI for mapping table management
- Historical trend dashboards via BigQuery
- Automated duplicate detection in DMS sheet
- Real-time processing from live Google Sheets

---

## Support & Contact

**Original Developer:** Lee Reevis (IFRC Coordinator, Philippines deployment)
**Deployment Period:** November 2025 - February 2026
**Handover:** Contact DMS for technical issues

**For code issues:**
1. Check Streamlit Cloud logs (Manage App → Logs)
2. Review error messages in user interface
3. Test with sample CSR file to reproduce
4. Check Google API permissions/quotas
5. Review GitHub commit history for recent changes

---
