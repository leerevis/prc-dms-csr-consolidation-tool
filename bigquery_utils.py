from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import pandas as pd
import hashlib
from datetime import datetime

def upload_to_bigquery(df, credentials_dict, uploaded_by="Unknown"):
    """
    Upload processed data to BigQuery with deduplication
    
    Args:
        df: DataFrame with processed DMS 5W data
        credentials_dict: Service account credentials
        uploaded_by: Username or identifier of uploader
    
    Returns:
        Number of rows inserted/updated
    """
    
    # Create BigQuery client
    credentials = Credentials.from_service_account_info(credentials_dict)
    client = bigquery.Client(
        credentials=credentials,
        project='prc-automatic-data-parsing'
    )
    
    table_id = "prc-automatic-data-parsing.prc_5w_lake.consolidated_activities"
    
    # Prepare the dataframe for BigQuery
    bq_df = prepare_for_bigquery(df, uploaded_by)
    
    # Get existing hashes from BigQuery to check for duplicates
    existing_hashes = get_existing_hashes(client, table_id)
    
    # Split into new records and updates
    new_records = bq_df[~bq_df['record_hash'].isin(existing_hashes)]
    update_records = bq_df[bq_df['record_hash'].isin(existing_hashes)]
    
    rows_affected = 0
    
    # Insert new records
    if len(new_records) > 0:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )
        job = client.load_table_from_dataframe(new_records, table_id, job_config=job_config)
        job.result()  # Wait for completion
        rows_affected += len(new_records)
    
    # Update existing records
    if len(update_records) > 0:
        # For updates, we'll delete old records and insert new ones
        # (BigQuery doesn't have native UPDATE from DataFrame)
        hashes_to_update = update_records['record_hash'].tolist()
        
        # Delete old records
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE record_hash IN UNNEST(@hashes)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("hashes", "STRING", hashes_to_update)
            ]
        )
        delete_job = client.query(delete_query, job_config=job_config)
        delete_job.result()
        
        # Insert updated records
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )
        job = client.load_table_from_dataframe(update_records, table_id, job_config=job_config)
        job.result()
        rows_affected += len(update_records)
    
    return rows_affected, len(new_records), len(update_records)


def prepare_for_bigquery(df, uploaded_by):
    """
    Prepare DataFrame for BigQuery upload:
    - Rename columns (remove spaces, special chars)
    - Add metadata columns
    - Generate record hashes
    """
    
    bq_df = df.copy()
    
    # Rename columns to BigQuery-friendly names (no spaces, special chars)
    column_mapping = {
        "Organisation": "Organisation",
        "Implementing Partner/Supported By": "Implementing_Partner",
        "Phase": "Phase",
        "Sector/Cluster": "Sector_Cluster",
        "Sub Sector": "Sub_Sector",
        "Region": "Region",
        "Province": "Province",
        "Prov_CODE": "Prov_CODE",
        "Municipality/City": "Municipality_City",
        "Mun_Code": "Mun_Code",
        "Barangay": "Barangay",
        "Place Name": "Place_Name",
        "Activity": "Activity",
        "Materials/Service Provided": "Materials_Service_Provided",
        "DSR Intervention Team": "DSR_Intervention_Team",
        "Count": "Count",
        "Unit": "Unit",
        "# of Beneficiaries Served": "Num_Beneficiaries_Served",
        "Primary Beneficiary Served": "Primary_Beneficiary_Served",
        "DSR Unit": "DSR_Unit",
        "Status": "Status",
        "Start Date": "Start_Date",
        "End Date": "End_Date",
        "Source": "Source",
        "Signature": "Signature",
        "Weather System": "Weather_System",
        "Remarks": "Remarks",
        "Date Modified": "Date_Modified",
        "ACTIVITY COSTING": "Activity_Costing",
        "Total Cost": "Total_Cost",
        "Month": "Month",
        "Validation Status": "Validation_Status"
    }
    
    bq_df = bq_df.rename(columns=column_mapping)
    
    # Add metadata columns
    current_time = datetime.utcnow()
    bq_df['upload_timestamp'] = current_time
    bq_df['uploaded_by'] = uploaded_by
    bq_df['source_filename'] = "consolidated"  # Will be updated per file
    bq_df['last_updated'] = current_time
    
    # Generate record hash for deduplication
    bq_df['record_hash'] = bq_df.apply(generate_record_hash, axis=1)
    
    return bq_df


def generate_record_hash(row):
    """
    Generate a unique hash for a record based on key fields
    Hash includes: Start_Date, Province, Municipality, Barangay, Activity, Materials, Count
    """
    
    hash_fields = [
        str(row.get('Start_Date', '')),
        str(row.get('Province', '')),
        str(row.get('Municipality_City', '')),
        str(row.get('Barangay', '')),
        str(row.get('Activity', '')),
        str(row.get('Materials_Service_Provided', '')),
        str(row.get('Count', ''))
    ]
    
    hash_string = '|'.join(hash_fields)
    return hashlib.sha256(hash_string.encode()).hexdigest()


def get_existing_hashes(client, table_id):
    """
    Query BigQuery to get all existing record hashes
    """
    
    query = f"""
    SELECT record_hash
    FROM `{table_id}`
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        return set(row.record_hash for row in results)
    except:
        # Table might be empty
        return set()