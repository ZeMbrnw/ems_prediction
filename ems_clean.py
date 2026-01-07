import pandas as pd
import s3fs
import pickle

S3_BASE_DIR = 's3://bucket_path'

YN_COLS = [
    'valid_dispatch_rspns_time_indc','valid_incident_rspns_time_indc','held_indicator',
    'reopen_indicator','special_event_indicator','standby_indicator','transfer_indicator'
]

NUM_COLS = [
    'initial_severity_level_code','final_severity_level_code','dispatch_response_seconds_qy',
    'incident_response_seconds_qy','incident_travel_tm_seconds_qy','incident_disposition_code',
    'zipcode','policeprecinct','citycouncildistrict','communitydistrict',
    'communityschooldistrict','congressionaldistrict'
]

def clean_ems_data():
    s3 = s3fs.S3FileSystem()
    raw_path = f"{S3_BASE_DIR}/ems_2024_full.pkl"

    # Load raw file
    with s3.open(raw_path, 'rb') as f:
        df = pickle.load(f)

    print(f"Loaded {len(df):,} rows from {raw_path} – starting cleaning")

    # Parse timestamps
    df['incident_datetime'] = pd.to_datetime(df['incident_datetime'], errors='coerce')
    df = df.dropna(subset=['incident_datetime'])

    # Filter 2024
    df = df[df['incident_datetime'].dt.year == 2024].copy()
    print(f"After year filter → {len(df):,} rows")

    # Fix typos
    for col in ['initial_call_type', 'final_call_type']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('UNKNOW', 'UNKNOWN', case=False)

    # Convert Y/N to boolean
    for c in YN_COLS:
        if c in df.columns:
            df[c] = df[c].map({'Y': True, 'N': False})
            df[c] = df[c].fillna(False)

    # Convert numeric columns
    for c in NUM_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    # Fill categorical columns with 'Unknown'
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].fillna('Unknown')

    print(f"Cleaning done → {len(df):,} final rows")

    # Save cleaned file
    clean_path = f"{S3_BASE_DIR}/ems_cleaned_2024.pkl"
    with s3.open(clean_path, 'wb') as f:
        pickle.dump(df, f)
    print(f"CLEAN DATA SAVED → {clean_path}")