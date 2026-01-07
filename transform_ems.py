import pandas as pd
import s3fs
import pickle

CLEAN_FILE_PATH = 's3://bucket_path'
TRANSFORM_OUTPUT_PREFIX = 's3://bucket_path'


def transform_ems_data():
    # Local import to prevent crashing
    import numpy as np
    s3 = s3fs.S3FileSystem()

    if not s3.exists(CLEAN_FILE_PATH):
        raise FileNotFoundError(f"File not found at: {CLEAN_FILE_PATH}")

    with s3.open(CLEAN_FILE_PATH, 'rb') as f:
        df = pickle.load(f)

    print(f"Data loaded, starting transformation on {len(df):,} rows.")

    # Features
    df['incident_datetime'] = pd.to_datetime(df['incident_datetime'])
    df['hour'] = df['incident_datetime'].dt.hour
    df['dow'] = df['incident_datetime'].dt.dayofweek
    df['month'] = df['incident_datetime'].dt.month

    # Imputation
    NUM_COLS = ['incident_response_seconds_qy', 'incident_travel_tm_seconds_qy',
                'dispatch_response_seconds_qy', 'initial_severity_level_code',
                'final_severity_level_code']

    for col in NUM_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            median_val = df[col].median()
            df[col].fillna(median_val, inplace=True)

    CAT_COLS = ['borough', 'initial_call_type', 'final_call_type']
    for col in CAT_COLS:
        if col in df.columns:
            df[col].fillna('Unknown', inplace=True)

    # Outlier handling with clipping
    RESPONSE_COLS = ['incident_response_seconds_qy', 'incident_travel_tm_seconds_qy',
                     'dispatch_response_seconds_qy']
    for col in RESPONSE_COLS:
        if col in df.columns:
            lower = df[col].quantile(0.01)
            upper = df[col].quantile(0.99)
            df[col] = df[col].clip(lower, upper)

    # Save
    out_path = f'{TRANSFORM_OUTPUT_PREFIX}/ems_transformed_2024.pkl'
    print(f"Saving transformed data to {out_path}")
    with s3.open(out_path, 'wb') as f:
        pickle.dump(df, f)

    print(f"TRANSFORMED DATA SAVED → {out_path}")