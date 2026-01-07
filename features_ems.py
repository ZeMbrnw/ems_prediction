import pandas as pd
import pickle
import s3fs
import os

S3_BUCKET = 's3_bucket_path'
TRANSFORM_PREFIX = 'ems_prediction/ems_transformed'
FEATURE_PREFIX = 'ems_prediction/04_features'


def get_s3_uri(prefix, filename):
    return f's3://{S3_BUCKET}/{prefix}/{filename}'


def extract_features_ems():
    # Local import to avoid crashing
    import numpy as np

    s3 = s3fs.S3FileSystem()

    # Loading transformed data
    input_path = get_s3_uri(TRANSFORM_PREFIX, 'ems_transformed_2024.pkl')
    print(f"Loading transformed data from {input_path}")
    with s3.open(input_path, 'rb') as f:
        df = pickle.load(f)

    # Metadata PowerBI
    metadata_cols = ['incident_datetime', 'borough', 'zipcode', 'initial_call_type', 'dow', 'month']
    available_meta = [c for c in metadata_cols if c in df.columns]
    metadata = df[available_meta].copy()

    # Categorical features
    cat_cols = ['borough', 'initial_call_type', 'final_call_type']
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].astype('category').cat.codes

    # Features and targets
    feature_cols = ['hour', 'dow', 'month', 'borough', 'initial_severity_level_code', 'final_severity_level_code']

    X = df[feature_cols].values.astype(np.float32)
    y = df['incident_response_seconds_qy'].values.astype(np.float32)

    # 2D format for XGBoost
    data_bundle = {
        'X': X,
        'y': y,
        'metadata': metadata
    }

    out_path = get_s3_uri(FEATURE_PREFIX, 'ems_features_2024.pkl')

    print(f"Saving features and metadata to {out_path}")
    with s3.open(out_path, 'wb') as f:
        pickle.dump(data_bundle, f)