import pandas as pd
import requests
import s3fs
import pickle

DIR = 's3://bucket_path/'
API_URL = 'API_call'

def ingest_2024_data():
    url = (
        f"{API_URL}?"
        "$where=incident_datetime >= '2024-01-01T00:00:00.000' "
        "AND incident_datetime < '2025-01-01T00:00:00.000'"
        "&$limit=5000000"
    )
    response = requests.get(url, timeout=600)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)
    print(f"Got {len(df):,} rows in {response.elapsed.total_seconds():.1f}s")

    s3 = s3fs.S3FileSystem()
    path = f"{DIR}/ems_2024_full.pkl"
    with s3.open(path, 'wb') as f:
        f.write(pickle.dumps(df))
    print(f"FULL 2024 SAVED → {path}")