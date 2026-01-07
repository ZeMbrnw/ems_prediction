from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import pandas as pd
import os

USER = "user"
PASSWORD = "password"
ENDPOINT = "db_endpoint"
DB_NAME = "db_username"  

S3_RESULTS_PATH = 'bucket_path'
TABLE_NAME = 'ems_predictions' 


def load_data():
    from s3fs.core import S3FileSystem
    import pymysql  
    print(f"Starting database load for table: {TABLE_NAME}")

    s3 = S3FileSystem()

    print(f"Loading CSV data from S3: {S3_RESULTS_PATH}")
    try:
        with s3.open(S3_RESULTS_PATH, 'r') as f:
            df = pd.read_csv(f)
    except FileNotFoundError:
        print(
            f"Error: The required file {S3_RESULTS_PATH} was not found. Has the build_train_model_2024 task run successfully?")
        raise

    print(f"Loaded {len(df)} rows for database insertion.")


    try:
        engine_root = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{ENDPOINT}")

        with engine_root.connect() as connection:
            result = connection.execute(f"SHOW DATABASES LIKE '{DB_NAME}';")
            db_exists = result.fetchone()

            if not db_exists:
                print(f"Database {DB_NAME} not found. Creating...")
                connection.execute(f"CREATE DATABASE {DB_NAME}")
            else:
                print(f"Database {DB_NAME} already exists.")

    except OperationalError as e:
        print(f"FATAL ERROR: Could not connect to MySQL server. Check credentials or endpoint. Error: {e}")
        raise

    engine_db = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{ENDPOINT}/{DB_NAME}")

    print(f"Inserting data into table: {TABLE_NAME}")
    try:
        # index=False ensures the DataFrame index is not added as a column
        df.to_sql(TABLE_NAME, con=engine_db, if_exists='replace', index=False, chunksize=1000)
        print(f"SUCCESS: {len(df)} records pushed to {DB_NAME}.{TABLE_NAME}")
    except Exception as e:
        print(f"Error during data insertion: {e}")
        raise