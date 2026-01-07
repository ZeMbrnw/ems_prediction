import pickle
import s3fs
import pandas as pd
import os

# S3 bucket path 
S3_BUCKET = 's3_bucket_path'
FEATURE_PREFIX = 'ems_prediction/04_features'
MODEL_PREFIX = 'ems_prediction/05_models'
RESULTS_PREFIX = 'ems_prediction/06_results'


def get_s3_uri(prefix, filename):
    return f's3://{S3_BUCKET}/{prefix}/{filename}'


def build_train_model_ems():
    import numpy as np
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error
    from xgboost import XGBRegressor

    s3 = s3fs.S3FileSystem()

    features_path = get_s3_uri(FEATURE_PREFIX, 'ems_features_2024.pkl')

    with s3.open(features_path, 'rb') as f:
        data = pickle.load(f)

    X = data['X']
    y = data['y']
    metadata = data['metadata']


    X_reshaped = X.reshape(X.shape[0], -1)


    # Split
    X_train, X_test, y_train, y_test, meta_train, meta_test = train_test_split(
        X_reshaped, y, metadata, test_size=0.2, random_state=42
    )

    # Build and train
    model = XGBRegressor(
        objective='reg:squarederror',
        n_estimators=500,
        learning_rate=0.05,
        random_state=42
    )

    model.fit(X_train, y_train)

    # Feature Importance
    feature_cols = ['hour', 'dow', 'month', 'borough', 'initial_severity_level_code', 'final_severity_level_code']
    feature_importances = dict(zip(feature_cols, model.feature_importances_))
    print("Feature Importances:", feature_importances)

    # Saving model
    model_path = get_s3_uri(MODEL_PREFIX, 'ems_xgb_model_2024.pkl')
    with s3.open(model_path, 'wb') as f:
        pickle.dump(model, f)
    print("XGBoost Model saved.")

    # CSV Power BI
    predictions = model.predict(X_test)

    # DF metadata, actual and prediction
    results_df = meta_test.copy()
    results_df['Actual_Response_Sec'] = y_test
    results_df['Predicted_Response_Sec'] = predictions

    # Save CSV for Power BI
    csv_path = get_s3_uri(RESULTS_PREFIX, 'ems_predictions_powerbi.csv')
    with s3.open(csv_path, 'w') as f:
        results_df.to_csv(f, index=False)

    mae = mean_absolute_error(y_test, predictions)
    print(f"Model trained. Test MAE: {mae:.2f} seconds.")
    print(f"SUCCESS: CSV saved to {csv_path} for Power BI.")