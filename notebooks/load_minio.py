#!/usr/bin/env python3
"""
MinIO Data Loader for Healthcare Analytics
==========================================
Loads data from CSV into MinIO buckets according to the data architecture:

- healthcare-lifestyle: lifestyle factors (smoking, exercise, diet, etc.)
- healthcare-batch: stroke outcomes, conditions (hypertension, heart_disease, diabetes), medical history
- healthcare-imaging: reserved for future medical imaging data

All columns are explicitly defined to match the data architecture.
"""

import pandas as pd
import random
import io
import os
import sys
from datetime import datetime
from minio import Minio
from minio.error import S3Error

def load_minio():
    print("=" * 60)
    print("MINIO DATA LOADER (Docker Network)")
    print("=" * 60)
    
    MINIO_ENDPOINT = "healthcare-minio:9000"
    MINIO_ACCESS_KEY = "healthcareadmin"
    MINIO_SECRET_KEY = "HealthCare2024!"
    
    # Read CSV
    csv_file = 'synthetic_healthcare_300k_stroke_model.csv'
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found")
        return False
    
    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df):,} rows from CSV")
    
    # Connect to MinIO
    print(f"\nConnecting to MinIO at {MINIO_ENDPOINT}...")
    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        buckets = minio_client.list_buckets()
        print(f"Connected to MinIO successfully")
        print(f"Existing buckets: {[b.name for b in buckets]}")
    except Exception as e:
        print(f"Failed to connect to MinIO: {e}")
        return False
    
    # Create buckets
    print("\nCreating buckets...")
    buckets = ['healthcare-lifestyle', 'healthcare-batch', 'healthcare-imaging']
    for bucket in buckets:
        try:
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)
                print(f"  Created bucket: {bucket}")
            else:
                print(f"  Bucket already exists: {bucket}")
        except S3Error as e:
            print(f"  Bucket {bucket} error: {e}")
    
    # ========================================================================
    # BUCKET 1: healthcare-lifestyle
    # Columns: patient_id, smoking_status, alcohol_consumption, exercise_frequency,
    #          diet_type, stress_level, sleep_hours, height_cm, weight_kg
    # ========================================================================
    print("\nUploading lifestyle data...")
    
    lifestyle_cols = [
        'patient_id', 'smoking_status', 'alcohol_consumption', 'exercise_frequency',
        'diet_type', 'stress_level', 'sleep_hours', 'height_cm', 'weight_kg'
    ]
    
    # Verify all columns exist
    missing_cols = [col for col in lifestyle_cols if col not in df.columns]
    if missing_cols:
        print(f"  Warning: Missing columns in CSV: {missing_cols}")
        print(f"  Available columns: {list(df.columns)}")
    
    lifestyle_data = df[lifestyle_cols].copy()
    
    parquet_buffer = io.BytesIO()
    lifestyle_data.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
    parquet_buffer.seek(0)
    
    minio_client.put_object(
        'healthcare-lifestyle',
        'lifestyle_data.parquet',
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    
    print(f"  Uploaded lifestyle_data.parquet ({len(lifestyle_data):,} records, {len(lifestyle_cols)} columns)")
    
    # ========================================================================
    # BUCKET 2: healthcare-batch
    # FILE 1: stroke_outcomes.parquet
    # Columns: patient_id, hypertension, heart_disease, diabetes, stroke_occurred,
    #          stroke_probability, stroke_severity, stroke_date, recovery_status
    # ========================================================================
    print("\nUploading stroke outcomes and conditions data...")
    
    stroke_outcome_cols = [
        'patient_id', 'hypertension', 'heart_disease', 'diabetes',
        'stroke_occurred', 'stroke_probability', 'stroke_severity',
        'stroke_date', 'recovery_status'
    ]
    
    # Check which columns exist
    available_stroke_cols = [col for col in stroke_outcome_cols if col in df.columns]
    missing_stroke_cols = [col for col in stroke_outcome_cols if col not in df.columns]
    
    if missing_stroke_cols:
        print(f"  Warning: Missing columns in CSV: {missing_stroke_cols}")
        print(f"  Proceeding with available columns: {available_stroke_cols}")
    
    stroke_data = df[available_stroke_cols].copy()
    
    parquet_buffer = io.BytesIO()
    stroke_data.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
    parquet_buffer.seek(0)
    
    minio_client.put_object(
        'healthcare-batch',
        'stroke_outcomes.parquet',
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    
    print(f"  Uploaded stroke_outcomes.parquet ({len(stroke_data):,} records, {len(available_stroke_cols)} columns)")
    
    # ========================================================================
    # BUCKET 2: healthcare-batch
    # FILE 2: medical_history.parquet
    # Columns: patient_id, registration_date, visit_date, family_history,
    #          previous_conditions, medication_history
    # ========================================================================
    print("\nUploading medical history data...")
    
    # Start with existing columns
    history_data = df[['patient_id', 'registration_date', 'visit_date']].copy()
    
    # Generate synthetic history data
    history_data['family_history'] = [
        random.choice(['None', 'Heart Disease', 'Diabetes', 'Stroke', 'Cancer']) 
        for _ in range(len(history_data))
    ]
    history_data['previous_conditions'] = [
        random.choice(['None', 'Hypertension', 'Diabetes', 'Previous Stroke', 'Asthma']) 
        for _ in range(len(history_data))
    ]
    history_data['medication_history'] = [
        random.choice(['None', 'Blood Pressure', 'Diabetes', 'Cholesterol', 'Anticoagulants']) 
        for _ in range(len(history_data))
    ]
    
    parquet_buffer = io.BytesIO()
    history_data.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
    parquet_buffer.seek(0)
    
    minio_client.put_object(
        'healthcare-batch',
        'medical_history.parquet',
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    
    print(f"  Uploaded medical_history.parquet ({len(history_data):,} records, {len(history_data.columns)} columns)")
    
    # ========================================================================
    # VERIFICATION
    # ========================================================================
    print("\nVerifying uploads...")
    
    try:
        # healthcare-lifestyle
        lifestyle_objects = list(minio_client.list_objects('healthcare-lifestyle'))
        print(f"\n  healthcare-lifestyle bucket:")
        for obj in lifestyle_objects:
            size_mb = obj.size / (1024 * 1024)
            print(f"    - {obj.object_name} ({size_mb:.2f} MB)")
        
        # healthcare-batch
        batch_objects = list(minio_client.list_objects('healthcare-batch'))
        print(f"\n  healthcare-batch bucket:")
        for obj in batch_objects:
            size_mb = obj.size / (1024 * 1024)
            print(f"    - {obj.object_name} ({size_mb:.2f} MB)")
        
        # Test read back
        print("\n  Testing data retrieval...")
        response = minio_client.get_object('healthcare-lifestyle', 'lifestyle_data.parquet')
        test_df = pd.read_parquet(io.BytesIO(response.read()))
        print(f"    Successfully read back {len(test_df):,} rows from lifestyle_data.parquet")
        
        response = minio_client.get_object('healthcare-batch', 'stroke_outcomes.parquet')
        test_df = pd.read_parquet(io.BytesIO(response.read()))
        print(f"    Successfully read back {len(test_df):,} rows from stroke_outcomes.parquet")
        print(f"    Columns in stroke_outcomes: {list(test_df.columns)}")
        
    except Exception as e:
        print(f"  Verification warning: {e}")
    
    print("\n" + "=" * 60)
    print("MINIO DATA LOADING COMPLETED")
    print("=" * 60)
    print("\nData Architecture Summary:")
    print("\n  healthcare-lifestyle/lifestyle_data.parquet")
    print("    Columns: patient_id, smoking_status, alcohol_consumption,")
    print("             exercise_frequency, diet_type, stress_level,")
    print("             sleep_hours, height_cm, weight_kg")
    print("\n  healthcare-batch/stroke_outcomes.parquet")
    print("    Columns: patient_id, hypertension, heart_disease, diabetes,")
    print("             stroke_occurred, stroke_probability, stroke_severity,")
    print("             stroke_date, recovery_status")
    print("\n  healthcare-batch/medical_history.parquet")
    print("    Columns: patient_id, registration_date, visit_date,")
    print("             family_history, previous_conditions, medication_history")
    
    return True

if __name__ == "__main__":
    success = load_minio()
    sys.exit(0 if success else 1)