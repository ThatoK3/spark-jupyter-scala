#!/usr/bin/env python3
import pandas as pd
import io
import sys
from minio import Minio
from minio.error import S3Error

def get_minio_client():
    """Connect to MinIO with fallback options"""
    endpoints = [
        "healthcare-minio:9000",  # Docker network
        "localhost:9000",          # Host access
    ]
    
    for endpoint in endpoints:
        try:
            client = Minio(
                endpoint,
                access_key="healthcareadmin",
                secret_key="HealthCare2024!",
                secure=False
            )
            # Test connection
            client.list_buckets()
            print(f"✅ Connected to MinIO at {endpoint}")
            return client
        except Exception:
            continue
    
    raise ConnectionError("Could not connect to MinIO")

def get_bucket_files(client, bucket_name):
    """Get list of files in a bucket"""
    try:
        objects = client.list_objects(bucket_name, recursive=True)
        return [obj for obj in objects]
    except S3Error as e:
        print(f"❌ Error listing bucket {bucket_name}: {e}")
        return []

def read_parquet_from_minio(client, bucket_name, object_name):
    """Read Parquet file from MinIO into pandas DataFrame"""
    try:
        response = client.get_object(bucket_name, object_name)
        data = response.read()
        df = pd.read_parquet(io.BytesIO(data))
        return df
    except Exception as e:
        print(f"❌ Error reading {object_name}: {e}")
        return None
    finally:
        response.close()
        response.release_conn()

def display_horizontal(df, bucket_name, object_name, limit):
    """Display data in horizontal format with 10 columns max"""
    
    print(f"\n{'='*80}")
    print(f"📦 Bucket: {bucket_name}")
    print(f"📄 File: {object_name}")
    print(f"📊 Showing: {min(limit, len(df))} of {len(df)} total records")
    print(f"{'='*80}\n")
    
    if df.empty:
        print("❌ No data found in file")
        return
    
    # Display file stats
    print(f"Total Records: {df.shape[0]:,}")
    print(f"Total Columns: {df.shape[1]}")
    print(f"Columns: {list(df.columns)}\n")
    
    # Set pandas display options for horizontal view
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.width', 200)
    pd.set_option('display.max_colwidth', 25)
    pd.set_option('display.colheader_justify', 'left')
    
    # Show first 10 columns only
    cols_to_show = df.columns[:10].tolist()
    df_display = df[cols_to_show].head(limit)
    
    print(f"📝 First 10 columns (showing {len(df_display)} rows):\n")
    print(df_display.to_string())
    
    # If more than 10 columns, show remaining
    if len(df.columns) > 10:
        print(f"\n... ({len(df.columns) - 10} more columns: {list(df.columns[10:])})")
    
    print(f"\n{'='*80}")
    print("📈 Data Types:")
    print(f"{'='*80}")
    print(df.dtypes.head(10))
    
    print(f"\n{'='*80}")
    print("🔍 Sample Values (First Row):")
    print(f"{'='*80}")
    for col in cols_to_show:
        val = df.iloc[0][col] if not df.empty else 'N/A'
        print(f"{col:30}: {str(val)[:50]}")

def display_summary_stats(df, bucket_name, object_name):
    """Display summary statistics for numeric columns"""
    print(f"\n{'='*80}")
    print("📊 Summary Statistics (Numeric Columns):")
    print(f"{'='*80}")
    
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns[:5]  # First 5 numeric
    if len(numeric_cols) > 0:
        print(df[numeric_cols].describe().to_string())
    else:
        print("No numeric columns found")

def check_minio_bucket(bucket_name='healthcare-lifestyle', object_name=None, limit=10):
    """Main function to check MinIO bucket contents"""
    
    try:
        client = get_minio_client()
        
        # List all buckets
        print(f"\n{'='*80}")
        print("🪣 Available Buckets:")
        print(f"{'='*80}")
        buckets = client.list_buckets()
        for bucket in buckets:
            print(f"  - {bucket.name} (Created: {bucket.creation_date})")
        
        # Check if bucket exists
        if not client.bucket_exists(bucket_name):
            print(f"\n❌ Bucket '{bucket_name}' not found")
            return
        
        # If no object specified, list all objects in bucket
        if object_name is None:
            print(f"\n{'='*80}")
            print(f"📁 Files in bucket '{bucket_name}':")
            print(f"{'='*80}")
            files = get_bucket_files(client, bucket_name)
            
            if not files:
                print("  No files found")
                return
            
            for i, obj in enumerate(files, 1):
                size_mb = obj.size / (1024 * 1024)
                print(f"  {i}. {obj.object_name} ({size_mb:.2f} MB)")
            
            # Auto-select first Parquet file
            parquet_files = [f for f in files if f.object_name.endswith('.parquet')]
            if parquet_files:
                object_name = parquet_files[0].object_name
                print(f"\n👉 Auto-selected: {object_name}")
            else:
                print("\n❌ No Parquet files found in bucket")
                return
        
        # Read and display the file
        print(f"\nReading {object_name}...")
        df = read_parquet_from_minio(client, bucket_name, object_name)
        
        if df is not None:
            display_horizontal(df, bucket_name, object_name, limit)
            display_summary_stats(df, bucket_name, object_name)
            
            # Show unique values for categorical columns
            categorical_cols = df.select_dtypes(include=['object']).columns[:3]
            if len(categorical_cols) > 0:
                print(f"\n{'='*80}")
                print("🏷️  Unique Values (Categorical Columns):")
                print(f"{'='*80}")
                for col in categorical_cols:
                    unique_vals = df[col].unique()
                    print(f"\n{col}:")
                    print(f"  Unique: {len(unique_vals)}")
                    print(f"  Values: {list(unique_vals)[:5]}{'...' if len(unique_vals) > 5 else ''}")
        
        print(f"\n✅ Inspection complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Parse arguments
    bucket_name = sys.argv[1] if len(sys.argv) > 1 else 'healthcare-lifestyle'
    object_name = sys.argv[2] if len(sys.argv) > 2 else None  # Optional specific file
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    
    check_minio_bucket(bucket_name, object_name, limit)