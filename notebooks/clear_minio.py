#!/usr/bin/env python3
"""
MinIO Data Cleaner
==================
Clears all data from MinIO buckets in preparation for fresh data load.
"""

import sys
from minio import Minio
from minio.error import S3Error

def clear_minio():
    print("=" * 60)
    print("MINIO DATA CLEANER")
    print("=" * 60)
    
    MINIO_ENDPOINT = "healthcare-minio:9000"
    MINIO_ACCESS_KEY = "healthcareadmin"
    MINIO_SECRET_KEY = "HealthCare2024!"
    
    print(f"\nConnecting to MinIO at {MINIO_ENDPOINT}...")
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        client.list_buckets()
        print("Connected successfully")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return False
    
    buckets = ['healthcare-lifestyle', 'healthcare-batch', 'healthcare-imaging']
    
    for bucket in buckets:
        print(f"\nProcessing bucket: {bucket}")
        try:
            if client.bucket_exists(bucket):
                # List all objects
                objects = list(client.list_objects(bucket, recursive=True))
                
                if objects:
                    print(f"  Found {len(objects)} objects")
                    
                    # Delete all objects
                    for obj in objects:
                        client.remove_object(bucket, obj.object_name)
                        print(f"    Deleted: {obj.object_name}")
                    
                    print(f"  Cleared all objects from {bucket}")
                else:
                    print(f"  Bucket is empty")
                    
            else:
                print(f"  Bucket does not exist")
                
        except S3Error as e:
            print(f"  Error processing bucket: {e}")
    
    print("\n" + "=" * 60)
    print("MINIO CLEANUP COMPLETED")
    print("=" * 60)
    return True

if __name__ == "__main__":
    success = clear_minio()
    sys.exit(0 if success else 1)