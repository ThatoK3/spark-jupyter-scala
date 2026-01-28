#!/usr/bin/env python3
import pandas as pd
import os
from pymongo import MongoClient
from datetime import datetime
import sys
import time

def load_mongodb():
    print("Loading data to MongoDB...")
    
    # Read CSV
    csv_file = 'synthetic_healthcare_300k_stroke_model.csv'
    if not os.path.exists(csv_file):
        print(f"❌ Error: CSV file '{csv_file}' not found")
        return False
    
    df = pd.read_csv(csv_file)
    print(f"✅ Loaded {len(df)} rows from CSV")
    
    # Connection string options - try each one
    connection_strings = [
        # Option 1: Using service name from Docker network
        "mongodb://healthcare_admin:HealthCare2024!@healthcare-mongodb:27017/healthcare?authSource=admin",
        # Option 2: Using localhost (if running outside Docker)
        "mongodb://healthcare_admin:HealthCare2024!@localhost:27017/healthcare?authSource=admin",
        # Option 3: Simpler format without database in URI
        "mongodb://healthcare_admin:HealthCare2024!@healthcare-mongodb:27017/?authSource=admin",
        # Option 4: With SSL disabled
        "mongodb://healthcare_admin:HealthCare2024!@healthcare-mongodb:27017/healthcare?authSource=admin&ssl=false",
    ]
    
    client = None
    last_error = None
    
    for i, mongo_uri in enumerate(connection_strings):
        try:
            print(f"\nTrying connection {i+1}/{len(connection_strings)}...")
            print(f"URI: {mongo_uri.replace('HealthCare2024!', '***')}")
            
            # Create client with shorter timeout for faster failure
            client = MongoClient(
                mongo_uri,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=5000
            )
            
            # Test connection
            client.admin.command('ping')
            print("✅ Connection successful!")
            
            # List databases to verify access
            dbs = client.list_database_names()
            print(f"Available databases: {dbs}")
            
            break  # Success, exit loop
            
        except Exception as e:
            print(f"❌ Connection {i+1} failed: {e}")
            last_error = e
            if client:
                client.close()
            client = None
            time.sleep(1)  # Wait before trying next
    
    if not client:
        print(f"\n❌ All connection attempts failed")
        print(f"Last error: {last_error}")
        return False
    
    try:
        db = client.healthcare
        
        # MongoDB Collections based on data source mapping
        # Patient Demographics Collection
        demographics_cols = ['patient_id', 'first_name', 'last_name', 'date_of_birth', 
                            'gender', 'blood_type', 'province', 'city', 'postal_code', 
                            'healthcare_region', 'insurance_type', 'insurance_id', 
                            'registration_date']
        
        demographics_data = df[demographics_cols].to_dict('records')
        
        # Convert dates to datetime objects
        print("\nConverting dates...")
        for record in demographics_data:
            try:
                record['date_of_birth'] = datetime.strptime(record['date_of_birth'], '%Y-%m-%d')
                record['registration_date'] = datetime.strptime(record['registration_date'], '%Y-%m-%d')
            except Exception as e:
                print(f"Warning: Date conversion failed for record: {e}")
        
        # Insert into MongoDB in batches
        print("\nInserting demographics data...")
        batch_size = 10000
        db.patient_demographics.delete_many({})  # Clear existing
        
        for i in range(0, len(demographics_data), batch_size):
            batch = demographics_data[i:i + batch_size]
            db.patient_demographics.insert_many(batch)
            print(f"  Inserted batch {i//batch_size + 1}/{(len(demographics_data)-1)//batch_size + 1}")
        
        # Create indexes
        print("Creating indexes...")
        db.patient_demographics.create_index("patient_id", unique=True)
        db.patient_demographics.create_index("province")
        db.patient_demographics.create_index("insurance_type")
        
        print(f"✅ MongoDB: Loaded {len(demographics_data)} patient demographics records")
        
        # Additional collections for other MongoDB data
        # Geographic data
        print("\nInserting geographic data...")
        geographic_data = df[['patient_id', 'province', 'city', 'postal_code', 'healthcare_region']].drop_duplicates()
        geographic_records = geographic_data.to_dict('records')
        
        db.geographic_data.delete_many({})
        db.geographic_data.insert_many(geographic_records)
        db.geographic_data.create_index("province")
        
        print(f"✅ MongoDB: Loaded {len(geographic_records)} geographic records")
        
        # Verify data
        print("\nVerification:")
        print(f"  patient_demographics: {db.patient_demographics.count_documents({})} documents")
        print(f"  geographic_data: {db.geographic_data.count_documents({})} documents")
        
        client.close()
        print("\n✅ MongoDB: Connection closed - Data loading completed successfully")
        return True
        
    except Exception as e:
        print(f"\n❌ Error during data loading: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = load_mongodb()
    sys.exit(0 if success else 1)