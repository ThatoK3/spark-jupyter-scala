#!/usr/bin/env python3
"""
PostgreSQL Data Loader for Healthcare Database
Loads clinical measurements and lab results from CSV
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sys

def load_postgres():
    print("Loading data to PostgreSQL...")
    
    # Read CSV
    csv_file = 'synthetic_healthcare_300k_stroke_model.csv'
    if not os.path.exists(csv_file):
        print(f"❌ Error: CSV file '{csv_file}' not found")
        return False
    
    df = pd.read_csv(csv_file)
    print(f"✅ Loaded {len(df)} rows from CSV")
    
    # PostgreSQL connection URI (directly in script)
    postgres_uri = "postgresql://healthcare_user:HealthCare2024!@healthcare-postgres:5432/healthcare"
    # Alternative: localhost if running outside Docker
    # postgres_uri = "postgresql://healthcare_user:HealthCare2024!@localhost:5432/healthcare"
    
    print(f"Connecting to PostgreSQL...")
    engine = create_engine(postgres_uri)
    
    try:
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ Connected to: {version.split(',')[0]}")
        
        # PostgreSQL Tables based on data source mapping
        # ============================================
        # Table 1: Clinical Measurements
        # ============================================
        print("\nLoading clinical_measurements...")
        
        clinical_cols = ['patient_id', 'visit_date', 'avg_glucose_level', 'bmi', 'heart_rate',
                        'temperature', 'oxygen_saturation', 'blood_pressure_systolic',
                        'blood_pressure_diastolic', 'cholesterol_level']
        
        clinical_data = df[clinical_cols].copy()
        
        # Convert date column
        clinical_data['visit_date'] = pd.to_datetime(clinical_data['visit_date'])
        
        # Create table and load data
        clinical_data.to_sql('clinical_measurements', engine, if_exists='replace', 
                            index=False, method='multi', chunksize=10000)
        
        # Create indexes
        with engine.connect() as conn:
            conn.execute(text("CREATE INDEX idx_clinical_patient_id ON clinical_measurements(patient_id)"))
            conn.execute(text("CREATE INDEX idx_clinical_visit_date ON clinical_measurements(visit_date)"))
            conn.execute(text("CREATE INDEX idx_clinical_glucose ON clinical_measurements(avg_glucose_level)"))
            conn.commit()
        
        print(f"✅ Loaded {len(clinical_data):,} clinical measurements records")
        
        # ============================================
        # Table 2: Lab Results
        # ============================================
        print("\nLoading lab_results...")
        
        lab_cols = ['patient_id', 'visit_date', 'cholesterol_level', 'avg_glucose_level',
                    'blood_pressure_systolic', 'blood_pressure_diastolic']
        
        lab_data = df[lab_cols].copy()
        lab_data['visit_date'] = pd.to_datetime(lab_data['visit_date'])
        lab_data['lab_test_id'] = [f"LAB{i:08d}" for i in range(1, len(lab_data) + 1)]
        
        # Reorder columns to match schema
        lab_data = lab_data[['patient_id', 'visit_date', 'lab_test_id', 'cholesterol_level',
                            'avg_glucose_level', 'blood_pressure_systolic', 'blood_pressure_diastolic']]
        
        lab_data.to_sql('lab_results', engine, if_exists='replace', index=False,
                       method='multi', chunksize=10000)
        
        with engine.connect() as conn:
            conn.execute(text("CREATE INDEX idx_lab_patient_id ON lab_results(patient_id)"))
            conn.execute(text("CREATE INDEX idx_lab_test_id ON lab_results(lab_test_id)"))
            conn.execute(text("CREATE INDEX idx_lab_visit_date ON lab_results(visit_date)"))
            conn.commit()
        
        print(f"✅ Loaded {len(lab_data):,} lab results records")
        
        # ============================================
        # Verification
        # ============================================
        print("\n" + "="*50)
        print("Verification:")
        print("="*50)
        
        with engine.connect() as conn:
            # Count records
            clinical_count = conn.execute(text("SELECT COUNT(*) FROM clinical_measurements")).fetchone()[0]
            lab_count = conn.execute(text("SELECT COUNT(*) FROM lab_results")).fetchone()[0]
            
            print(f"  clinical_measurements: {clinical_count:,} records")
            print(f"  lab_results: {lab_count:,} records")
            
            # Show table structures
            print("\nTable Structures:")
            tables = ['clinical_measurements', 'lab_results']
            for table in tables:
                print(f"\n  {table}:")
                columns = conn.execute(text(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                """)).fetchall()
                for col, dtype in columns:
                    print(f"    - {col}: {dtype}")
        
        engine.dispose()
        print(f"\n✅ PostgreSQL: Data loading completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import os
    success = load_postgres()
    sys.exit(0 if success else 1)