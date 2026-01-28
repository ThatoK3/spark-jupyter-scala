#!/usr/bin/env python3
""""
# Check clinical_measurements (default, 10 rows)
python check_postgres.py

# Check lab_results table
python check_postgres.py lab_results

# Check with 5 rows
python check_postgres.py clinical_measurements 5

# Check with 20 rows
python check_postgres.py lab_results 20
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sys

def get_postgres_engine():
    """Connect to PostgreSQL with fallback options"""
    connection_strings = [
        "postgresql://healthcare_user:HealthCare2024!@healthcare-postgres:5432/healthcare",
        "postgresql://healthcare_user:HealthCare2024!@localhost:5432/healthcare",
    ]
    
    for conn_str in connection_strings:
        try:
            engine = create_engine(conn_str, connect_args={'connect_timeout': 5})
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception:
            continue
    
    raise ConnectionError("Could not connect to PostgreSQL")

def display_horizontal(df, table_name, limit):
    """Display data in horizontal format with 10 columns max"""
    
    print(f"\n{'='*80}")
    print(f"📊 Table: {table_name} | Showing: {len(df)} of {limit} requested records")
    print(f"{'='*80}\n")
    
    if df.empty:
        print("❌ No data found")
        return
    
    # Display table stats
    print(f"Total Rows: {df.shape[0]}")
    print(f"Total Columns: {df.shape[1]}")
    print(f"Columns: {list(df.columns)}\n")
    
    # Set pandas display options for horizontal view
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.width', 200)
    pd.set_option('display.max_colwidth', 25)
    pd.set_option('display.colheader_justify', 'left')
    
    # Show first 10 columns only
    cols_to_show = df.columns[:10].tolist()
    df_display = df[cols_to_show]
    
    print(f"📝 First 10 columns:\n")
    print(df_display.head(20))
    
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
        print(f"{col:25}: {str(val)[:50]}")

def check_table(table_name='clinical_measurements', limit=10):
    """Main function to check PostgreSQL table"""
    
    try:
        engine = get_postgres_engine()
        
        # Check if table exists
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = :table
            """), {"table": table_name})
            
            if not result.fetchone():
                # List available tables
                tables = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)).fetchall()
                print(f"❌ Table '{table_name}' not found")
                print(f"Available tables: {[t[0] for t in tables]}")
                return
        
        print(f"Fetching {limit} rows from '{table_name}'...")
        
        # Fetch data
        query = text(f"SELECT * FROM {table_name} LIMIT {limit}")
        df = pd.read_sql(query, engine)
        
        display_horizontal(df, table_name, limit)
        
        # Show indexes
        print(f"\n{'='*80}")
        print("📇 Indexes:")
        print(f"{'='*80}")
        with engine.connect() as conn:
            indexes = conn.execute(text("""
                SELECT indexname, indexdef 
                FROM pg_indexes 
                WHERE tablename = :table AND schemaname = 'public'
            """), {"table": table_name}).fetchall()
            
            if indexes:
                for idx_name, idx_def in indexes:
                    print(f"  - {idx_name}")
            else:
                print("  No indexes found")
        
        # Show table size
        print(f"\n{'='*80}")
        print("💾 Table Size:")
        print(f"{'='*80}")
        with engine.connect() as conn:
            size = conn.execute(text("""
                SELECT pg_size_pretty(pg_total_relation_size(:table))
            """), {"table": table_name}).fetchone()[0]
            print(f"  Total size: {size}")
        
        engine.dispose()
        print(f"\n✅ Inspection complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Parse arguments
    table_name = sys.argv[1] if len(sys.argv) > 1 else 'clinical_measurements'
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    
    check_table(table_name, limit)