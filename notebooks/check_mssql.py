#!/usr/bin/env python3
"""
MS SQL Server Data Inspector - Horizontal Display (10 columns)
Usage: python check_mssql.py [table_name] [limit]
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sys

def get_mssql_engine():
    """Connect to MS SQL Server with fallback options"""
    connection_strings = [
        "mssql+pymssql://sa:HealthCare2024!@healthcare-mssql:1433/healthcare",
        "mssql+pymssql://sa:HealthCare2024!@localhost:1433/healthcare",
    ]
    
    for conn_str in connection_strings:
        try:
            engine = create_engine(conn_str, connect_args={'timeout': 5})
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception:
            continue
    
    raise ConnectionError("Could not connect to MS SQL Server")

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

def check_table(table_name='hospital_administration', limit=10):
    """Main function to check MS SQL Server table"""
    
    try:
        engine = get_mssql_engine()
        
        # Check if table exists
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = :table
            """), {"table": table_name})
            
            if not result.fetchone():
                # List available tables
                tables = conn.execute(text("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                """)).fetchall()
                print(f"❌ Table '{table_name}' not found")
                print(f"Available tables: {[t[0] for t in tables]}")
                return
        
        print(f"Fetching {limit} rows from '{table_name}'...")
        
        # Fetch data
        query = text(f"SELECT TOP {limit} * FROM {table_name}")
        df = pd.read_sql(query, engine)
        
        display_horizontal(df, table_name, limit)
        
        # Show indexes
        print(f"\n{'='*80}")
        print("📇 Indexes:")
        print(f"{'='*80}")
        with engine.connect() as conn:
            indexes = conn.execute(text("""
                SELECT i.name AS index_name, 
                       STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                WHERE t.name = :table AND i.type > 0
                GROUP BY i.name
            """), {"table": table_name}).fetchall()
            
            if indexes:
                for idx_name, columns in indexes:
                    print(f"  - {idx_name}: ({columns})")
            else:
                print("  No indexes found")
        
        # Show table size
        print(f"\n{'='*80}")
        print("💾 Table Size:")
        print(f"{'='*80}")
        with engine.connect() as conn:
            size = conn.execute(text("""
                SELECT CONCAT(
                    CAST(SUM(a.total_pages) * 8 / 1024 AS VARCHAR(20)), ' MB'
                ) AS size
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                WHERE t.name = :table
                GROUP BY t.name
            """), {"table": table_name}).fetchone()
            
            if size:
                print(f"  Total size: {size[0]}")
            else:
                print(f"  Size: < 1 MB")
        
        engine.dispose()
        print(f"\n✅ Inspection complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Parse arguments
    table_name = sys.argv[1] if len(sys.argv) > 1 else 'hospital_administration'
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    
    check_table(table_name, limit)