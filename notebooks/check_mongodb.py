import pandas as pd
from pymongo import MongoClient
from bson import ObjectId
import sys

def get_mongodb_client():
    """Connect to MongoDB with multiple fallback options"""
    connection_strings = [
        "mongodb://healthcare_admin:HealthCare2024!@healthcare-mongodb:27017/healthcare?authSource=admin",
        "mongodb://healthcare_admin:HealthCare2024!@localhost:27017/healthcare?authSource=admin",
    ]
    
    for mongo_uri in connection_strings:
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            return client
        except Exception:
            continue
    
    raise ConnectionError("Could not connect to MongoDB")

def flatten_document(doc, parent_key='', sep='_'):
    """Flatten nested MongoDB documents"""
    items = []
    for k, v in doc.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if isinstance(v, dict):
            items.extend(flatten_document(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, str(v)[:50]))
        elif isinstance(v, ObjectId):
            items.append((new_key, str(v)))
        elif isinstance(v, datetime):
            items.append((new_key, v.strftime('%Y-%m-%d')))
        else:
            items.append((new_key, v))
    
    return dict(items)

def display_horizontal(df, collection_name, limit):
    """Display data in horizontal format with 10 columns max"""
    
    print(f"\n{'='*80}")
    print(f"📊 Collection: {collection_name} | Showing: {len(df)} of {limit} requested records")
    print(f"{'='*80}\n")
    
    if df.empty:
        print("❌ No data found")
        return
    
    # Display collection stats
    print(f"Total Documents: {df.shape[0]}")
    print(f"Total Fields: {df.shape[1]}")
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

def check_collection(collection_name='patient_demographics', limit=10):
    """Main function to check MongoDB collection"""
    
    try:
        client = get_mongodb_client()
        db = client.healthcare
        
        # Check if collection exists
        if collection_name not in db.list_collection_names():
            print(f"❌ Collection '{collection_name}' not found")
            print(f"Available collections: {db.list_collection_names()}")
            return
        
        collection = db[collection_name]
        
        # Fetch data
        print(f"Fetching {limit} documents from '{collection_name}'...")
        cursor = collection.find().limit(limit)
        
        # Flatten documents and convert to DataFrame
        documents = []
        for doc in cursor:
            flat_doc = flatten_document(doc)
            documents.append(flat_doc)
        
        df = pd.DataFrame(documents)
        
        # Remove MongoDB _id if present (usually first column)
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])
        
        display_horizontal(df, collection_name, limit)
        
        # Show index info
        print(f"\n{'='*80}")
        print("📇 Indexes:")
        print(f"{'='*80}")
        for idx in collection.list_indexes():
            print(f"  - {idx['name']}: {idx['key']}")
        
        client.close()
        print(f"\n✅ Inspection complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Parse arguments
    collection_name = sys.argv[1] if len(sys.argv) > 1 else 'patient_demographics'
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    
    from datetime import datetime  # Add this import
    
    check_collection(collection_name, limit)