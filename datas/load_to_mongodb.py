import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import csv

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')  # Adjust connection string as needed
db = client['stock_data']  # Database name
collection = db['prices']  # Collection name

def tsv_to_mongodb(tsv_file_path):
    """
    Read TSV file and insert data into MongoDB
    """
    try:
        # Read TSV file (tab-separated)
        df = pd.read_csv(tsv_file_path, sep='\t')
        
        # Convert DataFrame to dictionary records
        records = df.to_dict('records')
        
        # Optional: Add timestamp for when data was inserted
        for record in records:
            record['inserted_at'] = datetime.now()
            # Convert PRICE to float if it's stored as string
            record['PRICE'] = float(record['PRICE'])
        
        # Insert records into MongoDB
        result = collection.insert_many(records)
        print(f"Successfully inserted {len(result.inserted_ids)} records")
        
        return result.inserted_ids
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def tsv_to_mongodb_with_csv_reader(tsv_file_path):
    """
    Alternative method using csv.DictReader for more control with TSV
    """
    try:
        records = []
        
        with open(tsv_file_path, 'r') as file:
            # Specify tab delimiter for TSV
            csv_reader = csv.DictReader(file, delimiter='\t')
            
            for row in csv_reader:
                # Process each row
                record = {
                    'symbol': row['SYMBOL'],
                    'timestamp': row['TIMESTAMP'],
                    'price': float(row['PRICE']),
                    'inserted_at': datetime.now()
                }
                records.append(record)
        
        # Bulk insert
        if records:
            result = collection.insert_many(records)
            print(f"Successfully inserted {len(result.inserted_ids)} records")
            return result.inserted_ids
        
    except Exception as e:
        print(f"Error: {e}")
        return None

# Usage example
if __name__ == "__main__":
    # Replace with your TSV file path
    tsv_file = "datas\cleaned_stock.tsv"
    
    # Method 1: Using pandas
    tsv_to_mongodb(tsv_file)
    
    # Method 2: Using csv reader (uncomment to use)
    # tsv_to_mongodb_with_csv_reader(tsv_file)
    
    # Query to verify data was inserted
    print("\nSample records from MongoDB:")
    for record in collection.find().limit(5):
        print(record)
    
    # Close connection
    client.close()