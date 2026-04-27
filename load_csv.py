import sqlite3
import csv
import sys
import os

def load_csv(db_path, csv_path, table_name):
    print(f"Loading {csv_path} into {table_name}...")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # Connect to DB
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table
        cols = ", ".join([f'"{h}" TEXT' for h in headers])
        drop_table = f'DROP TABLE IF EXISTS "{table_name}"'
        create_table = f'CREATE TABLE "{table_name}" ({cols})'
        
        cursor.execute(drop_table)
        cursor.execute(create_table)
        
        # Insert data
        placeholders = ", ".join(["?"] * len(headers))
        insert_query = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
        
        # Batched insert
        batch = []
        count = 0
        for row in reader:
            batch.append(row)
            if len(batch) >= 10000:
                cursor.executemany(insert_query, batch)
                batch = []
                count += 10000
                print(f"  Inserted {count} rows...")
        
        if batch:
            cursor.executemany(insert_query, batch)
            count += len(batch)
            print(f"  Inserted {count} rows...")
            
        conn.commit()
        conn.close()
        print(f"Finished loading {table_name}")

if __name__ == "__main__":
    db_file = os.path.join(os.path.dirname(__file__), "database.sqlite")
    
    # Safe limit for Windows
    csv.field_size_limit(2147483647)
    
    csv_1 = r"c:\Users\Administrator\Downloads\inventory_data.csv"
    csv_2 = r"c:\Users\Administrator\Downloads\nigerian_retail_and_ecommerce_supply_chain_logistics_data.csv"
    
    load_csv(db_file, csv_1, "inventory_data")
    load_csv(db_file, csv_2, "supply_chain_logistics")
    
    print(f"Database successfully created at {db_file}")
