import os
import sqlite3

# Paths
data_path = r"C:\Users\lenovo\OneDrive\Desktop\vendor\data"
db_path = r"C:\Users\lenovo\OneDrive\Desktop\vendor\inventory.db"

# Connect to DB
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("\n📌 Checking CSV vs Database Ingestion Status...\n")

# Loop through each CSV file
for file in os.listdir(data_path):
    if file.endswith(".csv"):
        
        csv_file_path = os.path.join(data_path, file)
        table_name = file[:-4]   # Remove .csv extension
        
        # Count CSV rows
        try:
            csv_rows = sum(1 for _ in open(csv_file_path, encoding="utf-8")) - 1
        except:
            csv_rows = sum(1 for _ in open(csv_file_path, encoding="latin-1")) - 1
        
        # Count DB table rows
        try:
            db_rows = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        except sqlite3.Error:
            db_rows = -1  # table missing
        
        # Print results
        print(f"🗂 File: {file}")
        print(f"   → CSV Rows: {csv_rows}")
        
        if db_rows == -1:
            print(f"   ❌ Table '{table_name}' does NOT exist in DB.\n")
        else:
            print(f"   → DB  Rows: {db_rows}")
            
            if csv_rows == db_rows:
                print("   ✅ Fully Ingested!\n")
            else:
                missing = csv_rows - db_rows
                if missing > 0:
                    print(f"   ⚠️ Missing {missing} rows — incomplete ingestion.\n")
                else:
                    print(f"   ⚠️ DB has MORE rows than CSV (Possible duplicate ingestion).\n")

# Close DB
conn.close()

print("✔️ Check completed.")
