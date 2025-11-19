import pandas as pd
import os
from sqlalchemy import create_engine, text
import logging
import time

# Define log folder
log_path = r"C:\Users\lenovo\OneDrive\Desktop\vendor\logs"
os.makedirs(log_path, exist_ok=True)
log_file = os.path.join(log_path, "app.log")

# Configure logging
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)
logging.info("Logging initialized successfully.")

# Database connection (absolute path)
db_path = r"C:\Users\lenovo\OneDrive\Desktop\vendor\inventory.db"
engine = create_engine(f"sqlite:///{db_path}")

# -------------------------------------------------
# DROP TABLE FUNCTION
# -------------------------------------------------
def drop_table_if_exists(engine, table_name):
    """Safely drop a table if it already exists."""
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
        print(f"   Old table '{table_name}' cleared.")
    except Exception as e:
        print(f"   Warning: Could not drop table {table_name}: {e}")

# -------------------------------------------------
# INGESTION FUNCTION
# -------------------------------------------------
def ingest_db(file_path, table_name, engine):
    """Ingests CSV data into SQLite DB with smart chunk sizing."""
    sample = pd.read_csv(file_path, nrows=5)
    num_columns = len(sample.columns)

    safe_chunk_size = max(1, 999 // num_columns)
    total_rows = 0

    print(f"   Detected {num_columns} columns → using chunk size = {safe_chunk_size}")

    try:
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=safe_chunk_size)):
            chunk.to_sql(
                table_name,
                con=engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            total_rows += len(chunk)
            logging.info(f"Chunk {i+1} for {file_path}: {len(chunk)} rows uploaded. Total: {total_rows}")
            print(f"   Chunk {i+1}: {len(chunk)} rows uploaded (Total: {total_rows})")

        print(f"✓ File '{table_name}' fully ingested ({total_rows} rows).\n")

    except Exception as e:
        logging.error(f"Error ingesting {file_path}: {e}")
        print(f"Error processing {file_path}: {e}\n")

# -------------------------------------------------
# LOAD ALL CSV FILES
# -------------------------------------------------
data_path = r"C:\Users\lenovo\OneDrive\Desktop\vendor\data"

def load_raw_data():
    start = time.time()
    print("🚀 Starting clean data ingestion...\n")

    for file in os.listdir(data_path):
        if file.endswith('.csv'):
            file_path = os.path.join(data_path, file)
            table_name = file[:-4]

            print(f"📌 Ingesting file: {file}")

            # Drop old table to prevent duplicate ingestion
            drop_table_if_exists(engine, table_name)

            # Fresh ingestion
            ingest_db(file_path, table_name, engine)

    total_time = (time.time() - start) / 60
    print(f"🎉 All files ingested successfully in {total_time:.2f} minutes.")

# Run when executed directly
if __name__ == "__main__":
    load_raw_data()
