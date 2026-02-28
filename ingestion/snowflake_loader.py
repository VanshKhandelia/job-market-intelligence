# snowflake_loader.py

import snowflake.connector  # the official Snowflake Python library
import os                 
from dotenv import load_dotenv  
import pandas as pd
load_dotenv()
import math

def get_snowflake_connection():
    """
    Creates and returns a connection to Snowflake.
    We wrap it in a function so we can reuse it anywhere in our project.
    """
    
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),       
        password=os.getenv("SNOWFLAKE_PASSWORD"), 
        account=os.getenv("SNOWFLAKE_ACCOUNT"),   
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),  
        schema=os.getenv("SNOWFLAKE_SCHEMA")      
    )
    
    return conn 

def test_connection():
    """Quick test to verify connection works"""
    
    conn = get_snowflake_connection()
  
    cursor = conn.cursor()
    
    cursor.execute("SELECT CURRENT_VERSION()")
    
    result = cursor.fetchone()
    
    print(f"✅ Connected to Snowflake! Version: {result[0]}")
    
    cursor.close()
    conn.close()
    


def load_csv_to_bronze():
    
    # Read the CSV
    df = pd.read_csv("data/raw_jobs.csv")
    
    def clean_value(v):
        try:
            if v is None:
                return None
            if isinstance(v, float) and math.isnan(v):
                return None
            if isinstance(v, str) and v.lower() == 'nan':
                return None
            return v
        except:
            return None

    rows = [tuple(clean_value(v) for v in row) for row in df.itertuples(index=False, name=None)]
    
    # Connect to Snowflake
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    # Get all existing job_ids from Snowflake in one query
    cursor.execute("SELECT job_id FROM BRONZE.RAW_JOB_POSTINGS")
    existing_ids = {row[0] for row in cursor.fetchall()}  # a set for fast lookup
    
    # Filter to only new rows
    new_rows = [row for row in rows if row[1] not in existing_ids]  # row[1] is job_id
    
    if not new_rows:
        print("✅ No new jobs to load — everything already exists in Snowflake")
        return

    cursor.executemany("""
        INSERT INTO BRONZE.RAW_JOB_POSTINGS (
            company_name_searched, job_id, job_title, company,
            location, description, salary_min, salary_max,
            contract_type, contract_time, category, created,
            redirect_url, extracted_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, new_rows)
    
    conn.commit()
    print(f"✅ Loaded {len(new_rows)} new rows into Snowflake Bronze layer")
    print(f"⏭️  Skipped {len(rows) - len(new_rows)} duplicate rows")
    
    cursor.close()
    conn.close()


# This means: only run test_connection() if we run THIS file directly
# If another file imports this, it won't auto-run
if __name__ == "__main__":
    # test_connection()
    load_csv_to_bronze()
