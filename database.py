import os
import psycopg2
import sys
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

def get_connection():
    try:
        # Added client_encoding='UTF8' to fix special character issues
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            client_encoding="UTF8",
            options="-c search_path=orcid_source,public"
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Database Connection Failed: {e}")
        sys.exit(1)

def init_db():
    print("--> Attempting to connect to PostgreSQL...")
    conn = get_connection()
    
    # Verify we can see the tables now
    cur = conn.cursor()
    try:
        cur.execute("SELECT count(*) FROM profile;")
        print("✅ Successfully connected and found table 'profile' in schema 'orcid_source'.")
    except Exception as e:
        print(f"❌ Connection worked, but table still not found: {e}")
        
    conn.close()

if __name__ == "__main__":
    init_db()