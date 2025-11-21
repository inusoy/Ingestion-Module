from database import get_connection

def check_database_structure():
    conn = get_connection()
    cursor = conn.cursor()
    
    print(f"--- Checking tables in database: {conn.info.dbname} ---")
    
    # 1. Get all table names
    cursor.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name;
    """)
    tables = cursor.fetchall()
    
    if not tables:
        print("❌ NO TABLES FOUND! The database is empty.")
    else:
        print(f"✅ Found {len(tables)} tables:")
        for schema, table in tables:
            print(f"   - {schema}.{table}")
            
            # Optional: Print columns for the 'profile' table if found
            if table == 'profile':
                cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'profile'")
                columns = cursor.fetchall()
                print("     Columns:", [col[0] for col in columns])

    conn.close()

if __name__ == "__main__":
    check_database_structure()