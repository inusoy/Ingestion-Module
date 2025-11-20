import sqlite3

DB_NAME = "thesis_data.db"

def get_connection():
    """Returns a database connection. Easy to swap for Postgres later."""
    return sqlite3.connect(DB_NAME)

def init_db():
    """Creates the table structure."""
    conn = get_connection()
    cursor = conn.cursor()

    # Create table to store raw paper data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT,
            source_name TEXT,
            title TEXT,
            authors_json TEXT,
            year INTEGER,
            venue TEXT,
            doi TEXT,
            -- Prevent duplicates from the same source
            UNIQUE(source_id, source_name)
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Database '{DB_NAME}' initialized.")

if __name__ == "__main__":
    init_db()