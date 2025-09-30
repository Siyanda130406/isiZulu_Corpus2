# reset_database.py - Completely reset the database
import os
import sqlite3

def reset_database():
    print("COMPLETELY RESETTING DATABASE...")
    
    # Remove existing database
    db_path = 'data/corpus.db'
    if os.path.exists(db_path):
        os.remove(db_path)
        print("✅ Old database deleted")
    
    # Import and run the fixed app.py initialization
    from app import init_database
    init_database()
    print("✅ New database created with ALL content")

if __name__ == '__main__':
    reset_database()