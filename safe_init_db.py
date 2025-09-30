# safe_init_db.py - Safe database initialization that preserves existing data
import os
import sys

# Add the current directory to the path so we can import app
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import init_database, get_db_connection

def safe_init_database():
    """Safely initialize the database without dropping existing tables"""
    print("=== SAFE DATABASE INITIALIZATION ===")
    print("This will create any missing tables but will NOT delete existing data.")
    print("Existing user contributions will be preserved.")
    
    response = input("Do you want to continue? (y/N): ").strip().lower()
    if response != 'y':
        print("Operation cancelled.")
        return False
    
    print("Initializing database safely...")
    success = init_database()
    
    if success:
        print("Database initialized successfully without losing any data!")
    else:
        print("Database initialization failed.")
    
    return success

if __name__ == '__main__':
    safe_init_database()
    