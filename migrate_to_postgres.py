# migrate_to_postgres.py - Safe migration from SQLite to PostgreSQL
import os
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor

def migrate_database():
    print("🔄 Starting database migration from SQLite to PostgreSQL...")
    
    # Check if we have both databases
    if not os.path.exists('data/corpus.db'):
        print("❌ SQLite database not found")
        return
    
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not set")
        return
    
    try:
        # Connect to SQLite
        sqlite_conn = sqlite3.connect('data/corpus.db')
        sqlite_conn.row_factory = sqlite3.Row
        sqlite_cursor = sqlite_conn.cursor()
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(database_url, sslmode='require')
        pg_cursor = pg_conn.cursor()
        
        # Migrate users
        print("👥 Migrating users...")
        sqlite_cursor.execute("SELECT * FROM users")
        users = sqlite_cursor.fetchall()
        
        for user in users:
            pg_cursor.execute(
                "INSERT INTO users (id, username, email, password_hash, role, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
                (user['id'], user['username'], user['email'], user['password_hash'], user['role'], user['created_at'])
            )
        
        # Migrate texts
        print("📝 Migrating texts...")
        sqlite_cursor.execute("SELECT * FROM texts")
        texts = sqlite_cursor.fetchall()
        
        for text in texts:
            pg_cursor.execute('''
                INSERT INTO texts (id, title, title_en, content, content_en, full_content, full_content_en, category, date_added, word_count, unique_words, source, user_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                text['id'], text['title'], text['title_en'], text['content'], text['content_en'],
                text['full_content'], text['full_content_en'], text['category'], text['date_added'],
                text['word_count'], text['unique_words'], text['source'], text['user_id'], text['status']
            ))
        
        pg_conn.commit()
        print("✅ Migration completed successfully!")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        pg_conn.rollback()
    finally:
        sqlite_cursor.close()
        sqlite_conn.close()
        pg_cursor.close()
        pg_conn.close()

if __name__ == '__main__':
    migrate_database()