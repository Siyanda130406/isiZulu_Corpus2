# app.py - PostgreSQL compatible version
import os
import re
from flask import Flask, render_template, request, redirect, url_for, abort, flash
from collections import Counter

# Try to import PostgreSQL
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    import sqlite3

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')

# Category mapping
CATEGORY_MAP = {
    'izaga': {'en': 'proverbs', 'zu': 'izaga'},
    'izibongo': {'en': 'praise poetry', 'zu': 'izibongo'},
    'izisho': {'en': 'idioms', 'zu': 'izisho'},
    'philosophy': {'en': 'philosophy', 'zu': 'ifilosofi'},
    'folktale': {'en': 'folktale', 'zu': 'inganekwane'},
    'history': {'en': 'history', 'zu': 'umlando'},
    'other': {'en': 'other', 'zu': 'okunye'}
}

def get_db_connection():
    """Get database connection, using PostgreSQL if available"""
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url and POSTGRES_AVAILABLE:
        # Use PostgreSQL
        conn = psycopg2.connect(database_url, sslmode='require')
        return conn
    else:
        # Fallback to SQLite for local development
        conn = sqlite3.connect('corpus.db')
        conn.row_factory = sqlite3.Row
        return conn

def dict_factory(cursor, row):
    """Convert database row to dictionary for both SQLite and PostgreSQL"""
    if POSTGRES_AVAILABLE and os.environ.get('DATABASE_URL'):
        # PostgreSQL already returns dict-like objects
        return row
    else:
        # SQLite: convert row to dictionary
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

def init_database():
    """Initialize database tables"""
    print("Initializing database...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if we're using PostgreSQL
        is_postgres = POSTGRES_AVAILABLE and os.environ.get('DATABASE_URL')
        
        if is_postgres:
            # PostgreSQL table creation
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS texts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    title_en TEXT NOT NULL,
                    content TEXT NOT NULL,
                    content_en TEXT NOT NULL,
                    full_content TEXT,
                    full_content_en TEXT,
                    category TEXT NOT NULL,
                    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    word_count INTEGER DEFAULT 0,
                    unique_words INTEGER DEFAULT 0,
                    source TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS corpus_stats (
                    id SERIAL PRIMARY KEY,
                    total_words INTEGER DEFAULT 0,
                    total_unique_words INTEGER DEFAULT 0,
                    total_texts INTEGER DEFAULT 0,
                    avg_word_length REAL DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        else:
            # SQLite table creation (for local development)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS texts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    title_en TEXT NOT NULL,
                    content TEXT NOT NULL,
                    content_en TEXT NOT NULL,
                    full_content TEXT,
                    full_content_en TEXT,
                    category TEXT NOT NULL,
                    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    word_count INTEGER DEFAULT 0,
                    unique_words INTEGER DEFAULT 0,
                    source TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS corpus_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    total_words INTEGER DEFAULT 0,
                    total_unique_words INTEGER DEFAULT 0,
                    total_texts INTEGER DEFAULT 0,
                    avg_word_length REAL DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        
        # Initialize corpus_stats if empty
        if is_postgres:
            cursor.execute("SELECT COUNT(*) as count FROM corpus_stats")
            result = cursor.fetchone()
            count = result['count'] if result else 0
        else:
            cursor.execute("SELECT COUNT(*) as count FROM corpus_stats")
            count = cursor.fetchone()[0]
        
        if count == 0:
            if is_postgres:
                cursor.execute("INSERT INTO corpus_stats (total_words, total_unique_words, total_texts, avg_word_length) VALUES (0, 0, 0, 0)")
            else:
                cursor.execute("INSERT INTO corpus_stats (total_words, total_unique_words, total_texts, avg_word_length) VALUES (0, 0, 0, 0)")
        
        conn.commit()
        print("Database initialization completed successfully!")
        return True
    except Exception as e:
        print(f"Error initializing database: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

# Initialize database when the app starts
init_database()

def parse_search_query(q):
    parsed = {'terms': [], 'phrases': [], 'filters': {}, 'category_search': None}
    if not q:
        return parsed
    # quoted phrases
    parsed['phrases'] = re.findall(r'"([^"]+)"', q)
    tmp = re.sub(r'"[^"]+"', '', q)
    # filters of form key:value
    for m in re.findall(r'(\w+):(".*?"|\S+)', tmp):
        key = m[0]
        val = m[1].strip('"')
        parsed['filters'][key] = val
        tmp = tmp.replace(f'{key}:{m[1]}', '')
    # remaining terms
    parsed['terms'] = [t for t in re.split(r'\s+', tmp) if t]
    
    # Check if the query matches any category term (English or IsiZulu)
    query_lower = q.strip().lower()
    for category_id, names in CATEGORY_MAP.items():
        if query_lower == names['en'].lower() or query_lower == names['zu'].lower() or query_lower == category_id:
            parsed['category_search'] = category_id
            break
    
    return parsed

def get_category_display_name(category_id, language='en'):
    """Get the display name for a category in the specified language"""
    if category_id in CATEGORY_MAP:
        return CATEGORY_MAP[category_id][language]
    return category_id

def extract_words(text):
    """Extract words from text using regex"""
    if not text:
        return []
    # Match words with Unicode support for isiZulu characters
    return re.findall(r'[\w\u00C0-\u017F]+', text.lower())

def get_corpus_statistics():
    """Get corpus statistics with error handling - for both languages"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if we're using PostgreSQL
        is_postgres = POSTGRES_AVAILABLE and os.environ.get('DATABASE_URL')
        
        # Get basic statistics
        if is_postgres:
            cursor.execute("SELECT * FROM corpus_stats ORDER BY last_updated DESC LIMIT 1")
            stats = cursor.fetchone()
        else:
            cursor.execute("SELECT * FROM corpus_stats ORDER BY last_updated DESC LIMIT 1")
            stats = dict_factory(cursor, cursor.fetchone()) if cursor.fetchone() else None
        
        # Get all texts for analysis
        cursor.execute("SELECT content, content_en, full_content, full_content_en FROM texts")
        if is_postgres:
            texts = cursor.fetchall()
        else:
            texts = [dict_factory(cursor, row) for row in cursor.fetchall()]
        
        # Analyze both languages
        zu_words = []
        en_words = []
        
        for text in texts:
            # Extract isiZulu words
            if text['content']:
                zu_words.extend(extract_words(text['content']))
            if text['full_content']:
                zu_words.extend(extract_words(text['full_content']))
            
            # Extract English words
            if text['content_en']:
                en_words.extend(extract_words(text['content_en']))
            if text['full_content_en']:
                en_words.extend(extract_words(text['full_content_en']))
        
        # Get word frequency for both languages
        zu_word_freq = Counter(zu_words).most_common(20)
        en_word_freq = Counter(en_words).most_common(20)
        
        # Get word pairs for both languages
        def get_word_pairs(words, n=15):
            pairs = Counter()
            for i in range(len(words) - 1):
                pair = (words[i], words[i+1])
                pairs[pair] += 1
            return pairs.most_common(n)
        
        zu_word_pairs = get_word_pairs(zu_words)
        en_word_pairs = get_word_pairs(en_words)
        
        return {
            'stats': stats if stats else {
                'total_words': 0,
                'total_unique_words': 0,
                'total_texts': 0,
                'avg_word_length': 0,
                'last_updated': 'Never'
            },
            'zu_word_frequency': [{'word': word, 'frequency': count} for word, count in zu_word_freq],
            'en_word_frequency': [{'word': word, 'frequency': count} for word, count in en_word_freq],
            'zu_word_pairs': [{'word1': pair[0], 'word2': pair[1], 'frequency': count} for pair, count in zu_word_pairs],
            'en_word_pairs': [{'word1': pair[0], 'word2': pair[1], 'frequency': count} for pair, count in en_word_pairs]
        }
    except Exception as e:
        print(f"Error getting corpus statistics: {e}")
        return {
            'stats': {
                'total_words': 0,
                'total_unique_words': 0,
                'total_texts': 0,
                'avg_word_length': 0,
                'last_updated': 'Never'
            },
            'zu_word_frequency': [],
            'en_word_frequency': [],
            'zu_word_pairs': [],
            'en_word_pairs': []
        }
    finally:
        cursor.close()
        conn.close()

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search', methods=['GET', 'POST'])
def search():
    query = ''
    results = []
    total_results = 0
    per_page = 10
    current_page = 1
    parsed_query = None
    is_category_search = False
    total_pages = 1
    category_display_name = None

    if request.method == 'POST':
        query = (request.form.get('query') or '').strip()
        try:
            current_page = int(request.form.get('page', '1'))
            if current_page < 1:
                current_page = 1
        except ValueError:
            current_page = 1

        parsed_query = parse_search_query(query)
        if parsed_query.get('category_search'):
            is_category_search = True
            category_display_name = get_category_display_name(parsed_query['category_search'], 'en')

        if query:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                # Check if we're using PostgreSQL
                is_postgres = POSTGRES_AVAILABLE and os.environ.get('DATABASE_URL')
                
                params = []
                where_clauses = []
                
                if is_category_search:
                    # Exact category match (search by category ID)
                    where_clauses.append("category = %s" if is_postgres else "category = ?")
                    params.append(parsed_query['category_search'])
                else:
                    # Search in both isiZulu and English fields
                    search_terms = []
                    
                    # Add quoted phrases as exact matches
                    for phrase in parsed_query['phrases']:
                        search_terms.append(f'%{phrase}%')
                    
                    # Add individual terms
                    for term in parsed_query['terms']:
                        search_terms.append(f'%{term}%')
                    
                    # Build OR conditions for all search terms across all fields
                    or_conditions = []
                    for term in search_terms:
                        if is_postgres:
                            or_conditions.append("(title ILIKE %s OR title_en ILIKE %s OR content ILIKE %s OR content_en ILIKE %s OR full_content ILIKE %s OR full_content_en ILIKE %s OR category ILIKE %s)")
                            params.extend([term, term, term, term, term, term, term])
                        else:
                            or_conditions.append("(title LIKE ? OR title_en LIKE ? OR content LIKE ? OR content_en LIKE ? OR full_content LIKE ? OR full_content_en LIKE ? OR category LIKE ?)")
                            params.extend([term, term, term, term, term, term, term])
                    
                    if or_conditions:
                        where_clauses.append(f"({' OR '.join(or_conditions)})")
                
                # Handle filters
                for key, value in parsed_query['filters'].items():
                    if key.lower() == 'category':
                        # Map English category names to category IDs
                        category_id = None
                        for cat_id, names in CATEGORY_MAP.items():
                            if value.lower() == names['en'].lower() or value.lower() == names['zu'].lower() or value.lower() == cat_id:
                                category_id = cat_id
                                break
                        
                        if category_id:
                            where_clauses.append("category = %s" if is_postgres else "category = ?")
                            params.append(category_id)
                        else:
                            where_clauses.append("category ILIKE %s" if is_postgres else "category LIKE ?")
                            params.append(f'%{value}%')
                    elif key.lower() == 'title':
                        where_clauses.append("(title ILIKE %s OR title_en ILIKE %s)" if is_postgres else "(title LIKE ? OR title_en LIKE ?)")
                        params.extend([f'%{value}%', f'%{value}%'])
                    elif key.lower() == 'content':
                        where_clauses.append("(content ILIKE %s OR content_en ILIKE %s)" if is_postgres else "(content LIKE ? OR content_en LIKE ?)")
                        params.extend([f'%{value}%', f'%{value}%'])

                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                
                # count total
                count_sql = f"SELECT COUNT(1) FROM texts {where_sql}"
                cursor.execute(count_sql, params)
                
                if is_postgres:
                    total_results = cursor.fetchone()['count'] or 0
                else:
                    total_results = cursor.fetchone()[0] or 0
                    
                total_pages = (total_results + per_page - 1) // per_page if total_results else 1
                
                # Get results
                offset = (current_page - 1) * per_page
                sql = f"SELECT id, title, title_en, content, content_en, category, word_count, unique_words FROM texts {where_sql} ORDER BY id DESC LIMIT %s OFFSET %s" if is_postgres else f"SELECT id, title, title_en, content, content_en, category, word_count, unique_words FROM texts {where_sql} ORDER BY id DESC LIMIT ? OFFSET ?"
                
                cursor.execute(sql, params + [per_page, offset])
                
                if is_postgres:
                    rows = cursor.fetchall()
                else:
                    rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
                
                for r in rows:
                    # Highlight search terms in the content
                    content = f"{r['content']} {r['content_en']}" or ''
                    snippet = content[:300]
                    
                    # Simple highlighting for each search term
                    for term in parsed_query['terms'] + parsed_query['phrases']:
                        if term:
                            snippet = snippet.replace(term, f'<mark>{term}</mark>')
                    
                    # Get category display name
                    category_display = get_category_display_name(r['category'], 'en')
                    
                    results.append((r['id'], r['title'], snippet, category_display, r['word_count'], r['unique_words']))
                    
            except Exception as e:
                print("DB search error:", e)
                results = []
                total_results = 0
                total_pages = 1
            finally:
                cursor.close()
                conn.close()

    return render_template('search.html',
                           query=query,
                           results=results,
                           total_results=total_results,
                           total_pages=total_pages,
                           current_page=current_page,
                           parsed_query=parsed_query,
                           is_category_search=is_category_search,
                           category_display_name=category_display_name)

@app.route('/detail/<int:item_id>')
def detail(item_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if we're using PostgreSQL
        is_postgres = POSTGRES_AVAILABLE and os.environ.get('DATABASE_URL')
        
        if is_postgres:
            cursor.execute("SELECT id, title, title_en, content, content_en, full_content, full_content_en, category, date_added, word_count, unique_words, source FROM texts WHERE id = %s", (item_id,))
            row = cursor.fetchone()
        else:
            cursor.execute("SELECT id, title, title_en, content, content_en, full_content, full_content_en, category, date_added, word_count, unique_words, source FROM texts WHERE id = ?", (item_id,))
            row = dict_factory(cursor, cursor.fetchone()) if cursor.fetchone() else None
            
        if not row:
            return ("Not found", 404)
        
        # Get category display name
        category_display = get_category_display_name(row['category'], 'en')
        
        # Prepare the data in the format your template expects - as a tuple
        text = (
            row['id'],           # text[0] - ID (not used in template)
            row['title'],        # text[1] - Title (isiZulu)
            row['title_en'],     # text[2] - Title (English)
            row['content'],      # text[3] - Summary/Short content (isiZulu)
            row['content_en'],   # text[4] - Summary/Short content (English)
            row['full_content'] or row['content'],  # text[5] - Full content (isiZulu)
            row['full_content_en'] or row['content_en'],  # text[6] - Full content (English)
            category_display,    # text[7] - Category (display name)
            row['date_added'],   # text[8] - Date added
            row['word_count'],   # text[9] - Word count
            row['unique_words'], # text[10] - Unique words
            row['source']        # text[11] - Source information
        )
        
        return render_template('detail.html', text=text)
    except Exception as e:
        print("Error in detail view:", e)
        return ("Error loading content", 500)
    finally:
        cursor.close()
        conn.close()

@app.route('/contribute', methods=['GET', 'POST'])
def contribute():
    """Page for users to contribute new content to the corpus"""
    if request.method == 'POST':
        try:
            # Get form data
            title_zu = request.form.get('title_zu', '').strip()
            title_en = request.form.get('title_en', '').strip()
            content_zu = request.form.get('content_zu', '').strip()
            content_en = request.form.get('content_en', '').strip()
            full_content_zu = request.form.get('full_content_zu', '').strip()
            full_content_en = request.form.get('full_content_en', '').strip()
            category = request.form.get('category', '').strip()
            source = request.form.get('source', '').strip()
            
            print(f"Form data received: title_zu={title_zu}, title_en={title_en}, category={category}")
            
            # Basic validation
            if not all([title_zu, title_en, content_zu, content_en, category]):
                flash("Please fill in all required fields", "error")
                return render_template('contribute.html', categories=CATEGORY_MAP)
            
            # Calculate word statistics
            def count_words(text):
                return len(text.split()) if text else 0
            
            def get_unique_words(text):
                if not text:
                    return set()
                words = text.lower().split()
                return set(words)
            
            word_count = count_words(content_zu)
            unique_words = len(get_unique_words(content_zu))
            
            print(f"Word count: {word_count}, Unique words: {unique_words}")
            
            # Save to database
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                # Check if we're using PostgreSQL
                is_postgres = POSTGRES_AVAILABLE and os.environ.get('DATABASE_URL')
                
                if is_postgres:
                    # PostgreSQL insert
                    cursor.execute(
                        "INSERT INTO texts (title, title_en, content, content_en, full_content, full_content_en, category, word_count, unique_words, source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                        (title_zu, title_en, content_zu, content_en, full_content_zu or content_zu, full_content_en or content_en, category, word_count, unique_words, source)
                    )
                else:
                    # SQLite insert
                    cursor.execute(
                        "INSERT INTO texts (title, title_en, content, content_en, full_content, full_content_en, category, word_count, unique_words, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (title_zu, title_en, content_zu, content_en, full_content_zu or content_zu, full_content_en or content_en, category, word_count, unique_words, source)
                    )
                
                # Update corpus statistics
                if is_postgres:
                    cursor.execute("UPDATE corpus_stats SET total_words = total_words + %s, total_unique_words = total_unique_words + %s, total_texts = total_texts + 1, last_updated = CURRENT_TIMESTAMP", 
                                (word_count, unique_words))
                else:
                    cursor.execute("UPDATE corpus_stats SET total_words = total_words + ?, total_unique_words = total_unique_words + ?, total_texts = total_texts + 1, last_updated = CURRENT_TIMESTAMP", 
                                (word_count, unique_words))
                
                conn.commit()
                print("Contribution saved successfully")
                flash("Thank you for your contribution! Your content has been added to the corpus.", "success")
                return redirect(url_for('contribute'))
                
            except Exception as e:
                print(f"Error saving contribution: {e}")
                flash("An error occurred while saving your contribution. Please try again.", "error")
                return render_template('contribute.html', categories=CATEGORY_MAP)
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            print(f"Error processing contribution form: {e}")
            flash("An error occurred while processing your contribution. Please try again.", "error")
            return render_template('contribute.html', categories=CATEGORY_MAP)
    
    return render_template('contribute.html', categories=CATEGORY_MAP)

@app.route('/statistics')
def statistics():
    """Page showing corpus statistics"""
    try:
        stats = get_corpus_statistics()
        return render_template('statistics.html', stats=stats)
    except Exception as e:
        print(f"Error in statistics route: {e}")
        return render_template('error.html', error_message='Error loading statistics'), 500

@app.route('/test-db')
def test_db():
    """Test route to check if database is working"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if we're using PostgreSQL
        is_postgres = POSTGRES_AVAILABLE and os.environ.get('DATABASE_URL')
        
        # Check if tables exist
        if is_postgres:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [row['table_name'] for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
        
        # Check if we can insert a test record
        if is_postgres:
            cursor.execute("INSERT INTO texts (title, title_en, content, content_en, category) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                         ("Test Title", "Test Title EN", "Test content", "Test content EN", "test"))
            inserted_id = cursor.fetchone()['id']
            cursor.execute("SELECT * FROM texts WHERE id = %s", (inserted_id,))
            result = cursor.fetchone()
            # Clean up test record
            cursor.execute("DELETE FROM texts WHERE id = %s", (inserted_id,))
        else:
            cursor.execute("INSERT INTO texts (title, title_en, content, content_en, category) VALUES (?, ?, ?, ?, ?)",
                         ("Test Title", "Test Title EN", "Test content", "Test content EN", "test"))
            conn.commit()
            cursor.execute("SELECT * FROM texts WHERE title = ?", ("Test Title",))
            result = dict_factory(cursor, cursor.fetchone()) if cursor.fetchone() else None
            # Clean up test record
            cursor.execute("DELETE FROM texts WHERE title = ?", ("Test Title",))
        
        conn.commit()
        conn.close()
        
        return f"Database test successful! Tables: {tables}, Inserted record: {result['id'] if result else 'None'}"
        
    except Exception as e:
        return f"Database test failed: {str(e)}"

@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', error_message='Page not found'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', error_message='Internal server error'), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)