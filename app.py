
import os
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from datetime import datetime
import re
from collections import Counter

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    print("PostgreSQL not available, using SQLite only")

try:
    import sqlite3
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False
    print("SQLite not available")

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')


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
    """Get database connection - PostgreSQL for production, SQLite for local dev"""
    database_url = os.environ.get('DATABASE_URL')
    
 
    if database_url and POSTGRES_AVAILABLE:
        try:
       
            conn = psycopg2.connect(database_url, sslmode='require')
            print("✅ Connected to PostgreSQL database")
            return conn
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            print("🔄 Falling back to SQLite...")
    

    if SQLITE_AVAILABLE:
        try:
            os.makedirs('data', exist_ok=True)
            conn = sqlite3.connect('data/corpus.db')
            conn.row_factory = sqlite3.Row
            print("✅ Connected to SQLite database")
            return conn
        except Exception as e:
            print(f"❌ SQLite connection failed: {e}")
    
    raise Exception("No database connection available")

def execute_query(query, params=()):
    """Execute query with proper database handling"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(query, params)
        

        if query.strip().upper().startswith('SELECT'):
            result = cursor.fetchall()
        else:
         
            conn.commit()
            result = cursor.rowcount
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def init_database():
    """Initialize database tables with sample data - WORKS WITH BOTH SQLite & PostgreSQL"""
    print("🔄 Initializing database...")
    
    conn = None
    try:
        conn = get_db_connection()
        
 
        is_postgres = 'postgresql' in str(conn).lower() if conn else False
        print(f"📊 Database type: {'PostgreSQL' if is_postgres else 'SQLite'}")
        
        cursor = conn.cursor()
        
   
        if is_postgres:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(80) UNIQUE NOT NULL,
                    email VARCHAR(120) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
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
                    source TEXT,
                    user_id INTEGER,
                    status TEXT DEFAULT 'approved'
                )
            ''')
        else:
        
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    role TEXT DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
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
                    source TEXT,
                    user_id INTEGER,
                    status TEXT DEFAULT 'approved'
                )
            ''')
        
   
        if is_postgres:
            cursor.execute("SELECT COUNT(*) FROM users")
        else:
            cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        
        if user_count == 0:
            print("👥 Creating default users...")
            users_data = [
                ('admin', 'admin@corpus.com', generate_password_hash('admin123'), 'admin'),
                ('manager', 'manager@corpus.com', generate_password_hash('manager123'), 'manager'),
                ('user', 'user@corpus.com', generate_password_hash('user123'), 'user')
            ]
            
            for username, email, password_hash, role in users_data:
                if is_postgres:
                    cursor.execute(
                        "INSERT INTO users (username, email, password_hash, role) VALUES (%s, %s, %s, %s)",
                        (username, email, password_hash, role)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, ?)",
                        (username, email, password_hash, role)
                    )
        
     
        if is_postgres:
            cursor.execute("SELECT COUNT(*) FROM texts")
        else:
            cursor.execute("SELECT COUNT(*) FROM texts")
        text_count = cursor.fetchone()[0]
        
        if text_count == 0:
            print("📝 Adding comprehensive sample texts...")
            
    
            sample_texts = [
                ("Indlela ibuzwa kwabaphambili", "A path is asked from those who have walked it before",
                 "Isaga elikhuthaza ukulalela abanolwazi.", "A proverb that encourages listening to those with knowledge.",
                 "Indlela ibuzwa kwabaphambili. Leli isaga likhombisa ukubaluleka kokulalela abantu abanolwazi.", 
                 "A path is asked from those who have walked it before. This proverb shows the importance of listening to knowledgeable people.",
                 "izaga", "Traditional isiZulu wisdom", 60, 45),
                
                ("Umuntu ngumuntu ngabantu", "A person is a person through other people", 
                 "Isaga elichaza ukubaluleka kobuntu.", "A proverb that explains the importance of humanity.",
                 "Umuntu ngumuntu ngabantu. Leli isaga ligcizelela ukuxhumana kwabantu.", 
                 "A person is a person through other people. This proverb emphasizes human interconnectedness.",
                 "izaga", "Traditional isiZulu philosophy", 55, 40),
                
                ("Izibongo zikaShaka", "Praise Poetry of Shaka",
                 "Ubulawu obungelanga bulawu! Wen' owadl' amanye amadoda.", "The magic that was not magic! You who devoured other men.",
                 "Izibongo zenkosi uShaka kaSenzangakhona. Ubulawu obungelanga bulawu! Wen' owadl' amanye amadoda.", 
                 "Praise poetry of King Shaka kaSenzangakhona. The magic that was not magic! You who devoured other men.",
                 "izibongo", "Historical Zulu oral tradition", 80, 55),
                
                ("Isisho sokuthi ukuhamba kukufunda", "The saying that traveling is learning",
                 "Lesi isisho sikhomba ukubaluleka kokuhamba nokufunda.", "This saying shows the importance of traveling and learning.",
                 "Isisho sokuthi ukuhamba kukufunda. Lesi isisho sikhombisa ukuthi ukuhamba kuyindlela yokufunda.", 
                 "The saying that traveling is learning. This saying shows that traveling is a way of learning.",
                 "izisho", "Traditional wisdom", 50, 35),
                
                ("Ubuntu botho", "Humanity philosophy",
                 "Umqondo wobuntu owawuthandwa ngabantu basendulo.", "The concept of humanity that was loved by ancient people.",
                 "Ubuntu botho. Umqondo wobuntu owawuthandwa ngabantu basendulo.", 
                 "Humanity philosophy. The concept of humanity that was loved by ancient people.",
                 "philosophy", "Traditional philosophy", 45, 30)
            ]
            
            for text in sample_texts:
                if is_postgres:
                    cursor.execute('''
                        INSERT INTO texts (title, title_en, content, content_en, full_content, full_content_en, category, source, word_count, unique_words, status) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'approved')
                    ''', text)
                else:
                    cursor.execute('''
                        INSERT INTO texts (title, title_en, content, content_en, full_content, full_content_en, category, source, word_count, unique_words, status) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'approved')
                    ''', text)
        
        conn.commit()
   
        if is_postgres:
            cursor.execute("SELECT COUNT(*) as count, status FROM texts GROUP BY status")
        else:
            cursor.execute("SELECT COUNT(*) as count, status FROM texts GROUP BY status")
        status_counts = cursor.fetchall()
        
        print("✅ Database initialized successfully!")
        print("📊 Texts in database:")
        for row in status_counts:
            if is_postgres:
                print(f"   {row[1]}: {row[0]} texts")
            else:
                print(f"   {row['status']}: {row['count']} texts")
        
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

init_database()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def manager_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('login'))
        if session.get('role') not in ['manager', 'admin']:
            flash('You need manager privileges to access this page.', 'error')
            return redirect(url_for('home'))
        return f(*args, **kwargs)
    return decorated_function

def get_current_user():
    if 'user_id' in session:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
        
            is_postgres = 'postgresql' in str(conn).lower()
            
            if is_postgres:
                cursor.execute("SELECT id, username, email, role FROM users WHERE id = %s", (session['user_id'],))
            else:
                cursor.execute("SELECT id, username, email, role FROM users WHERE id = ?", (session['user_id'],))
            
            row = cursor.fetchone()
            
            if row:
                if is_postgres:
                    user = {'id': row[0], 'username': row[1], 'email': row[2], 'role': row[3]}
                else:
                    user = {'id': row[0], 'username': row[1], 'email': row[2], 'role': row[3]}
                return user
        finally:
            cursor.close()
            conn.close()
    return None

def get_pending_count():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        is_postgres = 'postgresql' in str(conn).lower()
        
        if is_postgres:
            cursor.execute("SELECT COUNT(*) FROM texts WHERE status = 'pending'")
        else:
            cursor.execute("SELECT COUNT(*) FROM texts WHERE status = 'pending'")
        
        count = cursor.fetchone()[0]
        return count
    finally:
        cursor.close()
        conn.close()

def extract_words(text):
    """Extract words from text for frequency analysis"""
    if not text:
        return []
    words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
    return [word for word in words if len(word) > 2]  

def generate_statistics():
    """Generate comprehensive statistics for the corpus"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        is_postgres = 'postgresql' in str(conn).lower()
        
    
        if is_postgres:
            cursor.execute("SELECT * FROM texts WHERE status = 'approved'")
        else:
            cursor.execute("SELECT * FROM texts WHERE status = 'approved'")
        
        texts = cursor.fetchall()

        total_texts = len(texts)
        total_words = 0
        total_unique_words = 0
        all_zu_text = ""
        all_en_text = ""
        
        for text in texts:
            if is_postgres:
                total_words += text[9] or 0 
                total_unique_words += text[10] or 0 
                all_zu_text += f" {text[1]} {text[3]} {text[5] or ''}"  
                all_en_text += f" {text[2]} {text[4]} {text[6] or ''}"  
            else:
                total_words += text['word_count'] or 0
                total_unique_words += text['unique_words'] or 0
                all_zu_text += f" {text['title']} {text['content']} {text['full_content'] or ''}"
                all_en_text += f" {text['title_en']} {text['content_en']} {text['full_content_en'] or ''}"
        

        zu_words = extract_words(all_zu_text)
        en_words = extract_words(all_en_text)
        
        zu_word_freq = Counter(zu_words).most_common(20)
        en_word_freq = Counter(en_words).most_common(20)
        

        zu_word_pairs = []
        en_word_pairs = []
        
        if len(zu_words) > 1:
            zu_bigrams = zip(zu_words[:-1], zu_words[1:])
            zu_word_pairs = Counter(zu_bigrams).most_common(10)
            zu_word_pairs = [{'word1': pair[0][0], 'word2': pair[0][1], 'frequency': pair[1]} for pair in zu_word_pairs]
        
        if len(en_words) > 1:
            en_bigrams = zip(en_words[:-1], en_words[1:])
            en_word_pairs = Counter(en_bigrams).most_common(10)
            en_word_pairs = [{'word1': pair[0][0], 'word2': pair[0][1], 'frequency': pair[1]} for pair in en_word_pairs]
        

        if is_postgres:
            cursor.execute("SELECT category, COUNT(*) FROM texts WHERE status = 'approved' GROUP BY category")
        else:
            cursor.execute("SELECT category, COUNT(*) FROM texts WHERE status = 'approved' GROUP BY category")
        category_stats = cursor.fetchall()
        
        stats = {
            'stats': {
                'total_texts': total_texts,
                'total_words': total_words,
                'total_unique_words': total_unique_words,
                'avg_word_length': total_words / max(total_texts, 1),
                'last_updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            'zu_word_frequency': [{'word': word, 'frequency': freq} for word, freq in zu_word_freq],
            'en_word_frequency': [{'word': word, 'frequency': freq} for word, freq in en_word_freq],
            'zu_word_pairs': zu_word_pairs,
            'en_word_pairs': en_word_pairs,
            'category_stats': category_stats
        }
        
        return stats
        
    finally:
        cursor.close()
        conn.close()

def count_search_occurrences(query):
    """Count total occurrences of search term in all texts"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        is_postgres = 'postgresql' in str(conn).lower()
        

        if is_postgres:
            cursor.execute("SELECT title, title_en, content, content_en, full_content, full_content_en FROM texts WHERE status = 'approved'")
        else:
            cursor.execute("SELECT title, title_en, content, content_en, full_content, full_content_en FROM texts WHERE status = 'approved'")
        
        texts = cursor.fetchall()
        total_occurrences = 0
        
        for text in texts:
            if is_postgres:
           
                combined_text = f"{text[0]} {text[1]} {text[2]} {text[3]} {text[4] or ''} {text[5] or ''}".lower()
            else:
                combined_text = f"{text['title']} {text['title_en']} {text['content']} {text['content_en']} {text['full_content'] or ''} {text['full_content_en'] or ''}".lower()
            
       
            total_occurrences += combined_text.count(query.lower())
        
        return total_occurrences
        
    finally:
        cursor.close()
        conn.close()


@app.route('/')
def root_redirect():
    return redirect(url_for('login'))

@app.route('/home')
@login_required
def home():
    """Home page dashboard after login"""
    user = get_current_user()
    pending_count = get_pending_count()
    

    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        is_postgres = 'postgresql' in str(conn).lower()
        
  
        if is_postgres:
            cursor.execute("SELECT COUNT(*) FROM texts WHERE status = 'approved'")
        else:
            cursor.execute("SELECT COUNT(*) FROM texts WHERE status = 'approved'")
        total_texts = cursor.fetchone()[0]
        
 
        if is_postgres:
            cursor.execute("SELECT category, COUNT(*) FROM texts WHERE status = 'approved' GROUP BY category")
        else:
            cursor.execute("SELECT category, COUNT(*) FROM texts WHERE status = 'approved' GROUP BY category")
        category_stats = cursor.fetchall()
        
  
        if is_postgres:
            cursor.execute("SELECT title, title_en, category, date_added FROM texts WHERE status = 'approved' ORDER BY date_added DESC LIMIT 5")
        else:
            cursor.execute("SELECT title, title_en, category, date_added FROM texts WHERE status = 'approved' ORDER BY date_added DESC LIMIT 5")
        recent_texts = cursor.fetchall()
        
        stats = {
            'total_texts': total_texts,
            'category_stats': category_stats,
            'recent_texts': recent_texts,
            'is_postgres': is_postgres
        }
        
    finally:
        cursor.close()
        conn.close()
    
    return render_template('index.html', user=user, stats=stats, categories=CATEGORY_MAP, pending_count=pending_count)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        
        if not username or not password:
            flash('Please enter both username and password.', 'error')
            return render_template('login.html')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            is_postgres = 'postgresql' in str(conn).lower()
            
            if is_postgres:
                cursor.execute("SELECT id, username, password_hash, role FROM users WHERE username = %s OR email = %s", (username, username))
            else:
                cursor.execute("SELECT id, username, password_hash, role FROM users WHERE username = ? OR email = ?", (username, username))
            
            row = cursor.fetchone()
            
            if row and check_password_hash(row[2], password):
                session['user_id'] = row[0]
                session['username'] = row[1]
                session['role'] = row[3]
                flash('Login successful!', 'success')
                return redirect(url_for('home'))
            else:
                flash('Invalid username or password.', 'error')
        finally:
            cursor.close()
            conn.close()
    
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '').strip()
        
        if not username or not email or not password:
            flash('All fields are required.', 'error')
            return render_template('register.html')
        
        if len(password) < 6:
            flash('Password must be at least 6 characters long.', 'error')
            return render_template('register.html')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
 
        is_postgres = 'postgresql' in str(conn).lower()
        
        if is_postgres:
            cursor.execute("SELECT id FROM users WHERE username = %s OR email = %s", (username, email))
        else:
            cursor.execute("SELECT id FROM users WHERE username = ? OR email = ?", (username, email))
        
        if cursor.fetchone():
            flash('Username or email already exists.', 'error')
            conn.close()
            return render_template('register.html')
        

        hashed_password = generate_password_hash(password)
        
        if is_postgres:
            cursor.execute(
                "INSERT INTO users (username, email, password_hash, role) VALUES (%s, %s, %s, 'user')",
                (username, email, hashed_password)
            )
        else:
            cursor.execute(
                "INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, 'user')",
                (username, email, hashed_password)
            )
        
        conn.commit()
        conn.close()
        
        flash('Registration successful! Please log in.', 'success')
        return redirect(url_for('login'))
    
    return render_template('register.html')

@app.route('/contribute', methods=['GET', 'POST'])
@login_required
def contribute():
    user = get_current_user()
    
    if request.method == 'POST':
        title = request.form.get('title_zu', '').strip()
        title_en = request.form.get('title_en', '').strip()
        content = request.form.get('content_zu', '').strip()
        content_en = request.form.get('content_en', '').strip()
        full_content = request.form.get('full_content_zu', '').strip()
        full_content_en = request.form.get('full_content_en', '').strip()
        category = request.form.get('category', '').strip()
        source = request.form.get('source', '').strip()
        
        if not all([title, title_en, content, content_en, category]):
            flash('Please fill in all required fields.', 'error')
            return render_template('contribute.html', user=user, categories=CATEGORY_MAP)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            is_postgres = 'postgresql' in str(conn).lower()
            
     
            all_text = f"{content} {full_content}"
            words = all_text.split()
            word_count = len(words)
            unique_words = len(set(words))
            
        
            if is_postgres:
                cursor.execute('''
                    INSERT INTO texts (title, title_en, content, content_en, full_content, full_content_en, category, source, user_id, word_count, unique_words, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                ''', (title, title_en, content, content_en, full_content, full_content_en, category, source, session['user_id'], word_count, unique_words))
            else:
                cursor.execute('''
                    INSERT INTO texts (title, title_en, content, content_en, full_content, full_content_en, category, source, user_id, word_count, unique_words, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                ''', (title, title_en, content, content_en, full_content, full_content_en, category, source, session['user_id'], word_count, unique_words))
            
            conn.commit()
            flash('Text submitted successfully! It will be reviewed by a manager.', 'success')
            return redirect(url_for('contribute'))
        finally:
            cursor.close()
            conn.close()
    
    return render_template('contribute.html', user=user, categories=CATEGORY_MAP)

@app.route('/search', methods=['GET', 'POST'])
@login_required
def search():
    user = get_current_user()
    query = request.form.get('query', '').strip() if request.method == 'POST' else request.args.get('q', '')
    page = request.args.get('page', 1, type=int)
    results_per_page = 10
    
    results = []
    total_results = 0
    total_occurrences = 0
    is_category_search = False
    
    if query:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            is_postgres = 'postgresql' in str(conn).lower()
          
            category_query = None
            for category_id, category_info in CATEGORY_MAP.items():
                if query.lower() in [category_id, category_info['en'].lower(), category_info['zu'].lower()]:
                    category_query = category_id
                    is_category_search = True
                    break
            
            offset = (page - 1) * results_per_page
            
            if category_query:
            
                if is_postgres:
               
                    cursor.execute('''
                        SELECT COUNT(*) FROM texts 
                        WHERE status = 'approved' AND category = %s
                    ''', (category_query,))
                    total_results = cursor.fetchone()[0]
                    
             
                    cursor.execute('''
                        SELECT * FROM texts 
                        WHERE status = 'approved' AND category = %s
                        ORDER BY id DESC
                        LIMIT %s OFFSET %s
                    ''', (category_query, results_per_page, offset))
                else:
               
                    cursor.execute('''
                        SELECT COUNT(*) FROM texts 
                        WHERE status = 'approved' AND category = ?
                    ''', (category_query,))
                    total_results = cursor.fetchone()[0]
                    
               
                    cursor.execute('''
                        SELECT * FROM texts 
                        WHERE status = 'approved' AND category = ?
                        ORDER BY id DESC
                        LIMIT ? OFFSET ?
                    ''', (category_query, results_per_page, offset))
            else:
           
                search_term = f"%{query}%"
                
                if is_postgres:
              
                    cursor.execute('''
                        SELECT COUNT(*) FROM texts 
                        WHERE status = 'approved' 
                        AND (title ILIKE %s OR title_en ILIKE %s OR content ILIKE %s OR content_en ILIKE %s OR full_content ILIKE %s)
                    ''', (search_term, search_term, search_term, search_term, search_term))
                    total_results = cursor.fetchone()[0]
                    
       
                    cursor.execute('''
                        SELECT * FROM texts 
                        WHERE status = 'approved' 
                        AND (title ILIKE %s OR title_en ILIKE %s OR content ILIKE %s OR content_en ILIKE %s OR full_content ILIKE %s)
                        ORDER BY id DESC
                        LIMIT %s OFFSET %s
                    ''', (search_term, search_term, search_term, search_term, search_term, results_per_page, offset))
                else:
                
                    cursor.execute('''
                        SELECT COUNT(*) FROM texts 
                        WHERE status = 'approved' 
                        AND (title LIKE ? OR title_en LIKE ? OR content LIKE ? OR content_en LIKE ? OR full_content LIKE ?)
                    ''', (search_term, search_term, search_term, search_term, search_term))
                    total_results = cursor.fetchone()[0]
                    
               
                    cursor.execute('''
                        SELECT * FROM texts 
                        WHERE status = 'approved' 
                        AND (title LIKE ? OR title_en LIKE ? OR content LIKE ? OR content_en LIKE ? OR full_content LIKE ?)
                        ORDER BY id DESC
                        LIMIT ? OFFSET ?
                    ''', (search_term, search_term, search_term, search_term, search_term, results_per_page, offset))
            
            rows = cursor.fetchall()
            
            for row in rows:
                if is_postgres:
                    results.append({
                        'id': row[0],
                        'title': row[1],
                        'title_en': row[2],
                        'content': row[3],
                        'category': row[7],
                        'word_count': row[9],
                        'unique_words': row[10]
                    })
                else:
                    results.append({
                        'id': row[0],
                        'title': row[1],
                        'title_en': row[2],
                        'content': row[3],
                        'category': row[7],
                        'word_count': row[9],
                        'unique_words': row[10]
                    })
            

            if not is_category_search:
                total_occurrences = count_search_occurrences(query)
            
        finally:
            cursor.close()
            conn.close()
    
    total_pages = (total_results + results_per_page - 1) // results_per_page
    
    return render_template('search.html', 
                         query=query, 
                         results=results, 
                         total_results=total_results, 
                         total_occurrences=total_occurrences,
                         user=user,
                         page=page,
                         total_pages=total_pages,
                         is_category_search=is_category_search)

@app.route('/statistics')
@login_required
def statistics():
    """Statistics page"""
    user = get_current_user()
    

    stats = generate_statistics()
    
    return render_template('statistics.html', user=user, stats=stats, categories=CATEGORY_MAP)

@app.route('/manager/dashboard')
@manager_required
def manager_dashboard():
    """Manager dashboard"""
    user = get_current_user()
    pending_count = get_pending_count()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        is_postgres = 'postgresql' in str(conn).lower()
        

        if is_postgres:
            cursor.execute('''
                SELECT t.*, u.username 
                FROM texts t 
                LEFT JOIN users u ON t.user_id = u.id 
                WHERE t.status = 'pending' 
                ORDER BY t.date_added DESC 
                LIMIT 5
            ''')
        else:
            cursor.execute('''
                SELECT t.*, u.username 
                FROM texts t 
                LEFT JOIN users u ON t.user_id = u.id 
                WHERE t.status = 'pending' 
                ORDER BY t.date_added DESC 
                LIMIT 5
            ''')
        
        pending_texts = cursor.fetchall()
        

        if is_postgres:
            cursor.execute("SELECT COUNT(*) FROM texts WHERE status = 'approved'")
        else:
            cursor.execute("SELECT COUNT(*) FROM texts WHERE status = 'approved'")
        approved_count = cursor.fetchone()[0]
        
        if is_postgres:
            cursor.execute("SELECT COUNT(*) FROM texts WHERE status = 'rejected'")
        else:
            cursor.execute("SELECT COUNT(*) FROM texts WHERE status = 'rejected'")
        rejected_count = cursor.fetchone()[0]
        
        stats = {
            'approved': approved_count,
            'pending': pending_count,
            'rejected': rejected_count,
            'total': approved_count + pending_count + rejected_count
        }
        
    finally:
        cursor.close()
        conn.close()
    
    return render_template('manager.html', user=user, pending_texts=pending_texts, stats=stats, pending_count=pending_count)

@app.route('/manager/pending')
@manager_required
def manager_pending():
    """Manager pending review page"""
    user = get_current_user()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        is_postgres = 'postgresql' in str(conn).lower()
        

        if is_postgres:
            cursor.execute('''
                SELECT t.*, u.username 
                FROM texts t 
                LEFT JOIN users u ON t.user_id = u.id 
                WHERE t.status = 'pending' 
                ORDER BY t.date_added DESC
            ''')
        else:
            cursor.execute('''
                SELECT t.*, u.username 
                FROM texts t 
                LEFT JOIN users u ON t.user_id = u.id 
                WHERE t.status = 'pending' 
                ORDER BY t.date_added DESC
            ''')
        
        pending_texts = cursor.fetchall()
        
    finally:
        cursor.close()
        conn.close()
    
    return render_template('manager_pending.html', user=user, pending_texts=pending_texts, is_postgres=is_postgres)

@app.route('/manager/approve/<int:text_id>')
@manager_required
def manager_approve(text_id):
    """Approve a pending text"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        is_postgres = 'postgresql' in str(conn).lower()
        
        if is_postgres:
            cursor.execute("UPDATE texts SET status = 'approved' WHERE id = %s", (text_id,))
        else:
            cursor.execute("UPDATE texts SET status = 'approved' WHERE id = ?", (text_id,))
        
        conn.commit()
        flash('Text approved successfully!', 'success')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('manager_pending'))

@app.route('/manager/reject/<int:text_id>')
@manager_required
def manager_reject(text_id):
    """Reject a pending text"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        is_postgres = 'postgresql' in str(conn).lower()
        
        if is_postgres:
            cursor.execute("UPDATE texts SET status = 'rejected' WHERE id = %s", (text_id,))
        else:
            cursor.execute("UPDATE texts SET status = 'rejected' WHERE id = ?", (text_id,))
        
        conn.commit()
        flash('Text rejected.', 'info')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(url_for('manager_pending'))

@app.route('/text/<int:text_id>')
@login_required
def detail(text_id):
    """View text detail"""
    user = get_current_user()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        is_postgres = 'postgresql' in str(conn).lower()
        
        if is_postgres:
            cursor.execute('''
                SELECT t.*, u.username 
                FROM texts t 
                LEFT JOIN users u ON t.user_id = u.id 
                WHERE t.id = %s
            ''', (text_id,))
        else:
            cursor.execute('''
                SELECT t.*, u.username 
                FROM texts t 
                LEFT JOIN users u ON t.user_id = u.id 
                WHERE t.id = ?
            ''', (text_id,))
        
        row = cursor.fetchone()
        
        if not row:
            flash('Text not found.', 'error')
            return redirect(url_for('search'))
        
     
        if is_postgres:
            text_data = {
                'id': row[0],
                'title': row[1],
                'title_en': row[2],
                'content': row[3],
                'content_en': row[4],
                'full_content': row[5],
                'full_content_en': row[6],
                'category': row[7],
                'date_added': row[8].strftime('%Y-%m-%d %H:%M:%S') if row[8] else 'Unknown',
                'word_count': row[9],
                'unique_words': row[10],
                'source': row[11],
                'user_id': row[12],
                'status': row[13],
                'username': row[14] if len(row) > 14 and row[14] else 'Unknown'
            }
        else:
            text_data = {
                'id': row['id'],
                'title': row['title'],
                'title_en': row['title_en'],
                'content': row['content'],
                'content_en': row['content_en'],
                'full_content': row['full_content'],
                'full_content_en': row['full_content_en'],
                'category': row['category'],
                'date_added': row['date_added'],
                'word_count': row['word_count'],
                'unique_words': row['unique_words'],
                'source': row['source'],
                'user_id': row['user_id'],
                'status': row['status'],
                'username': row['username'] if row['username'] else 'Unknown'
            }
        
    except Exception as e:
        print(f"Error fetching text detail: {e}")
        flash('Error loading text details.', 'error')
        return redirect(url_for('search'))
    finally:
        cursor.close()
        conn.close()
    
    return render_template('detail.html', user=user, text=text_data, categories=CATEGORY_MAP)

@app.route('/logout')
@login_required
def logout():
    session.clear()
    flash('You have been logged out successfully.', 'success')
    return redirect(url_for('login'))


@app.errorhandler(404)
def not_found_error(error):
    return render_template('error.html', error='Page not found'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', error='Internal server error'), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)