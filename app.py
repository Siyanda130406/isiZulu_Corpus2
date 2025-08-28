# app.py
import os
import sqlite3
import shutil
import re
from flask import Flask, render_template, request, redirect, url_for, abort

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get('DB_PATH', os.path.join(BASE_DIR, 'corpus.db'))

# If an external DB path is configured (e.g., mounted disk) and it's missing,
# copy the repo DB there on first start.
if not os.path.exists(DB_PATH):
    src = os.path.join(BASE_DIR, 'corpus.db')
    if os.path.exists(src):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        try:
            shutil.copy(src, DB_PATH)
        except Exception as e:
            print("Could not copy corpus.db to DB_PATH:", e)

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')

# basic category list your template uses
CATEGORY_SET = {'izaga', 'izibongo', 'izisho', 'philosophy', 'folktale', 'history'}

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
    # category exact match detection
    if q.strip().lower() in CATEGORY_SET:
        parsed['category_search'] = q.strip().lower()
    return parsed

@app.route('/')
def home():
    # if you have an index.html, serve it; otherwise redirect to search page
    if os.path.exists(os.path.join(BASE_DIR, 'templates', 'index.html')):
        return render_template('index.html')
    return redirect(url_for('search'))

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

        if query:
            conn = get_db_connection()
            try:
                params = []
                where_clauses = []
                if is_category_search:
                    where_clauses.append("lower(category) = ?")
                    params.append(parsed_query['category_search'])
                else:
                    like_term = f'%{query}%'
                    where_clauses.append("(title LIKE ? OR content LIKE ? OR category LIKE ?)")
                    params.extend([like_term, like_term, like_term])

                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                # count total
                count_sql = f"SELECT COUNT(1) FROM texts {where_sql}"
                cur = conn.execute(count_sql, params)
                total_results = cur.fetchone()[0] or 0
                total_pages = (total_results + per_page - 1) // per_page if total_results else 1
                offset = (current_page - 1) * per_page
                sql = f"SELECT id, title, content, category FROM texts {where_sql} ORDER BY id DESC LIMIT ? OFFSET ?"
                cur = conn.execute(sql, params + [per_page, offset])
                rows = cur.fetchall()
                for r in rows:
                    snippet = (r['content'] or '')[:300]
                    results.append((r['id'], r['title'], snippet, r['category']))
            except Exception as e:
                print("DB search error:", e)
                results = []
                total_results = 0
                total_pages = 1
            finally:
                conn.close()

    return render_template('search.html',
                           query=query,
                           results=results,
                           total_results=total_results,
                           total_pages=total_pages,
                           current_page=current_page,
                           parsed_query=parsed_query,
                           is_category_search=is_category_search)

@app.route('/detail/<int:item_id>')
def detail(item_id):
    conn = get_db_connection()
    try:
        cur = conn.execute("SELECT id, title, content, full_content, category, date_added FROM texts WHERE id = ?", (item_id,))
        row = cur.fetchone()
        if not row:
            return ("Not found", 404)
        
        # Prepare the data in the format your template expects - as a tuple
        text = (
            row['id'],           # text[0] - ID (not used in template)
            row['title'],        # text[1] - Title
            row['content'],      # text[2] - Summary/Short content
            row['full_content'] or row['content'],  # text[3] - Full content
            row['category'],     # text[4] - Category
            row['date_added']    # text[5] - Date added
        )
        
        if os.path.exists(os.path.join(BASE_DIR, 'templates', 'detail.html')):
            return render_template('detail.html', text=text)
        # fallback simple page if template missing
        return f"<h1>{text[1]}</h1><p>{text[3]}</p><p><strong>Category:</strong> {text[4]}</p>"
    except Exception as e:
        print("Error in detail view:", e)
        return ("Error loading content", 500)
    finally:
        conn.close()

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