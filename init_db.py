# init_db.py
import sqlite3
import os
from datetime import datetime

# Remove the existing database file if it exists to start fresh
db_path = 'corpus.db'
if os.path.exists(db_path):
    os.remove(db_path)
    print("Removed existing database to create a new one with the updated schema.")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create the main texts table with source field
cursor.execute('''
CREATE TABLE texts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    title_en TEXT NOT NULL,
    content TEXT NOT NULL,
    content_en TEXT NOT NULL,
    full_content TEXT NOT NULL,
    full_content_en TEXT NOT NULL,
    category TEXT NOT NULL,
    date_added DATE DEFAULT CURRENT_DATE,
    word_count INTEGER DEFAULT 0,
    unique_words INTEGER DEFAULT 0,
    source TEXT
)
''')

# Create table for word frequency statistics
cursor.execute('''
CREATE TABLE word_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    word TEXT NOT NULL,
    word_en TEXT NOT NULL,
    frequency INTEGER DEFAULT 0,
    category TEXT,
    UNIQUE(word, category)
)
''')

# Create table for word pairs (commonly paired words)
cursor.execute('''
CREATE TABLE word_pairs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    word1 TEXT NOT NULL,
    word2 TEXT NOT NULL,
    word1_en TEXT NOT NULL,
    word2_en TEXT NOT NULL,
    frequency INTEGER DEFAULT 0,
    category TEXT,
    UNIQUE(word1, word2, category)
)
''')

# Create table for corpus statistics
cursor.execute('''
CREATE TABLE corpus_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    total_words INTEGER DEFAULT 0,
    total_unique_words INTEGER DEFAULT 0,
    total_texts INTEGER DEFAULT 0,
    avg_word_length REAL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

# Category mapping - both English and IsiZulu names for each category
CATEGORY_MAP = {
    'izaga': {'en': 'proverbs', 'zu': 'izaga'},
    'izibongo': {'en': 'praise poetry', 'zu': 'izibongo'},
    'izisho': {'en': 'idioms', 'zu': 'izisho'},
    'philosophy': {'en': 'philosophy', 'zu': 'ifilosofi'},
    'folktale': {'en': 'folktale', 'zu': 'inganekwane'},
    'history': {'en': 'history', 'zu': 'umlando'}
}

def count_words(text):
    """Count words in a text"""
    return len(text.split())

def get_unique_words(text):
    """Get unique words from text"""
    words = text.lower().split()
    return set(words)

def extract_word_pairs(text, max_distance=3):
    """Extract word pairs that appear close to each other"""
    words = text.lower().split()
    pairs = []
    
    for i in range(len(words)):
        for j in range(i+1, min(i+max_distance+1, len(words))):
            if words[i] != words[j]:  # Don't pair same word
                pairs.append((words[i], words[j]))
    
    return pairs

# Sample data with both isiZulu and English versions
sample_data = [
    # Proverbs (Izaga)
    ("Indlela ibuzwa kwabaphambili",
     "A path is asked from those who have walked it before",
     "Isaga elikhuthaza ukulalela abanolwazi. Leli saga lifundisa ukuthi kufanele sizwe abadala abanolwazi ngempilo nangokwenza izinto.",
     "A proverb that encourages listening to those with knowledge. This proverb teaches that we should learn from elders who have knowledge about life and how to do things.",
     "Isaga elikhuthaza ukulalela abanolwazi. Leli saga lifundisa ukuthi kufanele sizwe abadala abanolwazi ngempilo nangokwenza izinto. Abadala bahamba indlela esingahamba yona futhi banolwazi olungasivikela eziphutheni.",
     "A proverb that encourages listening to those with knowledge. This proverb teaches that we should learn from elders who have knowledge about life and how to things. Elders have walked the path we are walking and have knowledge that can protect us from mistakes.",
     "izaga",
     "Traditional isiZulu wisdom passed down through generations"),

    ("Umuntu ngumuntu ngabantu",
     "A person is a person through other people",
     "Isaga elichaza ukubaluleka kobuntu. Leli saga lithi ubuntu bomuntu buphela uma ebonakala nobuntu babanye abantu.",
     "A proverb that explains the importance of humanity. This proverb says that a person's humanity is only complete when it is reflected in the humanity of others.",
     "Isaga elichaza ukubaluleka kobuntu. Leli saga lithi ubuntu bomuntu buphela uma ebonakala nobuntu babanye abantu. Asikwazi ukuziphilela sodwa, sidingana futhi sibone ubuntu babanye abantu ukuze sibe ngabantu abaphelele.",
     "A proverb that explains the importance of humanity. This proverb says that a person's humanity is only complete when it is reflected in the humanity of others. We cannot live by ourselves alone, we need each other and must see the humanity in others to be complete people.",
     "izaga",
     "Traditional isiZulu philosophy"),

    # Praise Poetry (Izibongo)
    ("Izibongo zikaShaka",
     "Praise Poetry of Shaka",
     "Izibongo zenkosi uShaka kaSenzangakhona. Ubulawu obungelanga bulawu! Wen' owadl' amanye amadoda, Wena kaMenzi owagez' esizibeni, Wena kaJama owageza ngobendlela, Wena weNdlovu eyageza emanzini aphakeme, Wena kaSenzangakhona owadela izwe!",
     "Praise poetry of King Shaka kaSenzangakhona. The magic that was not magic! You who devoured other men, You of Menzi who bathed in the pool, You of Jama who bathed in the open, You of the Elephant who bathed in deep waters, You of Senzangakhona who destroyed the nation!",
     "Izibongo zenkosi uShaka kaSenzangakhona. Ubulawu obungelanga bulawu! Wen' owadl' amanye amadoda, Wena kaMenzi owagez' esizibeni, Wena kaJama owageza ngobendlela, Wena weNdlovu eyageza emanzini aphakeme, Wena kaSenzangakhona owadela izwe! Indoda eyakha umbuso wamaZulu futhi yasishintsha isimo senhlalo yabantu bakwaZulu.",
     "Praise poetry of King Shaka kaSenzangakhona. The magic that was not magic! You who devoured other men, You of Menzi who bathed in the pool, You of Jama who bathed in the open, You of the Elephant who bathed in deep waters, You of Senzangakhona who destroyed the nation! A man who built the Zulu nation and changed the social structure of the Zulu people.",
     "izibongo",
     "Historical Zulu oral tradition"),

    # Idioms (Izisho)
    ("Ukubheka ngasekhohlo",
     "To look to the left side",
     "Isho elichaza ukungabaza noma ukungathembi. Lesi sisho sisetshenziswa uma umuntu engathembi into noma engabaza ukuthi into izophumelela.",
     "An idiom that describes doubt or distrust. This idiom is used when a person doesn't trust something or doubts that something will succeed.",
     "Isho elichaza ukungabaza noma ukungathembi. Lesi sisho sisetshenziswa uma umuntu engathembi into noma engabaza ukuthi into izophumelela. Kuthiwa ubheke ngasekhohlo uma unokungabaza.",
     "An idiom that describes doubt or distrust. This idiom is used when a person doesn't trust something or doubts that something will succeed. It is said that you look to the left when you have doubts.",
     "izisho",
     "Common isiZulu expression"),

    # Philosophy
    ("Ubuwula Bomuntu",
     "The Essence of Humanity",
     "Lena indaba yesintu echaza ubuwula bomuntu. Ifundisa isifundo esijulile sokuthi ukuziphatha kahle kuyinto ebalulekile empilweni yomuntu.",
     "This is a human story that explains the essence of humanity. It teaches a deep lesson that good behavior is important in human life.",
     "Lena indaba yesintu echaza ubuwula bomuntu. Ifundisa isifundo esijulile sokuthi ukuziphatha kahle kuyinto ebalulekile empilweni yomuntu. Indaba iqhubeka ichaza ukuthi ubuwula bomuntu buyasitshengisa ukuthi singaba njani abantu abanezinga eliphezulu lokucabanga nokuziphatha.",
     "This is a human story that explains the essence of humanity. It teaches a deep lesson that good behavior is important in human life. The story continues to explain that the essence of humanity shows us how we can be people with higher levels of thinking and behavior.",
     "philosophy",
     "Zulu philosophical teaching"),

    # Folktale
    ("Inganekwane kaNomzamo",
     "The Folktale of Nomzamo",
     "Umlando kaNomzamo ugcwele izifundo ezibalulekile ezinganeni. Abadala bathi uNomzamo wayeyindoda ebesekela umndeni wakhe.",
     "The story of Nomzamo is full of important lessons for children. Elders say that Nomzamo was a man who supported his family.",
     "Umlando kaNomzamo ugcwele izifundo ezibalulekile ezinganeni. Abadala bathi uNomzamo wayeyindoda ebesekela umndeni wakhe. Wayethanda ukusiza abantu abasweleyo. Indaba ifundisa ngobuqhawe nokuzethemba. UNomzamo wayengenayo imali kodwa wayenobuntu nobuhle bemvelo.",
     "The story of Nomzamo is full of important lessons for children. Elders say that Nomzamo was a man who supported his family. He loved to help poor people. The story teaches about bravery and self-confidence. Nomzamo didn't have money but he had humanity and natural beauty.",
     "folktale",
     "Traditional Zulu folktale"),

    # History
    ("Impi yaseIsandlwana",
     "The Battle of Isandlwana",
     "Umlando wempi yaseIsandlwana eyayimpi yokuqala yeMpi YamaZulu. Lapho amaZulu abulala amasosha amaningi aseBrithani.",
     "The history of the Battle of Isandlwana which was the first battle of the Anglo-Zulu War. Where the Zulus killed many British soldiers.",
     "Umlando wempi yaseIsandlwana eyayimpi yokuqala yeMpi YamaZulu. Lapho amaZulu abulala amasosha amaningi aseBrithani. Indaba ichaza amaqhinga ezempi asetshenziswa amaZulu. Le mpi yabonisa ubukhosi bamaZulu ngenkathi behlula ibutho laseBrithani.",
     "The history of the Battle of Isandlwana which was the first battle of the Anglo-Zulu War. Where the Zulus killed many British soldiers. The story describes the war strategies used by the Zulus. This battle showed the bravery of the Zulus when they defeated the British regiment.",
     "history",
     "Historical record of Zulu military history")
]

# Insert sample data and collect statistics
word_freq = {}
word_pairs_freq = {}
total_words = 0
total_unique_words = set()
total_texts = len(sample_data)

for title, title_en, content, content_en, full_content, full_content_en, category, source in sample_data:
    # Calculate word statistics
    zu_text = f"{title} {content} {full_content}"
    en_text = f"{title_en} {content_en} {full_content_en}"
    
    zu_words = zu_text.lower().split()
    en_words = en_text.lower().split()
    
    word_count = count_words(zu_text)
    unique_words = get_unique_words(zu_text)
    
    # Update word frequency
    for word in zu_words:
        if len(word) > 2:  # Ignore very short words
            word_freq[word] = word_freq.get(word, 0) + 1
    
    # Update word pairs
    pairs = extract_word_pairs(zu_text)
    for pair in pairs:
        if len(pair[0]) > 2 and len(pair[1]) > 2:  # Ignore very short words
            word_pairs_freq[pair] = word_pairs_freq.get(pair, 0) + 1
    
    total_words += word_count
    total_unique_words.update(unique_words)
    
    # Insert into database
    cursor.execute(
        "INSERT INTO texts (title, title_en, content, content_en, full_content, full_content_en, category, word_count, unique_words, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (title, title_en, content, content_en, full_content, full_content_en, category, word_count, len(unique_words), source)
    )

# Insert word frequency statistics
for word, freq in word_freq.items():
    # Find English equivalent (simplified - in real application would use dictionary)
    word_en = word  # Placeholder - would need proper translation
    cursor.execute(
        "INSERT OR REPLACE INTO word_stats (word, word_en, frequency) VALUES (?, ?, ?)",
        (word, word_en, freq)
    )

# Insert word pair statistics
for (word1, word2), freq in word_pairs_freq.items():
    # Find English equivalents
    word1_en = word1  # Placeholder
    word2_en = word2  # Placeholder
    cursor.execute(
        "INSERT OR REPLACE INTO word_pairs (word1, word2, word1_en, word2_en, frequency) VALUES (?, ?, ?, ?, ?)",
        (word1, word2, word1_en, word2_en, freq)
    )

# Insert corpus statistics
avg_word_length = total_words / len(total_unique_words) if total_unique_words else 0
cursor.execute(
    "INSERT INTO corpus_stats (total_words, total_unique_words, total_texts, avg_word_length) VALUES (?, ?, ?, ?)",
    (total_words, len(total_unique_words), total_texts, avg_word_length)
)

conn.commit()
conn.close()
print("Database initialized with comprehensive isiZulu-English cultural corpus.")
print(f"Total words: {total_words}")
print(f"Unique words: {len(total_unique_words)}")
print(f"Texts added: {total_texts}")
print(f"Average word length: {avg_word_length:.2f}")