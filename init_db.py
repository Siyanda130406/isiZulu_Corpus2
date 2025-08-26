import sqlite3

conn = sqlite3.connect('corpus.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS texts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    full_content TEXT NOT NULL,
    category TEXT NOT NULL,
    date_added DATE DEFAULT CURRENT_DATE
)
''')

sample_data = [
    ("Ubuwula Bomuntu",
     "Lena indaba yesintu echaza ubuwula bomuntu...",
     "Lena indaba yesintu echaza ubuwula bomuntu. Ifundisa isifundo esijulile sokuthi ukuziphatha kahle kuyinto ebalulekile empilweni yomuntu. Indaba iqhubeka ichaza ukuthi ubuwula bomuntu buyasitshengisa ukuthi singaba njani abantu abanezinga eliphezulu lokucabanga nokuziphatha. Lokhu kusitshela ukuthi silinganisise izenzo zethu nsuku zonke.",
     "Philosophy"),
    ("Ubuntu Nesintu",
     "Indaba echaza ubudlelwano phakathi kobuntu nesintu...",
     "Indaba echaza ubudlelwano phakathi kobuntu nesintu. Ubuntu ngumgogodla wesintu esimnyama. Asisiko isimo somuntu esibonakalayo kuphela, kodwa isimo somuntu ngokomoya. Umuntu ngumuntu ngabantu. Lokhu kusho ukuthi ubuntu bethu buphela uma sibonakala nobuntu babanye abantu.",
     "Philosophy"),

    ("Izibongo zikaShaka",
     "Izibongo zenkosi uShaka kaSenzangakhona...",
     "Izibongo zenkosi uShaka kaSenzangakhona. Ubulawu obungelanga bulawu! Wen' owadl' amanye amadoda, Wena kaMenzi owagez' esizibeni, Wena kaJama owageza ngobendlela, Wena weNdlovu eyageza emanzini aphakeme, Wena kaSenzangakhona owadela izwe!",
     "Praise Poetry"),
    ("Izibongo zikaCetshwayo",
     "Izibongo zenkosi uCetshwayo kaMpande...",
     "Izibongo zenkosi uCetshwayo kaMpande. Indlovu eyayibusa ngamandla! Wena weSilo esimnyama, Wena kaMpande owadela izwe, Wena weNdlovu engenasici, Wena owavimbezela amabutho, Wena owagcina isizwe samaZulu.",
     "Praise Poetry"),

    ("Indlela ibuzwa kwabaphambili",
     "Isaga elikhuthaza ukulalela abanolwazi...",
     "Isaga elikhuthaza ukulalela abanolwazi. Leli saga lifundisa ukuthi kufanele sizwe abadala abanolwazi ngempilo nangokwenza izinto. Abadala bahamba indlela esingahamba yona futhi banolwazi olungasivikela eziphutheni.",
     "Proverbs"),
    ("Umuntu ngumuntu ngabantu",
     "Isaga elichaza ukubaluleka kobuntu...",
     "Isaga elichaza ukubaluleka kobuntu. Leli saga lithi ubuntu bomuntu buphela uma ebonakala nobuntu babanye abantu. Asikwazi ukuziphilela sodwa, sidingana futhi sibone ubuntu babanye abantu ukuze sibe ngabantu abaphelele.",
     "Proverbs"),

    ("Ukubheka ngasekhohlo",
     "Isho elichaza ukungabaza noma ukungathembi...",
     "Isho elichaza ukungabaza noma ukungathembi. Lesi sisho sisetshenziswa uma umuntu engathembi into noma engabaza ukuthi into izophumelela. Kuthiwa ubheke ngasekhohlo uma unokungabaza.",
     "Idioms"),
    ("Uthini igwala libheke",
     "Isho elichaza ukwesaba okungapheli...",
     "Isho elichaza ukwesaba okungapheli. Lesi sisho sisetshenziswa kumuntu oyesaba kakhulu, ophila ngokwesaba izinto ezingenzeki. Igwala libheka noma yini futhi liyesabe yonke into.",
     "Idioms"),

    ("Inganekwane kaNomzamo",
     "Umlando kaNomzamo ugcwele izifundo ezibalulekile ezinganeni...",
     "Umlando kaNomzamo ugcwele izifundo ezibalulekile ezinganeni. ... Indaba ifundisa ngobuqhawe nokuzethemba.",
     "Folktale"),
    ("UDelile noCilo",
     "Inganekwane yothando lukaDelile noCilo...",
     "Inganekwane yothando lukaDelile noCilo. ... Indaba ifundisa ngokuzimisela nothando.",
     "Folktale"),

    ("Umlando kaZulu",
     "Lena yindaba emayelana nomlando wesizwe samaZulu...",
     "Lena yindaba emayelana nomlando wesizwe samaZulu ... Iqukethe imibiko yempi, amaqhawe, kanye nenkambiso.",
     "History"),
    ("Impi yaseIsandlwana",
     "Umlando wempi yaseIsandlwana eyayimpi yokuqala yeMpi YamaZulu...",
     "Umlando wempi yaseIsandlwana ... Indaba ichaza amaqhinga ezempi asetshenziswa amaZulu.",
     "History")
]

cursor.executemany(
    "INSERT INTO texts (title, content, full_content, category) VALUES (?, ?, ?, ?)",
    sample_data
)

conn.commit()
conn.close()
print("Database initialized with enhanced sample data across multiple categories.")
