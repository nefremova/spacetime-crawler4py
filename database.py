import sqlite3

'''
    DOMAIN IDS:
        1: *.ics.uci.edu/*
        2: *.cs.uci.edu/*
        3: *.informatics.uci.edu/*
        4: *.stat.uci.edu/*    
        5: today.uci.edu/department/information_computer_sciences/*
'''
class Database:
    def __init__(self, db_name):
        self._db_name = db_name
    
    def connect(self):
        self._conn = sqlite3.connect(self._db_name)
    
    def close_connection(self):
        if self._conn:
            self._conn.close()
        else:
             print("No Connection to Close.")
    
    def create_db(self):
        if not self._conn:
            print("No Database Connection")
            return

        c = self._conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS visited_urls (
            domain_id INT NOT NULL,
            subdomain VARCHAR[256] NOT NULL,
            path VARCHAR[MAX] NOT NULL,
            PRIMARY KEY (domain_id, subdomain, path)
        );''')

        c.execute('''CREATE TABLE IF NOT EXISTS word_counts (
            word_text VARCHAR[256] NOT NULL,
            count INT NOT NULL,
            PRIMARY KEY (word_text)
        );''')


    def clear_db(self):
        if not self._conn:
            print("No Database Connection")
            return
        
        c = self._conn.cursor()
        c.execute(''' DROP TABLE IF EXISTS visited_urls''')
        c.execute('''DROP TABLE IF EXISTS word_counts''')
        self._conn.commit()

    def upsert_word_counts(self, freqs):
        if not self._conn:
            print("No Database Connection")
            return
        try:
            c = self._conn.cursor()
            c.executemany(''' INSERT OR REPLACE INTO word_counts (word_text, count)
                                VALUES (
                                ?1,
                                COALESCE((SELECT count + ?2 FROM word_counts WHERE word_text = ?1), ?2)
                              );''', list(freqs.items()))
        except Exception as err:
            print(err)

        self._conn.commit()

    def insert_urls(self, urls):
        if not self._conn:
            print("No Database Connection")
            return
        try:
            c = self._conn.cursor()
            c.executemany(''' INSERT OR IGNORE INTO visited_urls(domain_id, subdomain, path)
                                VALUES(?, ?, ?) ''', urls)
        except Exception as err:
            print(err)

        self._conn.commit()

    def url_exists(self, url):
        if not self._conn:
            print("No Database Connection")
            return
        c = self._conn.cursor()
        c.execute('''SELECT EXISTS (SELECT 1 FROM visited_urls WHERE domain_id = ? AND subdomain = ? AND path = ? ); ''', url)
        return True if c.fetchall() else False

    def get_word_counts(self):
        if not self._conn:
            print("No Database Connection")
            return
        
        c = self._conn.cursor()
        c.execute(' SELECT * FROM word_counts ')
        print(c.fetchall())
    
    def get_visited_urls(self):
        if not self._conn:
            print("No Database Connection")
            return
        
        c = self._conn.cursor()
        c.execute(' SELECT * FROM visited_urls ')
        #print(c.fetchall())

        return c.fetchall()


