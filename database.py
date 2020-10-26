import sqlite3

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
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS word_counts (
            word_text VARCHAR[256] NOT NULL,
            count INT NOT NULL,
            PRIMARY KEY (word_text)
        )''')
    
    def clear_db(self):
        if not self._conn:
            print("No Database Connection")
            return
        
        c = self._conn.cursor()
        c.execute(''' DROP TABLE visited_urls;
                    DROP TABLE word_counts;''')
        self._conn.commit()

    def upsert_word_counts(self, freqs):
        if not self._conn:
            print("No Database Connection")
            return
        try:
            c = self._conn.cursor()
            c.executemany(''' INSERT INTO word_counts(word_text, count)
                                VALUES(?, ?) 
                                ON CONFLICT(word_text)
                                DO UPDATE SET count=count+excluded.count''', list(freqs.items()))
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
        print(c.fetchall())

        
