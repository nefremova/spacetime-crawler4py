import sqlite3

class Database:
    def __init__(self, db_name):
        self._db_name = db_name
    
    def connect(self):
        self._conn = sqlite3.connect(self.db_name)
    
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
            PRIMARY KEY (domain, subdomain, path)
        )''')
    
    def 