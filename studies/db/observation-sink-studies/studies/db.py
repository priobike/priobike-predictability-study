import psycopg2

class DBClient:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="priobike-sentry.inf.tu-dresden.de",
            port=443,
            database="observations",
            user="postgres",
            password="Et7RvZ4TjEBHRF")
        self.cursor = self.conn.cursor()
        
    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
        
    def close(self):
        self.cursor.close()
        self.conn.close()
        