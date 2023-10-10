import psycopg2

class DBClient:
    def __init__(self, window_size: int):
        self.conn = psycopg2.connect(
            host="priobike-sentry.inf.tu-dresden.de",
            port=443,
            database="observations",
            user="postgres",
            password="Et7RvZ4TjEBHRF")
        self.cursor = self.conn.cursor(name="studies_cursor")
        # Limit the number of rows fetched from the database to save memory on the client
        self.cursor.itersize = window_size
        
    def execute_query(self, query):
        self.cursor.execute(query)
        # Returns a generator object
        return self.cursor
        
    def close(self):
        self.cursor.close()
        self.conn.close()
        