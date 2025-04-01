class Database:
    def __init__(self, host, db_name, port, user=None, password=None):
        self.host = host
        self.db_name = db_name
        self.port = port
        self.user = user
        self.password = password

    def __repr__(self):
        return f"Database(host={self.host}, db_name={self.db_name}, port={self.port}, user={self.user})"