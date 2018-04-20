"""Migration tool"""

CONFIG = {
    "DEFAULT_CHUNK_SIZE": 1000,
    "DEFAULT_THROTTLE": 0.1,
    "DEFAULT_MAX_LENGTH_NAME": 60
}


class DatabaseFactory(object):
    """Model representing a database"""

    def __init__(self, dialect, connection, config=CONFIG):
        """Initialize the database"""
        self.dialect = dialect
        self.connection = connection
        self.config = config

    def fetch(self):
        if self.dialect == 'postgres':
            from postgres.base import PostgresDatabase
            return PostgresDatabase(self.connection, self.config)
        elif self.dialect == 'mysql':
            from mysql.base import MysqlDatabase
            return MysqlDatabase(self.connection, self.config)
        else:
            raise Exception('Database dialect %s not supported')
