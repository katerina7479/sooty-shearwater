"""Migration tool"""

from src.postgres.base import PostgresDatabase
from src.mysql.base import MySqlDatabase


CONFIG = {
    "DEFAULT_CHUNK_SIZE": 10000,
    "DEFAULT_THROTTLE": 0.1,
    "MAX_LENGTH_NAME": 60,
    "MAX_RENAME_RETRIES": 10,
    "RETRY_SLEEP_TIME": 10,
    "DIALECT": 'postgres'
}


class DatabaseFactory(object):
    """Model representing a database"""

    def __init__(self, name, connection, config=CONFIG):
        """Initialize the database"""
        self.name = name
        self.connection = connection
        self.config = config
        self.POSTGRES, self.MYSQL = False, False
        if self.config['DIALECT'] == 'postgres':
            self.POSTGRES = True
        elif self.config['DIALECT'] == 'mysql':
            self.MYSQL = True
        else:
            raise Exception('Database dialect %s not supported' % self.config['DIALECT'])

    def fetch(self):
        if self.POSTGRES:
            return PostgresDatabase(self.name, self.connection, self.config)
        if self.MYSQL:
            return MySqlDatabase(self.name, self.connection, self.config)
