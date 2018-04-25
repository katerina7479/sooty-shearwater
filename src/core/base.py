"""Migration tool"""
from src.core.tables import Table, MigrationTable


class Database(object):
    """Model representing a database"""

    def __init__(self, name, connection, config):
        """Initialize the database"""
        self.name = name
        self.connection = connection
        self.config = config
        self.commands = None
        self.table_class = Table
        self.migration_table_class = MigrationTable
        self.last_row = None

    def commit(self):
        self.connection.commit()

    def execute(self, sql):
        """Execute a query against the database. Returns empty tuple if no result"""
        with self.connection.cursor() as dbc:
            if sql[-1] != ';':
                sql += ';'
            dbc.execute(sql)
            self.last_row = dbc.lastrowid
            try:
                return dbc.fetchall()
            except:
                return

    def batch_execute(self, sql_list):
        """Execute a list of sql statements"""
        with self.connection.cursor() as dbc:
            responses = []
            for sql in sql_list:
                dbc.execute(sql)
                responses.append(dbc.fetchall())
            return responses

    @property
    def tables(self):
        """Get a list of non-system database table names"""
        result = self.execute(self.commands.get_tables(self.name))
        return [x[0] for x in result]

    def table_exists(self, table_name):
        """Check if table exists in database"""
        return table_name in self.tables

    def table(self, tablename, primary_key_column='id'):
        return self.table_class(database=self, name=tablename, primary_key_column=primary_key_column)

    def migration_table(self, source_table):
        return self.migration_table_class(database=self, source_table=source_table, primary_key_column=source_table.primary_key_column)
