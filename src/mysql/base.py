from src.core.base import Database
from src.mysql.commands import MySqlCommands
from src.mysql.tables import MysqlTable, MySqlMigrationTable


class MySqlDatabase(Database):
    '''Model representing a MySql database'''

    def __init__(self, name, connection, config):
        '''Initialize the database'''
        super(MySqlDatabase, self).__init__(name, connection, config)
        self.commands = MySqlCommands
        self.table_class = MysqlTable
        self.migration_table_class = MySqlMigrationTable

    def set_foreign_key_checks(self, state=True):
        '''Set foreign key checks on database'''
        self.execute(self.commands.set_foreign_key_checks(state))
