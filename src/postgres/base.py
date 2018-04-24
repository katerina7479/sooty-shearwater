from src.core.base import Database
from src.postgres.commands import PostgresCommands
from src.postgres.tables import PostgresTable


class PostgresDatabase(Database):

    def __init__(self, name, connection, config):
        '''Initialize the database'''
        super(PostgresDatabase, self).__init__(name, connection, config)
        self.commands = PostgresCommands
        self.add_show_create_table()
        self.table_class = PostgresTable


    def __del__(self):
        self.drop_show_create_table()

    def add_show_create_table(self):
        """
        Useful create statement function added to postgres
        from http://stackoverflow.com/questions/2593803/how-to-generate-the-create-table-sql-statement-for-an-existing-table-in-postgr
        """
        self.execute(self.commands.show_table_function)

    def drop_show_create_table(self):
        self.execute(self.commands.drop_show_create_table)

    @property
    def sequences(self):
        sql = self.commands.get_database_sequences(self.name)
        return [x[0] for x in self.execute(sql)]


    def add_sequence(self, name):
        if name not in self.sequences:
            self.execute(self.commands.create_sequence(name))
