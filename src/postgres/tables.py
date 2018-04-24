from src.core.tables import Table, MigrationTable, Intersection

class PostgresTable(Table):

    def add_sequences(self, statement):
        sequences = [x for x in statement.split("'") if x.endswith('_seq')]
        for seq in sequences:
            self.db.add_sequence(seq)


    def get_column_definition(self, column_name):
        """Get the sql column definition
           Selects the column type, and YES or NO from the column, IS NULLABLE.
           That's enough information to re-create the column.
        """
        sql = self.commands.column_definition(self.db.name, self.name, column_name)
        ans = self.execute(sql)[0]
        char_def = ans[0]
        if ans[1]:
            char_def = '{}({})'.format(char_def, ans[1])
        if ans[2] == 'NO':
            char_def = '{} NOT NULL'.format(char_def)
        if ans[3]:
            char_def = '{} default {}'.format(char_def, ans[3])
        return char_def
