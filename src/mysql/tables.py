import time
import re
from src.core.tables import Table, MigrationTable
from src.core.constraints import Index


class MysqlTable(Table):

    @staticmethod
    def _join_cols(cols):
        '''Join and escape a list'''
        return ', '.join(['`%s`' % i for i in cols])

    @staticmethod
    def _join_conditionals(row_dict):
        '''Create a joined conditional statement for updates
        return escaped string of `key`=val, `key`='val' for dictionary
        '''
        equalities = []
        for key, val in row_dict.items():
            temp = '`{}`='.format(key)
            if isinstance(val, (int, float)):
                temp += '{}'.format(val)
            elif isinstance(val, str):
                temp += '\'{}\''.format(val)
            else:
                raise TypeError('Value %s, type %s not recognised as a number or string' % (val, type(val)))
            equalities.append(temp)
        return ', '.join(equalities)

    @staticmethod
    def _qualify(table, cols):
        '''Qualify, join and escape the list'''
        return ', '.join(['`{}`.`{}`'.format(table, c) for c in cols])

    @staticmethod
    def _equals(cols, new_table, new_cols):
        '''Qualify, join and equate'''
        return ', '.join('`{}`=`{}`.`{}`'.format(cols[i], new_table, new_cols[i]) for i in range(len(cols)))

    def insert_row(self, row_dict):
        """Add a row to the table"""
        sql = self.commands.insert_row(
                self.name,
                self._join_cols(row_dict.keys()),
                self._join_values(row_dict.values())
            )
        self.execute(sql)
        return self.db.last_row

    def get_column_definition(self, column_name):
        '''Get the sql column definition
           Selects the column type, and YES or NO from the column, IS NULLABLE.
           That's enough information to re-create the column.
        '''
        sql = self.commands.column_definition(self.db.name, self.name, column_name)
        ans = self.execute(sql)[0]
        if ans[1] == 'NO':
            return '{} NOT NULL'.format(ans[0])
        else:
            return ans[0]


    def rename_column(self, old_name, new_name):
        '''Rename a column'''
        self.execute(self.commands.rename_column(
            self.name,
            old_name,
            new_name,
            self.get_column_definition(old_name))
        )

    @property
    def create_statement(self):
        """Get table create statement"""
        query = self.commands.get_table_create_statement(self.name)
        if self.db.table_exists(self.name):
            statement = self.execute(query)[0][1]
            statement = re.sub('\s+', ' ', statement)
            return statement
        raise ValueError('Table does not exist, no create statement')


    @property
    def indexes(self):
        """Return list of indexes"""
        indexes = self.execute(self.commands.get_indexes(self.name))
        return [Index(tup[0], tup[2], tup[1], tup[4]) for tup in indexes]


class MySqlMigrationTable(MysqlTable, MigrationTable):

    def create_from_source(self):
        """Create new table like source_table"""
        create_statement = self.source.create_statement.replace(
            'CREATE TABLE `{}`'.format(self.source.name),
            'CREATE TABLE `{}`'
        )
        self.create_from_statement(create_statement)

    def _trigger_name(self, method_type):
        'Create trigger name'
        name = 'migration_trigger_{}_{}'.format(method_type, self.source.name)
        return name[:self.db.config['MAX_LENGTH_NAME']]

    def create_insert_trigger(self):
        '''Set insert Triggers.
        'NEW' and 'OLD' are mysql references
        see https://dev.mysql.com/doc/refman/5.0/en/create-trigger.html
        '''
        sql = self.commands.insert_trigger(
            self._trigger_name('insert'),
            self.source.name,
            self.name,
            self._join_cols(self.intersection.dest_columns),
            self._qualify('NEW', self.intersection.origin_columns))
        self.execute(sql)

    def create_delete_trigger(self):
        '''Set delete triggers
        'NEW' and 'OLD' are mysql references
        see https://dev.mysql.com/doc/refman/5.0/en/create-trigger.html
        '''
        sql = self.commands.delete_trigger(
            self._trigger_name('delete'),
            self.source.name,
            self.name,
            self.primary_key_column)

        self.execute(sql)

    def create_update_trigger(self):
        '''Set update triggers
        'NEW' and 'OLD' are mysql references
        see https://dev.mysql.com/doc/refman/5.0/en/create-trigger.html
        '''
        sql = self.commands.update_trigger(
            self._trigger_name('update'),
            self.source.name,
            self.name,
            self._equals(self.intersection.dest_columns, 'NEW', self.intersection.origin_columns),
            self.primary_key_column
        )
        self.execute(sql)

    def rename_tables(self):
        'Rename the tables'
        self.delete_triggers()
        retries = 0
        source_name, archive_name, migrate_name = self.source.name, self.source.archive_name, self.name
        while True:
            try:
                self.execute(self.commands.rename_table(source_name, archive_name, migrate_name))
                break
            except Exception as e:
                retries += 1
                if retries > self.db.config['MAX_RENAME_RETRIES']:
                    self.create_triggers()
                    return False
                # TODO: make sure this is a Lock wait timeout error before retrying
                print('Rename retry %d, error: %s' % (retries, e))
                time.sleep(self.db.donfig['RETRY_SLEEP_TIME'])
        self.name, self.source.name = self.source.name, self.archive_name
        print("Rename complete!")
        return True
