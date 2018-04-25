import datetime
import re
import time
import random
import string
from src.core.constraints import Constraint, ForeignKey, Index


class Table(object):
    """
    Represents a table in a database
    """

    def __init__(self, database, name, primary_key_column='id'):
        """Initialize the table with database object and name"""
        self.db = database
        self.commands = database.commands
        self.name = name
        self.primary_key_column = primary_key_column

    @staticmethod
    def _join_cols(cols):
        return ', '.join(cols)

    @staticmethod
    def _join_values(vals):
        '''For values, create a join by type'''
        escaped = []
        for val in vals:
            if isinstance(val, (int, float)):
                escaped.append('{}'.format(val))
            elif isinstance(val, str):
                escaped.append("'{}'".format(val.replace("'", "''")))
            else:
                raise TypeError('Value %s, type %s not recognised as a number or string' % (val, type(val)))
        return ', '.join(escaped)

    @staticmethod
    def _join_batch_rows(rows):
        insert = ''
        for i, row in enumerate(rows):
            insert += '({}), '.format(Table._join_values(row))
        return insert[:-2]

    @staticmethod
    def _join_equality(row_dict):
        """Create a joined conditional statement for updates
        return escaped string of key=val, key='val' for dictionary
        """
        equalities = []
        for key, val in row_dict.items():
            temp = '{}='.format(key)
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
        """Qualify, join and escape the list"""
        return ', '.join(['{}.{}'.format(table, c) for c in cols])

    @staticmethod
    def _equals(cols, new_table, new_cols):
        """Qualify, join and equate"""
        return ', '.join('{}={}.{}'.format(cols[i], new_table, new_cols[i]) for i in range(len(cols)))

    @staticmethod
    def _dictify(cols, vals):
        """Return columns, values as dictionary"""
        assert (len(cols) == len(vals)), 'Columns and values must be the same length to map to dictionary'
        return {col: vals[i] for i, col in enumerate(cols)}

    @staticmethod
    def _random_string(length):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

    # Table Methods
    def execute(self, sql):
        """Execute a single sql statement"""
        return self.db.execute(sql)

    def commit(self):
        return self.db.commit()

    def create(self):
        """
        Create an initial table, with an incrementing primary key
        """
        self.execute(self.commands.create_table(self.name, self.primary_key_column))
        return self.commit()

    def add_sequences(self, statement):
        pass

    @property
    def create_statement(self):
        """Get table create statement"""
        query = self.commands.get_table_create_statement(self.name)

        if self.db.table_exists(self.name):
            statement = self.execute(query)[0][0]
            statement = re.sub('\s+', ' ', statement)
            return statement
        raise ValueError('Table does not exist, no create statement')

    def create_from_statement(self, statement):
        """Create a table from the create statement"""
        if not self.db.table_exists(self.name):
            self.add_sequences(statement)
            self.execute(statement.format(self.name))
            self.commit()

    def drop(self, cascade=False):
        """Delete table from database"""
        if self.db.table_exists(self.name):
            self.drop_foreign_keys()
            self.execute(self.commands.drop_table(self.name, cascade))
            self.commit()

    # Row Methods
    def get_row(self, pk):
        """Get row information, return as a dictionary"""
        ans = self.execute(self.commands.get_row(
            cols=self._join_cols(self.columns),
            table=self.name,
            pk_col=self.primary_key_column,
            pk=pk
        ))
        if not ans:
            return None
        return self._dictify(self.columns, ans[0])

    def insert_row(self, row_dict):
        """Add a row to the table"""
        sql = self.commands.insert_row(
                self.name,
                self._join_cols(row_dict.keys()),
                self._join_values(row_dict.values())
            )
        return self.execute(sql)[0][0]

    def update_row(self, pk, row_dict):
        """Update a row in the table"""
        return self.execute(self.commands.update_row(
            self.name,
            col_val=self._join_equality(row_dict),
            pk_col=self.primary_key_column,
            pk=pk
        ))

    def delete_row(self, pk):
        """Delete a row by pk"""
        return self.execute(self.commands.delete_row(self.name, self.primary_key_column, pk))

    @property
    def count(self):
        """Get the count for the table"""
        ans = self.execute(self.commands.table_count(self.name))
        return ans[0][0]

    # Column Methods
    @property
    def columns(self):
        """Return list of column names"""
        result = self.execute(self.commands.table_columns(self.name))
        return [x[0] for x in result]

    def column_exists(self, column_name):
        """Test for column exists"""
        return column_name in self.columns

    def get_column_definition(self, column_name):
        """Get the sql column definition
           Selects the column type, and YES or NO from the column, IS NULLABLE.
           That's enough information to re-create the column.
        """
        raise NotImplementedError('Column definition not implemented')

    def add_column(self, col_name, definition):
        """Add column to table"""
        if not self.column_exists(col_name):
            self.execute(self.commands.add_column(self.name, col_name, definition))

    def alter_column(self, col_name, definition):
        """Alter column"""
        self.execute(self.commands.alter_column(self.name, col_name, definition))

    def drop_column(self, col_name):
        """Delete column"""
        self.execute(self.commands.drop_column(self.name, col_name))

    def rename_column(self, old_name, new_name):
        """Rename a column"""
        self.execute(self.commands.rename_column(self.name, old_name, new_name))

    # Constraints
    @property
    def constraints(self):
        """Get the constraints on the table"""
        ans = self.execute(self.commands.get_constraints(self.name))
        return [Constraint(*tup) for tup in ans]

    @property
    def primary_key(self):
        return [x for x in self.constraints if x.type == 'PRIMARY KEY'][0]

    def add_constraints(self, constraints):
        """Add constraints from constraint objects"""
        for const in constraints:
            self.add_constraint(const.type, const.column, const.check_clause)

    def add_constraint(self, type, column, check_clause=None):
        """Add a non-foreign key database constraint"""
        if type == 'CHECK' and check_clause and 'NOT NULL' in check_clause:
            sql = self.commands.add_check_not_null(self.name, check_clause.split(' ')[0])
        elif type == 'CHECK':
            if check_clause and 'VALUE' in check_clause and column:
                check_clause.replace('VALUE', column)
            sql = self.commands.add_check(self.name, check_clause)
        elif type in ['UNIQUE', 'PRIMARY KEY']:
            constraint_name = self.new_constraint_name(column, type)
            sql = self.commands.add_constraint(self.name, constraint_name, type, column)
        else:
            raise Exception('Invalid constraint parameters')
        try:
            self.execute(sql)
        except Exception as e:
            print('Unable to add constraint: {}'.format(e))
        self.commit()

    def drop_constraint(self, name):
        self.execute(self.commands.drop_constraint(self.name, name))
        self.commit()

    # Foreign Keys
    @property
    def foreign_keys(self):
        """Return list of foreign_key constraints"""
        ans = self.execute(self.commands.foreign_keys(self.db.name, self.name))
        return [ForeignKey(*tup) for tup in ans]

    def get_foreign_key(self, name):
        """Return foreign key object by name, or None if not found"""
        for fk in self.foreign_keys:
            if fk.name == name:
                return fk
        return None

    def check_foreign_key_exists(self, table_name, column_name, referenced_table, referenced_column):
        """Check to see if the foreign key already exists"""
        ans = self.execute(self.commands.foreign_key_exists(self.db.name, table_name, column_name, referenced_table, referenced_column))
        if not ans:
            return False
        return True

    def add_foreign_keys(self, foreign_keys, override_table=None):
        """Applies foreign key objects to table. If table_name is specified, the foreign key constraints are
        copied to that table instead of being added to the original owner."""
        if not override_table:
            override_table = self.name

        for key in foreign_keys:
            # If the key is a referenced, then we want to set the referenced table to the override table
            if key.self_referential:
                table = override_table
                foreign_table = override_table
            elif key.referenced:
                table = key.table_name
                foreign_table = override_table
            else:
                table = override_table
                foreign_table = key.fk_table_name
            self.add_foreign_key(table, key.column_name, foreign_table, key.fk_column)

    def add_foreign_key(self, table_name, column, fk_table, fk_column, name=None):
        """Create a foreign key constraint, catch Integrity Errors"""
        if not table_name:
            table_name = self.name
        if not name:
            name = self.new_fk_index_name(column, fk_column)
        try:
            self.execute(self.commands.add_foreign_key(
                table_name, name, column, fk_table, fk_column
            ))
            self.commit()
        except Exception as e:
            print('Cannot add fk Integrity Error: {}'.format(e))

    def drop_foreign_keys(self):
        """Drops the table's foreign keys"""
        for key in self.foreign_keys:
            try:
                self.drop_foreign_key(key.table_name, key.name)
            except:
                # Dropping items that are not there
                print('Tried to drop key {}, did not exist'.format(key.name))

    def drop_foreign_key(self, fk_table_name, fk_name):
        """Drop a foreign key constraint"""
        self.execute(self.commands.drop_foreign_key(fk_table_name, fk_name))
        self.commit()

    # Indexes
    @property
    def indexes(self):
        """Return list of indexes"""
        indexes = self.execute(self.commands.get_indexes(self.name))
        return [Index(*tup) for tup in indexes]

    def get_index(self, name):
        """Return index object by name or None if not found"""
        for index in self.indexes:
            if index.name == name:
                return index
        return None

    def add_indexes(self, indexes):
        for index in indexes:
            if not index.unique:
                self.add_index([index.column])

    def add_index(self, column_list, name=None, unique=False):
        """Add an index to the table"""
        columns = self._join_cols(column_list)
        if not name:
            name = self.new_index_name(columns, unique)

        self.execute(self.commands.add_index(self.name, name, columns, unique))
        self.commit()

    def drop_index(self, index_name):
        """Drop an index from the table"""
        self.execute(self.commands.drop_index(index_name))

    # Naming
    def new_fk_index_name(self, column, fk_column):
        """Create a new foreign key index name"""
        return '{}_refs_{}_{}'.format(column, fk_column, self._random_string(8))

    def new_constraint_name(self, column, type):
        """Create a new unique constraint name"""
        name = self.name.lstrip('migrate_')[:30]
        if type == 'UNIQUE':
            return '{}_{}_{}_uniq'.format(name, column[:15], self._random_string(8))
        elif type == 'PRIMARY KEY':
            return '{}_{}_pkey'.format(name, self._random_string(4))
        else:
            raise NotImplementedError('Name not implemented for type {}'.format(type))

    def new_index_name(self, column, unique=False):
        """Return a name for an index"""
        table_name = self.name.lstrip('migrate_')
        column = column.replace(', ', '')
        return '{}_{}_{}{}'.format(table_name, column, self._random_string(6), '_unique' if unique else '')

    @property
    def migrate_name(self):
        return 'migrate_{}'.format(self.name)

    @property
    def archive_name(self):
        return 'archive_{}'.format(self.name)

    # Min and Max ids
    @property
    def min_pk(self):
        """Return the minimum id for the table rows"""
        start = self.execute(self.commands.min_pk(self.name, self.primary_key_column))
        return start[0][0]

    @property
    def max_pk(self):
        """Return the maximum id for the table rows"""
        end = self.execute(self.commands.max_pk(self.name, self.primary_key_column))
        return end[0][0]

    # Table Triggers
    def get_triggers(self, table_name=None):
        """Get triggers on table"""
        if not table_name:
            table_name = self.name

        triggers = self.execute(self.commands.get_triggers(self.db.name, table_name))
        return [x[0] for x in triggers]

    # get sequences
    @property
    def sequence_cols(self):
        ans = self.execute(self.commands.get_sequences(self.name))
        return ans

    def remove_sequence_from_col(self, column):
        self.execute(self.commands.remove_sequence_from_col(self.name, column))
        self.commit()

    def set_sequence_owner(self, name, table, col):
        self.execute(self.commands.set_sequence_owner(
            name,
            table,
            col
        ))
        self.commit()


class MigrationTable(Table):
    """Represents the new table with changes"""

    def __init__(self, database, source_table, primary_key_column='id'):
        """Initialize table with parent"""
        self.source = source_table
        super(MigrationTable, self).__init__(database, self.source.migrate_name, primary_key_column)
        self.renames = []
        self.triggers = {}
        for type in ['INSERT', 'UPDATE', 'DELETE']:
            self.triggers[type] = self._trigger_name(type)

    def create_from_source(self):
        """Create new table like source_table"""
        create_statement = self.source.create_statement
        self.create_from_statement(create_statement)
        # Add constraints
        constraints = self.source.constraints
        self.add_constraints(constraints)

        # Add indexes
        indexes = self.source.indexes
        self.add_indexes(indexes)

        # Add the non-referenced foreign keys
        non_referenced_fks = [x for x in self.source.foreign_keys if not x.referenced]
        self.add_foreign_keys(non_referenced_fks, override_table=self.name)

    def rename_column(self, original_column_name, new_column_name):
        """Map renamed columns across tables"""
        self.renames.append((original_column_name, new_column_name))
        if not self.column_exists(new_column_name):
            super(MigrationTable, self).rename_column(original_column_name, new_column_name)

    @property
    def intersection(self):
        """Returns an intersection object"""
        return Intersection(self.source, self)

    # Table Triggers
    def get_source_triggers(self):
        """Get triggers on source table"""
        return self.get_triggers(self.source.name)

    def create_triggers(self):
        """create triggers for source table"""
        triggers = self.get_source_triggers()
        if not triggers:
            self.create_insert_trigger()
            self.create_update_trigger()
            self.create_delete_trigger()
            self.commit()

    def create_insert_trigger(self):
        """Set insert Triggers.
        'NEW' and 'OLD' are sql references
        see https://www.postgresql.org/docs/9.2/static/plpgsql-trigger.html
        """
        self.execute(self.commands.insert_function(
                    self.name,
                    cols=self._join_cols(self.intersection.dest_columns),
                    vals=self._qualify('NEW', self.intersection.origin_columns)
        ))

        self.execute(self.commands.insert_trigger(
            self.triggers['INSERT'],
            self.source.name,
            self.name
        ))

    def create_update_trigger(self):
        """Set update triggers
        'NEW' and 'OLD' are sql references
        see https://www.postgresql.org/docs/9.2/static/plpgsql-trigger.html
        """
        self.execute(self.commands.update_function(
            self.name,
            self._equals(
                self.intersection.dest_columns,
                'NEW',
                self.intersection.origin_columns
            ),
            self.primary_key_column
        ))

        self.execute(self.commands.update_trigger(
            self.triggers['UPDATE'],
            self.source.name,
            self.name
        ))

    def create_delete_trigger(self):
        """Set delete triggers
        'NEW' and 'OLD' are sql references
        see https://www.postgresql.org/docs/9.2/static/plpgsql-trigger.html
        """
        self.execute(self.commands.delete_function(
            dest_table=self.name,
            pk_col=self.primary_key_column
        ))

        self.execute(self.commands.delete_trigger(
            self.triggers['DELETE'],
            self.source.name,
            self.name
        ))

    def delete_triggers(self):
        """Delete the triggers"""
        for trigger_method, trigger_name in self.triggers.items():
                self.execute(self.commands.drop_trigger(trigger_name, self.source.name))
                function_name = '{}_{}'.format(trigger_method.lower(), self.name)
                self.execute(self.commands.drop_function(function_name))

    def copy_in_chunks(self, chunk_size=None, throttle=None, start=None, limit=None):
        """Copy the data from the original table to the destination table in chunks"""
        # On restart, foreign_keys exist, don't remake them
        self.create_triggers()

        self.chunk_size = chunk_size if chunk_size else self.db.config['DEFAULT_CHUNK_SIZE']
        throttle = throttle if throttle else self.db.config['DEFAULT_THROTTLE']

        if self.count == 0 or self.count != self.source.count:
            if not start:
                start = self.source.min_pk
            if not limit:
                limit = self.source.max_pk

            self.start_time = datetime.datetime.now()

            pointer = start
            if not (pointer and limit):
                pass
            else:
                while pointer < limit:
                    self._copy_chunk(pointer)
                    pointer = self._get_next_pk(pointer)
                    self.log(start, pointer, limit)
                    time.sleep(throttle)
                if pointer == limit:
                    self._copy_chunk(pointer)
                    self.log(start, pointer, limit)

        print('Copy complete! Adding referenced foreign keys')
        referenced_fks = [x for x in self.source.foreign_keys if x.referenced]
        self.add_foreign_keys(referenced_fks, override_table=self.name)
        return True

    def _get_next_pk(self, last_pk):
        """Return the next id"""
        ans = self.execute(self.commands.next_pk(
            self.name,
            self.primary_key_column,
            last_pk,
            self.chunk_size
        ))[0][0]
        return ans

    def _copy_chunk(self, last_pk):
        """Copy this chunk to the destination table"""
        self.execute(self.commands.copy_chunk(
            self.name,
            self._join_cols(self.intersection.dest_columns),
            self._qualify(self.source.name, self.intersection.origin_columns),
            self.source.name,
            self.primary_key_column,
            last_pk,
            self.chunk_size
        ))
        self.commit()

    def _trigger_name(self, type):
        """Create trigger name"""
        name = 'migration_trigger_{}_{}'.format(type.lower(), self.source.name)
        return name[:self.db.config['MAX_LENGTH_NAME']]

    def rename_tables(self):
        """Rename the tables"""
        self.delete_triggers()
        success = False
        source_name, archive_name, migrate_name = self.source.name, self.source.archive_name, self.name
        try:
            self.execute(self.commands.BEGIN)
            self.execute(self.commands.rename_table(source_name, archive_name))
            self.execute(self.commands.rename_table(migrate_name, source_name))
            self.execute(self.commands.COMMIT)
            success = True
        except Exception as e:
            print('Rename Error', e)
        if success:
            print('Rename complete!')
            new = self.db.table(source_name)
            archive = self.db.table(archive_name)
            self.move_sequences(archive, new.name)
            return new, archive
        else:
            raise Exception('Unable to Rename')

    def move_sequences(self, archive, new_table_name):
        archive_sequence_cols = archive.sequence_cols
        for seq, col in archive_sequence_cols:
            archive.remove_sequence_from_col(col)
            archive.set_sequence_owner(seq, new_table_name, col)

    def log(self, start, current, last):
        """Prints status logs"""
        try:
            percent_complete = ((current - start) / (float(last) - start))
            if percent_complete == 0:
                return
            run_time = (datetime.datetime.now() - self.start_time).total_seconds()
            remaining = (run_time / percent_complete) - run_time
            time_remaining = datetime.timedelta(seconds=remaining)
            print('Processed %d/%d %.2f%% - time left: %s' % (
            current, last, percent_complete * 100, str(time_remaining)))
        except TypeError:
            print('Processed pk {} limit is {}'.format(current, last))


class Intersection(object):
    """Maps columns from origin to destination"""

    def __init__(self, origin_table, destination_table):
        """Initialize the intersection object"""
        self.origin = origin_table
        self.destination = destination_table
        self._intersection = None

    @property
    def origin_columns(self):
        """The columns requested when copying from the original table"""
        return self.intersection + self.origin_renames

    @property
    def origin_renames(self):
        """Return the origin columns in order"""
        try:
            return [tup[0] for tup in sorted(self.destination.renames)]
        except AttributeError:
            return []

    @property
    def dest_renames(self):
        """Return the destination renames in order"""
        try:
            return [tup[1] for tup in sorted(self.destination.renames)]
        except AttributeError:
            return []

    @property
    def dest_columns(self):
        """The columns written to the destination table"""
        return self.intersection + self.dest_renames

    @property
    def intersection(self):
        """The columns shared between the original and destination tables"""
        self._intersection = list(set(self.origin.columns[:]).intersection(self.destination.columns[:]))
        self._intersection.sort()
        return self._intersection
