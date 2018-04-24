
class MySqlCommands(object):

    @staticmethod
    def get_tables(database_name):
        return 'SHOW TABLES IN {}'.format(database_name)

    @staticmethod
    def set_foreign_key_checks(state=True):
        return 'SET FOREIGN_KEY_CHECKS = {};'.format(1 if state else 0)


    @staticmethod
    def create_table(tablename, primary_key_col):
        return '''CREATE TABLE
                  IF NOT EXISTS {}
                  ({} integer AUTO_INCREMENT NOT NULL PRIMARY KEY)
               '''.format(tablename, primary_key_col)

    @staticmethod
    def get_table_create_statement(tablename):
        return 'SHOW CREATE TABLE {}'.format(tablename)

    @staticmethod
    def drop_table(tablename, cascade=False):
        sql = 'DROP TABLE {}'.format(tablename)
        if cascade:
            sql += ' CASCADE'
        return sql

    @staticmethod
    def get_row(cols, table, pk_col, pk):
        return '''SELECT {}
                  FROM {}
                  WHERE {}={}
               '''.format(cols, table, pk_col, pk)

    @staticmethod
    def insert_row(table, cols, vals):
        return '''INSERT INTO {} (
                  {}) VALUES ({});
               '''.format(table, cols, vals)

    @staticmethod
    def update_row(table, col_val, pk_col, pk):
        return '''UPDATE {}
                  SET {}
                  WHERE {}={}
               '''.format(table, col_val, pk_col, pk)

    @staticmethod
    def delete_row(table, pk_col, pk):
        return '''DELETE FROM {}
                  WHERE {}={}
               '''.format(table, pk_col, pk)

    @staticmethod
    def table_count(tablename):
        return 'SELECT COUNT(1) FROM {}'.format(tablename)

    @staticmethod
    def table_columns(tablename):
        return '''SHOW COLUMNS IN {};'''.format(tablename)

    @staticmethod
    def column_definition(database_name, tablename, column_name):
        return '''SELECT COLUMN_TYPE, IS_NULLABLE
                 FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = '%s'
                 AND TABLE_NAME = '%s'
                 AND COLUMN_NAME = '%s';'''.format(database_name, tablename, column_name)

    @staticmethod
    def add_column(tablename, column_name, definition):
        return 'ALTER TABLE {} ADD COLUMN `{}` {}'.format(tablename, column_name, definition)

    @staticmethod
    def alter_column(tablename, col_name, definition):
        return '''ALTER TABLE {}
                  MODIFY COLUMN `{}`
                  {}'''.format(tablename, col_name, definition)

    @staticmethod
    def drop_column(tablename, col_name):
        return 'ALTER TABLE {} DROP `{}`'.format(tablename, col_name)

    @staticmethod
    def rename_column(tablename, old_name, new_name, data_type):
        return 'ALTER TABLE {} CHANGE COLUMN {} {} {}'.format(tablename, old_name, new_name, data_type)

    @staticmethod
    def get_constraints(database_name, tablename):
        return '''SELECT CONSTRAINT_NAME, CONSTRAINT_SCHEMA, TABLE_NAME, CONSTRAINT_TYPE
                 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                 WHERE INFORMATION_SCHEMA.TABLE_CONSTRAINTS.TABLE_SCHEMA = '{}'
                 AND INFORMATION_SCHEMA.TABLE_CONSTRAINTS.TABLE_NAME = '{}';'''.format(database_name, tablename)

    @staticmethod
    def add_check_not_null(tablename, column):
        return '''ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;'''.format(tablename, column)

    @staticmethod
    def add_check(tablename, check_clause):
        return '''ALTER TABLE {} ADD CHECK ({check_clause})'''.format(
                tablename,
                check_clause
            )

    @staticmethod
    def add_constraint(tablename, constraint_name, type, column):
        return '''ALTER TABLE {}
                  ADD CONSTRAINT {}
                  {} ({})
               '''.format(
                tablename,
                constraint_name,
                type.upper(),
                column
            )

    @staticmethod
    def drop_constraint(tablename, constraint_name):
        return 'ALTER TABLE {} DROP CONSTRAINT {};'.format(tablename, constraint_name)

    @staticmethod
    def foreign_keys(database_name, tablename):
        return '''SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                 WHERE TABLE_SCHEMA = '{}'
                 AND TABLE_NAME = '{}'
        '''.format(database_name, tablename)

    @staticmethod
    def foreign_key_exists(database_name, table_name, column_name, referenced_table, referenced_column):
        return '''SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                 WHERE TABLE_SCHEMA = '{}'
                 AND TABLE_NAME = '{}'
                 AND COLUMN_NAME = '{}'
                 AND REFERENCED_TABLE_NAME = '{}'
                 AND REFERENCED_COLUMN_NAME = '{}'
                 '''.format(
            database_name,
            table_name,
            column_name,
            referenced_table,
            referenced_column
        )

    @staticmethod
    def add_foreign_key(tablename, fk_name, column, fk_table, fk_column):
        return '''ALTER TABLE {table_name}
                  ADD CONSTRAINT {name}
                  FOREIGN KEY ({col})
                  REFERENCES {fk_table} ({fk_col})
               '''.format(
            table_name=tablename,
            name=fk_name,
            col=column,
            fk_table=fk_table,
            fk_col=fk_column
        )

    @staticmethod
    def drop_foreign_key(fk_tablename, fk_name):
        return 'ALTER TABLE {} DROP FOREIGN KEY IF EXISTS {}'.format(fk_tablename, fk_name)

    @staticmethod
    def get_indexes(tablename):
        return '''SHOW INDEX FROM {}'''.format(tablename)

    @staticmethod
    def add_index(tablename, index_name, columns, unique=False):
        unique_str = 'UNIQUE' if unique else ''
        return '''ALTER TABLE {}
                  ADD {} INDEX {}
                  ({})
               '''.format(
            tablename,
            unique_str,
            index_name,
            columns)

    @staticmethod
    def drop_index(tablename, index_name):
        return 'ALTER TABLE {} DROP INDEX IF EXISTS {}'.format(tablename, index_name)

    @staticmethod
    def min_pk(tablename, primary_key_col):
        return 'SELECT MIN({}) FROM {}'.format(
            primary_key_col,
            tablename
        )

    @staticmethod
    def max_pk(tablename, primary_key_col):
        return 'SELECT MAX({}) FROM {}'.format(
            primary_key_col,
            tablename
        )

    @staticmethod
    def get_triggers(databasename, tablename):
        return '''SELECT trigger_name FROM information_schema.triggers as it
                  WHERE it.trigger_schema = '{}'
                  AND it.event_object_table = '{}'
               '''.format(databasename, tablename)


    @staticmethod
    def insert_trigger(trigger_name, source_table, dest_table, columns, values):
        return '''CREATE TRIGGER {trigger_name}
              AFTER INSERT ON {source_table}
              FOR EACH ROW
              INSERT INTO {dest_table} ({columns}) VALUES {values}
              '''.format(
            trigger_name=trigger_name,
            source_table=source_table,
            dest_table=dest_table,
            columns=columns,
            values=values
        )

    @staticmethod
    def update_trigger(trigger_name, source_table, dest_table, equalities, pk_col):
        return '''CREATE TRIGGER {trigger_name}
                 AFTER UPDATE ON {source_table}
                 FOR EACH ROW
                 UPDATE {dest_table} SET {equalities}
                 WHERE `{pk_col}`=`NEW`.`{pk_col}`;
               '''.format(trigger_name=trigger_name,
                          source_table=source_table,
                          dest_table=dest_table,
                          equalities=equalities,
                          pk_col=pk_col
                          )

    @staticmethod
    def delete_trigger(trigger_name, source_table, dest_table, pk_col):
        return '''CREATE TRIGGER {trigger_name}
                 AFTER DELETE ON {source_table}
                 FOR EACH ROW
                 DELETE IGNORE FROM {dest_table}
                 WHERE {dest_table}.{pk_col} = OLD.{pk_col};
                 '''.format(
            trigger_name=trigger_name,
            source_table=source_table,
            dest_table=dest_table,
            pk_col=pk_col
        )

    @staticmethod
    def drop_trigger(trigger_name, source_table):
        return 'DROP TRIGGER IF EXISTS `{}`'.format(
            trigger_name
        )

    @staticmethod
    def next_pk(table, pk_col, last_pk, limit):
        return '''SELECT MAX(T1.{pk_col}) FROM (
                  SELECT {pk_col}
                  FROM {table}
                  WHERE {pk_col}>{last_pk}
                  ORDER BY {pk_col}
                  LIMIT {limit}) AS T1;'''.format(
            pk_col=pk_col,
            table=table,
            last_pk=last_pk,
            limit=limit
        )

    @staticmethod
    def copy_chunk(table, dest_cols, origin_cols, source_table, pk_col, last_pk, limit):
        return '''INSERT IGNORE INTO {table} ({dest_cols}) (
                  SELECT {origin_cols} FROM {source}
                  LEFT OUTER JOIN {table}
                  ON {source}.{pk_col}={table}.{pk_col}
                  WHERE {table}.{pk_col} IS NULL
                  AND {source}.{pk_col} >= {last_pk}
                  ORDER BY {pk_col}
                  LIMIT {limit}
                  );
              '''.format(
            table=table,
            dest_cols=dest_cols,
            origin_cols=origin_cols,
            source=source_table,
            pk_col=pk_col,
            last_pk=last_pk,
            limit=limit
        )

    @staticmethod
    def rename_table(source_name, archive_name, migration_name):
        return '''RENAME TABLE `{source_name}`
                  TO `{archive_name}`,
                  `{migration_name}` to `{source_name}`'''.format(
            source_name=source_name,
            archive_name=archive_name,
            migration_name=migration_name
        )
