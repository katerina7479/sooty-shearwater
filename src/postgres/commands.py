


class PostgresCommands(object):

    BEGIN = 'BEGIN;'
    COMMIT = 'COMMIT;'

    @staticmethod
    def get_tables(database_name):
        return '''SELECT DISTINCT(tablename)
                    FROM pg_catalog.pg_tables
                    WHERE schemaname != 'pg_catalog'
                    AND schemaname != 'information_schema'
                 '''

    show_table_function = """CREATE OR REPLACE FUNCTION show_create_table(p_table_name varchar)
                  RETURNS text AS
                $BODY$
                DECLARE
                    v_table_ddl   text;
                    column_record record;
                BEGIN
                    FOR column_record IN
                        SELECT
                            b.nspname as schema_name,
                            b.relname as table_name,
                            a.attname as column_name,
                            pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
                            CASE WHEN
                                (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                                 FROM pg_catalog.pg_attrdef d
                                 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT NULL THEN
                                'DEFAULT '|| (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                                              FROM pg_catalog.pg_attrdef d
                                              WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
                            ELSE
                                ''
                            END as column_default_value,
                            CASE WHEN a.attnotnull = true THEN
                                'NOT NULL'
                            ELSE
                                'NULL'
                            END as column_not_null,
                            a.attnum as attnum,
                            e.max_attnum as max_attnum
                        FROM
                            pg_catalog.pg_attribute a
                            INNER JOIN
                             (SELECT c.oid,
                                n.nspname,
                                c.relname
                              FROM pg_catalog.pg_class c
                                   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                              WHERE c.relname ~ ('^('||p_table_name||')$')
                                AND pg_catalog.pg_table_is_visible(c.oid)
                              ORDER BY 2, 3) b
                            ON a.attrelid = b.oid
                            INNER JOIN
                             (SELECT
                                  a.attrelid,
                                  max(a.attnum) as max_attnum
                              FROM pg_catalog.pg_attribute a
                              WHERE a.attnum > 0
                                AND NOT a.attisdropped
                              GROUP BY a.attrelid) e
                            ON a.attrelid=e.attrelid
                        WHERE a.attnum > 0
                          AND NOT a.attisdropped
                        ORDER BY a.attnum
                    LOOP
                        IF column_record.attnum = 1 THEN
                            v_table_ddl:='CREATE TABLE {} (';
                        ELSE
                            v_table_ddl:=v_table_ddl||',';
                        END IF;

                        IF column_record.attnum <= column_record.max_attnum THEN
                            v_table_ddl:=v_table_ddl||chr(10)||
                                     '    '||column_record.column_name||' '||column_record.column_type||' '||column_record.column_default_value||' '||column_record.column_not_null;
                        END IF;
                    END LOOP;

                    v_table_ddl:=v_table_ddl||');';
                    RETURN v_table_ddl;
                END;
                $BODY$
                  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER;
        """

    drop_show_create_table = '''DROP FUNCTION show_create_table(p_table_name varchar);'''

    @staticmethod
    def get_database_sequences(database_name):
        return '''SELECT sequence_name
                  FROM information_schema.sequences
                  WHERE sequence_catalog = '{}'
               '''.format(database_name)

    @staticmethod
    def create_sequence(sequence_name):
        return '''CREATE SEQUENCE {}'''.format(sequence_name)

    @staticmethod
    def create_table(tablename, primary_key_col):
        return '''CREATE TABLE
                  IF NOT EXISTS {}
                  ({} SERIAL PRIMARY KEY)
               '''.format(tablename, primary_key_col)

    @staticmethod
    def get_table_create_statement(tablename):
        return '''SELECT show_create_table(\'{}\')
               '''.format(tablename)

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
                  SELECT LASTVAL();
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
        return '''SELECT column_name
                  FROM information_schema.columns
                  WHERE table_name = '{}';'''.format(tablename)

    @staticmethod
    def column_definition(database_name, tablename, column_name):
        return '''SELECT udt_name, character_maximum_length, is_nullable, column_default
                  FROM information_schema.columns
                  WHERE TABLE_NAME = '{}'
                  AND COLUMN_NAME = '{}';'''.format(tablename, column_name)

    @staticmethod
    def add_column(tablename, column_name, definition):
        return 'ALTER TABLE {} ADD COLUMN {} {}'.format(tablename, column_name, definition)

    @staticmethod
    def alter_column(tablename, col_name, definition):
        return '''ALTER TABLE {}
                  ALTER COLUMN {}
                  {}'''.format(tablename, col_name, definition)

    @staticmethod
    def drop_column(tablename, col_name):
        return 'ALTER TABLE {} DROP {}'.format(tablename, col_name)

    @staticmethod
    def rename_column(tablename, old_name, new_name):
        return 'ALTER TABLE {} RENAME COLUMN {} TO {}'.format(tablename, old_name, new_name)

    @staticmethod
    def get_constraints(database_name, tablename):
        return '''SELECT tc.constraint_name,
                 tc.table_name,
                 tc.constraint_type,
                 ccu.column_name,
                 cc.check_clause
                 FROM information_schema.table_constraints AS tc
                 LEFT OUTER JOIN information_schema.constraint_column_usage AS ccu
                 ON ccu.constraint_name = tc.constraint_name
                 LEFT OUTER JOIN information_schema.check_constraints as cc
                 ON cc.constraint_name = tc.constraint_name
                 WHERE tc.table_name='{}'
                 AND tc.constraint_type != 'FOREIGN KEY';'''.format(tablename)

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
        return '''SELECT tc.constraint_name,
                 tc.table_name,
                 kcu.column_name,
                 ccu.table_name as ref_table,
                 ccu.column_name as ref_column,
                 CASE WHEN ccu.table_name='{table}' THEN TRUE ELSE FALSE END
                 FROM information_schema.table_constraints AS tc
                 LEFT OUTER JOIN information_schema.key_column_usage as kcu
                 ON tc.constraint_name = kcu.constraint_name
                 LEFT OUTER JOIN information_schema.constraint_column_usage as ccu
                 ON ccu.constraint_name = tc.constraint_name
                 WHERE (tc.table_name='{table}' or ccu.table_name='{table}')
                 AND tc.constraint_type = 'FOREIGN KEY';
        '''.format(table=tablename)

    @staticmethod
    def foreign_key_exists(database_name, table_name, column_name, referenced_table, referenced_column):
        return '''SELECT *
                 FROM information_schema.key_column_usage as kcu
                 JOIN information_schema.constraint_column_usage AS ccu
                 ON ccu.constraint_name = kcu.constraint_name
                 WHERE kcu.table_catalog = '{}'
                 AND kcu.table_name = '{}'
                 AND kcu.column_name = '{}'
                 AND ccu.table_name = '{}'
                 AND ccu.column_name = '{}'
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
        return 'ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}'.format(fk_tablename, fk_name)

    @staticmethod
    def get_indexes(tablename):
        return '''
            SELECT
             t.relname AS table_name,
             i.relname AS index_name,
             ix.indisunique AS index_unique,
             a.attname AS column_name
            FROM
             pg_class t,
             pg_class i,
             pg_index ix,
             pg_attribute a
            WHERE t.oid = ix.indrelid
             AND i.oid = ix.indexrelid
             AND a.attrelid = t.oid
             AND a.attnum = ANY(ix.indkey)
             AND t.relkind = 'r'
             AND t.relname = '{}'
            ORDER BY
             t.relname,
             i.relname;
        '''.format(tablename)

    @staticmethod
    def add_index(tablename, index_name, columns, unique=False):
        unique_str = 'UNIQUE' if unique else ''
        return '''CREATE {}
                  INDEX {}
                  ON {} ({});
               '''.format(
            unique_str,
            index_name,
            tablename,
            columns)

    @staticmethod
    def drop_index(tablename, index_name):
        return 'DROP INDEX IF EXISTS {}'.format(index_name)

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
    def get_sequences(tablename):
        return '''SELECT s.relname, a.attname
                         FROM pg_class s
                         JOIN pg_depend d ON d.objid=s.oid
                           AND d.classid='pg_class'::regclass
                           AND d.refclassid='pg_class'::regclass
                         JOIN pg_class t ON t.oid=d.refobjid
                         JOIN pg_attribute a ON a.attrelid=t.oid
                           AND a.attnum=d.refobjsubid
                         WHERE s.relkind='S' AND d.deptype='a'
                         AND t.relname='{}'
                '''.format(tablename)

    @staticmethod
    def remove_sequence_from_col(tablename, column):
        return 'ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT'.format(
            tablename,
            column
        )

    @staticmethod
    def set_sequence_owner(sequence_name, tablename, column):
        return 'ALTER SEQUENCE {} OWNED BY {}.{}'.format(
            sequence_name,
            tablename,
            column
        )

    @staticmethod
    def insert_function(destination_table, cols, vals):
        return '''CREATE OR REPLACE FUNCTION insert_{dest_table}() RETURNS TRIGGER AS
                  $BODY$
                  BEGIN
                      INSERT INTO
                        {dest_table}({cols})
                        VALUES({vals});
                       RETURN NEW;
                  END;
                  $BODY$
                  language plpgsql;
        '''.format(
            dest_table=destination_table,
            cols=cols,
            vals=vals
        )

    @staticmethod
    def insert_trigger(trigger_name, source_table, dest_table):
        return '''CREATE TRIGGER {trigger_name}
              AFTER INSERT ON {source_table}
              FOR EACH ROW
              EXECUTE PROCEDURE insert_{dest_table}();
              '''.format(
            trigger_name=trigger_name,
            source_table=source_table,
            dest_table=dest_table
        )

    @staticmethod
    def update_function(dest_table, cols_vals, pk_col):
        return '''CREATE OR REPLACE FUNCTION update_{dest_table}() RETURNS TRIGGER AS
                $BODY$
                BEGIN
                  UPDATE {dest_table} SET {cols_vals}
                  WHERE {pk_col}=NEW.{pk_col};
                  RETURN NEW;
                END;
                $BODY$
                language plpgsql;
                '''.format(dest_table=dest_table, cols_vals=cols_vals, pk_col=pk_col)

    @staticmethod
    def update_trigger(trigger_name, source_table, dest_table):
        return '''CREATE TRIGGER {trigger_name}
                 AFTER UPDATE ON {source_table}
                 FOR EACH ROW
                 EXECUTE PROCEDURE update_{dest_table}();
               '''.format(trigger_name=trigger_name,
                          source_table=source_table,
                          dest_table=dest_table
                          )

    @staticmethod
    def delete_function(dest_table, pk_col):
        return '''CREATE OR REPLACE FUNCTION delete_{dest_table}() RETURNS TRIGGER AS
                    $BODY$
                    BEGIN
                      DELETE FROM {dest_table}
                      WHERE {dest_table}.{pk_col}=OLD.{pk_col};
                      RETURN NEW;
                    END;
                    $BODY$
                    language plpgsql;
                    '''.format(
            dest_table=dest_table,
            pk_col=pk_col
        )

    @staticmethod
    def delete_trigger(trigger_name, source_table, dest_table):
        return '''CREATE TRIGGER {trigger_name}
                 AFTER DELETE ON {source_table}
                 FOR EACH ROW
                 EXECUTE PROCEDURE delete_{dest_table}();
                 '''.format(
            trigger_name=trigger_name,
            source_table=source_table,
            dest_table=dest_table
        )

    @staticmethod
    def drop_trigger(trigger_name, source_table):
        return 'DROP TRIGGER IF EXISTS {} ON {}'.format(
                    trigger_name,
                    source_table
        )

    @staticmethod
    def drop_function(function_name):
        return 'DROP FUNCTION IF EXISTS {}();'.format(
                    function_name
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
        return '''INSERT INTO {table} ({dest_cols}) (
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
    def rename_table(old_name, new_name):
        return '''ALTER TABLE {} RENAME TO {};'''.format(old_name, new_name)
