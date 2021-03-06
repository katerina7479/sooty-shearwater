"""Test model migration tool"""
import MySQLdb
import unittest
import configparser
from src import DatabaseFactory
from src.core.constraints import Constraint, Index
from src.core.tables import Table

# pylint: disable=print-statement

db_config = configparser.ConfigParser()
db_config.read('tests/.config.test')
section = 'MYSQL_TEST_DB'
TEST_DB = {
    'db': db_config.get(section, 'dbname'),
    'user': db_config.get(section, 'user'),
    'host': db_config.get(section, 'host'),
    'password': db_config.get(section, 'password')
}

CONFIG = {
    "DEFAULT_CHUNK_SIZE": 10000,
    "DEFAULT_THROTTLE": 0.1,
    "MAX_LENGTH_NAME": 60,
    "MAX_RENAME_RETRIES": 10,
    "RETRY_SLEEP_TIME": 10,
    "DIALECT": 'mysql'
}


class TestMySQLTable(unittest.TestCase):
    """Test for migration models"""

    def setUp(self):
        """Create a test table"""
        self.connection = MySQLdb.connect(**TEST_DB)
        dbf = DatabaseFactory(TEST_DB['db'], self.connection, CONFIG)
        self.db = dbf.fetch()
        self.employers = self.db.table('employers')
        self.users = self.db.table('users')

        # Remove later
        self.employers.drop()
        self.users.drop(cascade=True)
        self.users.create_from_statement("""
            CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name varchar(20)
            );""")
        self.employers.create_from_statement('''
            CREATE TABLE employers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            users_id integer REFERENCES users (id)
            );
            ''')

        id = self.users.insert_row({'name': 'Beyonce Knowles'})
        self.employers.insert_row({'users_id': id, 'name': 'Parkwood Entertainment'})
        id = self.users.insert_row({'name': 'Jeff Bridges'})
        self.employers.insert_row({'users_id': id, 'name': 'Marv Films'})

    def tearDown(self):
        self.employers.drop()
        self.users.drop(cascade=True)

    def test_db_tables(self):
        """Test get tables"""
        tables = self.db.tables
        self.assertIn('users', tables)
        self.assertTrue(self.db.table_exists('users'))

    def test_db_batch_execute(self):
        ans = self.db.batch_execute(['SELECT * FROM users', 'SELECT COUNT(1) FROM employers'])
        self.assertEqual(len(ans[0]), 2)
        self.assertEqual(ans[1][0][0], 2)

    def test_create(self):
        addresses = self.db.table('addresses')
        addresses.create()
        self.assertTrue(self.db.table_exists(addresses.name))
        addresses.drop()
        self.assertFalse(self.db.table_exists(addresses.name))

    def test_table(self):
        """Test base table"""
        b = self.db.table('users')
        create = b.create_statement
        self.assertEqual(create, 'CREATE TABLE `users` ( `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, `name` varchar(20) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `id` (`id`) ) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8')

        self.assertEqual(b.min_pk, 1)
        self.assertEqual(b.max_pk, 2)

    def test_rows(self):
        self.users.insert_row({'name': 'Bob Ross'})
        row = self.users.get_row(3)
        self.assertEqual(row['name'], 'Bob Ross')
        self.users.update_row(3, {'name': 'Robert Ross'})
        row = self.users.get_row(3)
        self.assertEqual(row['name'], 'Robert Ross')
        self.users.delete_row(3)
        row = self.users.get_row(3)
        self.assertIsNone(row)

    def test_count(self):
        count = self.users.count
        self.assertEqual(count, 2)

    def test_columns(self):
        cols = self.users.columns
        self.assertListEqual(cols, ['id', 'name'])

        self.users.add_column('email', 'varchar(255)')
        cols = self.users.columns
        self.assertListEqual(cols, ['id', 'name', 'email'])

        definition = self.users.get_column_definition('email')
        self.assertEqual(definition, 'varchar(255)')

        self.users.rename_column('email', 'email_address')
        cols = self.users.columns
        self.assertListEqual(cols, ['id', 'name', 'email_address'])

        self.users.drop_column('email_address')
        cols = self.users.columns
        self.assertListEqual(cols, ['id', 'name'])

        self.users.add_column('active', 'bool NOT NULL default true')
        definition = self.users.get_column_definition('active')
        self.assertEqual(definition, 'tinyint(1) NOT NULL')

        self.users.alter_column('active', 'tinyint(1) NOT NULL default false')
        definition = self.users.get_column_definition('active')
        self.assertEqual(definition, 'tinyint(1) NOT NULL')

        self.users.drop_column('active')

    # def test_foreign_key(self):
    #     """Foreign keys that affect a table can be on
    #     the table, or reference that table.
    #     We make a distinction.
    #     """
    #     employer_fks = self.employers.foreign_keys
    #     self.assertEqual(len(employer_fks), 1)
    #     employer_fk = employer_fks[0]
    #     self.assertEqual(employer_fk.table_name, 'employers')
    #     self.assertEqual(employer_fk.fk_table_name, 'users')
    #
    #     user_fks = self.users.foreign_keys
    #     self.assertEqual(employer_fk, user_fks[0])
    #
    #     ans = self.employers.check_foreign_key_exists(employer_fk.table_name,
    #                                                   employer_fk.column_name,
    #                                                   employer_fk.fk_table_name,
    #                                                   employer_fk.fk_column)
    #     self.assertTrue(ans)
    #
    #     self.employers.drop_foreign_keys()
    #
    #     fks = self.employers.foreign_keys
    #     self.assertListEqual(fks, [])
    #
    #     fks = self.users.foreign_keys
    #     self.assertListEqual(fks, [])
    #
    #     self.employers.add_foreign_keys(employer_fks)
    #
    #     fk = self.employers.foreign_keys[0]
    #     self.assertEqual(fk.table_name, 'employers')
    #     self.assertEqual(fk.fk_table_name, 'users')
    #
    # def test_additional_fks(self):
    #     address = self.db.table('address')
    #     address.create()
    #     add_fks = address.foreign_keys
    #     self.assertListEqual(add_fks, [])
    #
    #     fk = address.get_foreign_key('doesnt_exist')
    #     self.assertIsNone(fk)
    #
    #     address.add_column('user_id', 'integer')
    #     address.add_foreign_key('address', 'user_id', 'users', 'id')
    #
    #     fks = address.foreign_keys
    #     self.assertEqual(len(fks), 1)
    #     fk = fks[0]
    #     self.assertEqual(fk.table_name, 'address')
    #     self.assertEqual(fk.fk_table_name, 'users')
    #
    #     new_address = self.db.table('new_address')
    #     new_address.create_from_statement(address.create_statement)
    #
    #     new_address.add_foreign_keys(fks)
    #
    #     self.assertEqual(new_address.foreign_keys[0].table_name, 'new_address')
    #
    #     new_address.drop()
    #     address.drop()

    def test_constraints(self):
        constraints = self.employers.constraints
        self.assertEqual(len(constraints), 2)

        constraints = self.users.constraints
        self.assertEqual(len(constraints), 2)

        self.employers.add_constraint('UNIQUE', 'name')
        self.assertEqual(len(self.employers.constraints), 3)

        for constraint in self.employers.constraints:
            if constraint.type == 'UNIQUE' and constraint.column == 'name':
                self.employers.drop_constraint(constraint.name)

        self.assertEqual(len(self.employers.constraints), 2)

    def test_indexes(self):
        indices = self.users.indexes
        self.assertIn('PRIMARY', indices[0].name)
        pk = self.users.primary_key

        i = self.users.get_index(pk.name)
        self.assertEqual(i, indices[0])

        self.users.add_index(['id', 'name'], unique=True)

        indices = self.users.indexes
        self.assertEqual(len(indices), 4)

        for index in indices:
            if '_unique' in index.name:
                self.users.drop_index(index.name)
                break

        indices = self.users.indexes
        self.assertEqual(len(indices), 2)

    def test_triggers(self):
        triggers = self.users.get_triggers()
        self.assertListEqual(triggers, [])


class TestMySQLMigrationTable(unittest.TestCase):

    def setUp(self):
        self.connection = MySQLdb.connect(**TEST_DB)
        dbf = DatabaseFactory(TEST_DB['db'], self.connection, CONFIG)
        self.db = dbf.fetch()
        self.users = self.db.table('users')
        self.users.drop(cascade=True)

        self.users.create_from_statement('''
            CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name varchar(20),
            address text,
            city varchar(20),
            state varchar(2),
            zip integer
            );'''
        )

        self.users.insert_row({'name': 'J.J Abrams', 'address': '1221 Olympic Boulevard',
                               'city': 'Santa Monica', 'state': 'CA', 'zip': 90404})

        self.users.insert_row({'name': 'Joss Whedon', 'address': 'P.O. Box 988',
                               'city': 'Malibu', 'state': 'CA', 'zip': 90265})

    def tearDown(self):
        self.users.drop(cascade=True)

    def test_migrate(self):
        new_users = self.db.migration_table(self.users)
        new_users.create_from_source()

        index = new_users.indexes[0]
        self.assertIn('PRIMARY', index.name)
        self.assertTrue(isinstance(index, Index))

        primary_key = new_users.primary_key
        self.assertIn('PRIMARY', primary_key.name)
        self.assertTrue(isinstance(primary_key, Constraint))
        new_users.drop()

    def test_rename_triggers(self):
        new_users = self.db.migration_table(self.users)
        new_users.create_from_source()

        new_users.rename_column('zip', 'zipcode')
        self.assertEqual(new_users.renames[0], ('zip', 'zipcode'))
        self.assertListEqual(new_users.intersection.origin_columns, ['address', 'city', 'id', 'name', 'state', 'zip'])
        self.assertListEqual(new_users.intersection.origin_renames, ['zip'])
        self.assertListEqual(new_users.intersection.dest_renames, ['zipcode'])
        self.assertListEqual(new_users.intersection.dest_columns, ['address', 'city', 'id', 'name', 'state', 'zipcode'])
        self.assertListEqual(new_users.intersection.intersection, ['address', 'city', 'id', 'name', 'state'])

        new_users.create_triggers()

        id = self.users.insert_row({'name': 'Damien Chazelle', 'address': '1223 Wilshire Blvd.',
                                    'city': 'Santa Monica', 'state': 'CA', 'zip': 90403})
        row = new_users.get_row(id)
        self.assertDictEqual(row, {'city': 'Santa Monica', 'name': 'Damien Chazelle', 'zipcode': 90403, 'state': 'CA', 'address': '1223 Wilshire Blvd.', 'id': 3})
        count = new_users.count
        self.assertEqual(count, 1)

        self.users.delete_row(1)
        self.assertEqual(self.users.count, 2)
        self.assertEqual(new_users.count, 1)

        self.users.update_row(2, {'address': '1003 Amherst Ave.', 'city': 'Los Angeles', 'zip': 90049})
        self.assertEqual(self.users.count, 2)
        self.assertEqual(new_users.count, 1)

        self.users.delete_row(3)
        self.assertEqual(self.users.count, 1)
        self.assertEqual(new_users.count, 0)

        new_users.drop()

    # def test_copy_in_chunks(self):
    #     new_users = self.db.migration_table(self.users)
    #     new_users.create_from_source()
    #     new_users.rename_column('zip', 'zipcode')
    #
    #     new_users.copy_in_chunks()
    #
    #     self.assertEqual(new_users.count, self.users.count)
    #     self.users, archive = new_users.rename_tables()
    #     archive.drop()
#
#
# class TestMySQLComplexMigrations(unittest.TestCase):
#
#     def setUp(self):
#         self.connection = MySQLdb.connect(**TEST_DB)
#         dbf = DatabaseFactory(TEST_DB['db'], self.connection, CONFIG)
#         self.db = dbf.fetch()
#         self.users = self.db.table('users')
#         self.address = self.db.table('address')
#         self.org = self.db.table('org')
#
#         self.org.drop(cascade=True)
#         self.users.drop(cascade=True)
#         self.address.drop(cascade=True)
#
#         self.org.create_from_statement('''
#             CREATE TABLE org (
#             id SERIAL PRIMARY KEY,
#             name VARCHAR(40) UNIQUE NOT NULL)''')
#
#         self.users.create_from_statement('''
#             CREATE TABLE users (
#             id SERIAL PRIMARY KEY,
#             name varchar(20) UNIQUE,
#             created_at TIMESTAMP,
#             friend_id INTEGER REFERENCES users(id),
#             org_id INTEGER REFERENCES org(id)
#             )''')
#         self.users.add_index(['created_at'])
#
#         self.address.create_from_statement('''
#             CREATE TABLE address (
#             id SERIAL PRIMARY KEY,
#             address text,
#             city varchar(20),
#             state varchar(2),
#             zip integer NOT NULL,
#             user_id INTEGER references users(id)
#             )''')
#
#         self.org.insert_row({'name': 'Friend Face'})
#         self.org.insert_row({'name': 'Social Nook'})
#         self.users.insert_row({'name': 'founder', 'friend_id': 1, 'org_id': 1})
#         self.users.insert_row({'name': 'early adopter', 'friend_id': 1, 'org_id': 2})
#
#         self.address.insert_row({'zip': 90120, 'address': 'test place', 'user_id': 1})
#         self.address.insert_row({'zip': 70433, 'address': 'awful place', 'user_id': 2})
#
#     def tearDown(self):
#         self.org.drop(cascade=True)
#         self.users.drop(cascade=True)
#         self.address.drop(cascade=True)
#
#     def test_with_foreign_keys(self):
#         """Test with multiple foreign keys"""
#         new_users = self.db.migration_table(self.users)
#
#         self.assertEqual(len(self.users.constraints), 3)
#         self.assertEqual(len(self.users.foreign_keys), 3)
#         self.assertEqual(len(self.users.indexes), 3)
#
#         self.assertEqual(len(new_users.constraints), 0)
#         self.assertEqual(len(new_users.foreign_keys), 0)
#         self.assertEqual(len(new_users.indexes), 0)
#
#         new_users.create_from_source()
#
#         self.assertEqual(len(new_users.constraints), 3)
#         self.assertEqual(len(new_users.foreign_keys), 1)
#         self.assertEqual(len(new_users.indexes), 3)
#
#         new_users.add_column('profession', 'varchar(20)')
#         new_users.copy_in_chunks()
#         self.assertEqual(len(new_users.foreign_keys), 3)
#         self.users, archive = new_users.rename_tables()
#
#         self.assertEqual(len(self.users.constraints), 3)
#         self.assertEqual(len(self.users.foreign_keys), 3)
#         self.assertEqual(len(self.users.indexes), 3)
#
#         archive.drop()
#
#
class TestMySQLUtils(unittest.TestCase):
    """Test util functions"""

    def test_join_cols(self):
        ans = Table._join_cols(['this', 'that', 'something'])
        self.assertEqual(ans, 'this, that, something')

    def test_join_values(self):
        ans = Table._join_values(['this', 1, 7.0, 'that'])
        self.assertEqual(ans, "'this', 1, 7.0, 'that'")

        with self.assertRaises(TypeError):
            Table._join_values([('this', "won't"), 'work'])

    def test_join_conditionals(self):
        ans = Table._join_equality({'this': 'that', 'something': 3})
        self.assertEqual(ans, "this='that', something=3")

        with self.assertRaises(TypeError):
            Table._join_equality({'this': ("won't", 'work')})

    def test_qualify(self):
        ans = Table._qualify('mytable', ['col1', 'col2', 'col3'])
        self.assertEqual(ans, 'mytable.col1, mytable.col2, mytable.col3')

    def test_equals(self):
        ans = Table._equals(['this', 'something'], 'new_table', ['that', 'something_else'])
        self.assertEqual(ans, 'this=new_table.that, something=new_table.something_else')

    def test_dictify(self):
        ans = Table._dictify(['col1', 'col2', 'col3'], ['val1', 'val2', 'val3'])
        self.assertDictEqual(ans, {'col2': 'val2', 'col3': 'val3', 'col1': 'val1'})

    def test_random_string(self):
        ans = Table._random_string(5)
        self.assertEqual(len(ans), 5)

    def test_join_batch_rows(self):
        ans = Table._join_batch_rows([('this', 'that'), ('something', "something's else")])
        self.assertEqual(ans, "('this', 'that'), ('something', 'something''s else')")


if __name__ == '__main__':
    unittest.main()