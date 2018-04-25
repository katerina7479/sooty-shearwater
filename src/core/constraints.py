

class Constraint(object):
    """
    Represents a database constraint i.e. PRIMARY KEY, UNIQUE
    Not including Foreign Keys
    """
    allowed_types = ['UNIQUE', 'PRIMARY KEY', 'CHECK']

    def __init__(self, name, table_name, type, column, check_clause=None):
        """
        Initialize the constraint object
        """
        self.name = name
        self.table_name = table_name
        if type not in self.allowed_types:
            raise TypeError('Constraint Type {} not in {}'.format(type, self.allowed_types))
        self.type = type
        self.column = column
        self.check_clause = check_clause

    def __repr__(self):
        """String representation"""
        return '{}: {}, {} - {}'.format(self.type, self.name, self.table_name, self.column)


class ForeignKey(object):
    def __init__(self, name, table_name, column, fk_table_name, fk_column, referenced=False):
        """
        Initialize the constraint object
        """
        self.name = name
        self.table_name = table_name
        self.column_name = column
        self.fk_table_name = fk_table_name
        self.fk_column = fk_column
        self.referenced = referenced

    def __repr__(self):
        """String representation"""
        return 'FOREIGN KEY {}: {}.{} ref {}.{}'.format(
            self.name,
            self.table_name,
            self.column_name,
            self.fk_table_name,
            self.fk_column
        )

    def __eq__(self, other):
        """Determine constraint equality (don't match on referenced table)"""
        if (self.table_name == other.table_name and
            self.column_name == other.column_name and
                    self.fk_column == other.fk_column):
            return True
        return False

    @property
    def self_referential(self):
        """Returns True if self_referential"""
        return self.table_name == self.fk_table_name


class Index(object):
    """Represents a table index"""
    def __init__(self, table_name, index_name, unique, column_name):
        """Set the initial data"""
        self.table = table_name
        self.name = index_name
        self.unique = unique
        self.column = column_name

    def __eq__(self, other):
        """Determine index equality"""
        if (self.table == other.table and
                    self.name == other.name and
                    self.column == other.column):
            return True
        return False

    def __repr__(self):
        """String representation"""
        return 'Index {}: {}'.format(self.name, self.column)
