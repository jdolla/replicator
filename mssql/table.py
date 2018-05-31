from os import path, listdir
from textwrap import dedent
import pyodbc


def sqlTemplate(templatePath):
    with open(templatePath, 'r') as f:
        return f.read()


def sqlTemplates():
    """Loads templates into a dictionary"""

    templates = {}
    workingDir = path.dirname(path.realpath(__file__))
    sqlDir = path.join(workingDir, 'sql')
    for file in listdir(path.join(workingDir, 'sql')):
        templateName = path.splitext(file)[0]
        templates[templateName] = sqlTemplate(path.join(sqlDir, file))

    return templates


templates = sqlTemplates()


class Table:
    """Class for managing a sql table"""

    def __init__(self, connection, schemaName, tableName):
        self._batch = 10000
        self._connection = connection
        self._tableName = tableName
        self._schemaName = schemaName
        self._schema = {}
        self._columns = ()
        self._pkCols = ()
        self._tempTable = ""

    def __dict__(self):
        return self.__dict__

    def __eq__(self, other):
        """Two tables are equal if:
            - they have the same schema
            - they have the same pk
            note: rowver is excluded
        """
        sSchema = dict(self.schema)
        oSchema = dict(other.schema)

        if 'rowver' in sSchema:
            del sSchema['rowver']

        if 'rowver' in oSchema:
            del oSchema['rowver']

        if sSchema != oSchema:
            return False

        return set(self.schema) == set(other.schema)

    def __lt__(self, other):
        """A Table is less than another table if it is missing columns
        found in the other table"""

        sSchema = self.schema
        oSchema = other.schema

        for col, attr in oSchema.items():
            if col not in sSchema:
                return True
            if attr != oSchema[col]:
                return True

        return False

    def __gt__(self, other):
        """A Table is less than another table if it is missing columns
        found in the other table"""

        sSchema = self.schema
        oSchema = other.schema

        for col, attr in sSchema.items():
            if col not in oSchema:
                return True
            if attr != sSchema[col]:
                return True

        return False

    @property
    def batch(self):
        return self._batch

    @batch.setter
    def batch(self, size):
        self._batch = size

    @property
    def name(self):
        return f'[{self._schemaName}].[{self._tableName}]'

    @property
    def schema(self):
        """Dictionary of mssql schema"""

        if self._schema:
            return self._schema

        query = templates['tableSchema']
        with self._connection.cursor() as cursor:
            cursor.execute(query, self._schemaName, self._tableName)
            cols = cursor.fetchall()

            self._schema = {
                col.COLUMN_NAME: {
                    'ORDINAL_POSITION': col.ORDINAL_POSITION,
                    'IS_NULLABLE': col.IS_NULLABLE,
                    'DATA_TYPE': col.DATA_TYPE,
                    'CHARACTER_MAXIMUM_LENGTH': col.CHARACTER_MAXIMUM_LENGTH,
                    'NUMERIC_PRECISION': col.NUMERIC_PRECISION,
                    'NUMERIC_SCALE': col.NUMERIC_SCALE,
                }
                for col in cols
            }
            return self._schema

    @property
    def columns(self):
        """Returns a tuple of columns"""

        if self._columns:
            return self._columns

        query = templates['tableColumns']
        with self._connection.cursor() as cursor:
            cursor.execute(query, self._schemaName, self._tableName)
            cols = cursor.fetchall()
            self._columns = tuple([f"[{col.COLUMN_NAME}]" for col in cols])
            return self._columns

    @property
    def pkColumns(self):
        """Returns tuple representing the pk columns"""

        if self._pkCols:
            return self._pkCols

        query = templates['tablePk']
        with self._connection.cursor() as cursor:
            cursor.execute(query, self._schemaName, self._tableName)
            cols = cursor.fetchall()
            self._pkCols = tuple([f"[{col.COLUMN_NAME}]" for col in cols])
            return self._pkCols

    @property
    def tempTable(self):
        """Returns the SQL needed to create a local temporary table version
            of this table."""

        if self._tempTable:
            return self._tempTable

        pk = ", ".join(self.pkColumns)

        columns = ", ".join([f"[{col}] {TypeMap.typeFor(attr)}"
                             for col, attr in self.schema.items()])

        tempName = self._schemaName + self._tableName
        query = f"""
                IF OBJECT_ID('tempdb..#{tempName}') IS NOT NULL
                    DROP TABLE #{tempName};

                CREATE TABLE #{tempName}(
                    {columns},
                    primary key clustered ({pk})
                );
            """
        self._tempTable = dedent(query)
        return self._tempTable

    def syncWith(self, other):
        """Adds columns to this table so that columns match the source.
            DataTypes are always added according to the TypeMap
            New Columns are always nullable"""

        if self < other:
            newCols = {k: v for (k, v) in other.schema.items()
                       if k not in self.schema}

            for col, attr in newCols.items():
                query = f"Alter Table {self.name}" \
                    f" Add [{col}] {TypeMap.typeFor(attr)} NULL;"

                with self._connection.cursor() as cursor:
                    cursor.execute(query)
                    if not self._connection.autocommit:
                        cursor.commit()

    def deinit(self):
        """Deinitializes lazy props"""
        self._schema = {}
        self._columns = ()
        self._pkCols = ()
        self._tempTable = ""

    def rowver(self):
        query = f"""
            Select rowver = max(rowver)
            From {self.name};
        """

        with self._connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()[0]

    def rows(self, rowver, count=500):
        query = f"""
            Select top (?) {", ".join(self.columns)}
            From {self.name}
            Where rowver > ?
            Order by rowver asc
        """
        cursor = self._connection.cursor()
        while True:
            cursor.execute(query, (self.batch, rowver))
            rows = cursor.fetchmany(count)

            if not rows:
                break

            while True:
                yield rows
                rowver = max(row.rowver for row in rows)
                rows = cursor.fetchmany(count)

                if not rows:
                    break

    def insert(self, rows):
        pass

    def update(self, rows):
        pass

    def merge(self, rows):
        pass


class TypeMap:
    """Maps sql types to t-sql create statement"""

    swapTypes = {
        'timestamp': 'binary (8)'
    }

    sizedTypes = ('char', 'varchar', 'nvarchar', 'binary', 'varbinary')
    scaleTypes = ('numeric', 'decimal')

    @staticmethod
    def typeFor(type):
        base = type['DATA_TYPE']

        if base in TypeMap.swapTypes:
            return TypeMap.swapTypes[base]

        if base in TypeMap.sizedTypes:
            return f"{base} ({type['CHARACTER_MAXIMUM_LENGTH']})"

        if base in TypeMap.scaleTypes:
            return f"{base} ({type['NUMERIC_PRECISION']}" \
                ", {type['NUMERIC_SCALE']})"

        return base


if __name__ == "__main__":
    # Will remove this block when done testing
    genConn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=127.0.0.1,1433;"
        "DATABASE=replicatorDemo;"
        "UID=sa;"
        "PWD=p@ssword123!"
    )

    sinkConn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=127.0.0.1,1433;"
        "DATABASE=replicatorDemo;"
        "UID=sa;"
        "PWD=p@ssword123!"
    )

    animal = Table(genConn, 'dbo', 'animal')
    animalCopy = Table(sinkConn, 'dbo', 'animal_copy')

    # print(tableGen == tableSink)

    # print('<', animalCopy < animal)
    # print('=', animal == animalCopy)
    # print('<', animal > animalCopy)

    # animalCopy.syncWith(animal)

    # print(animalCopy.schema)
    # print(animalCopy.pkColumns)
    # print(animalCopy.pkColumns)

    # print(animal.rowver())
    # print(animalCopy.rowver())

    # print(animal.batch)
    # print(animalCopy.batch)

    # animal.batch = 1
    # print(animal.batch)

    # print(animal.rows(1))

    # print(animal.pkColumns)

    rowver = animalCopy.rowver()
    rows = animal.rows(rowver, 1)

    for row in rows:
        print(row)
