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
        self._mergeQueries = {}

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

        if rowver is None:
            rowver = b'\x00\x00\x00\x00\x00\x00\x00'

        cursor = self._connection.cursor()
        cursor.execute(query, (self.batch, rowver))
        rows = cursor.fetchmany(count)

        while True:
            yield rows
            rows = cursor.fetchmany(count)

            if not rows:
                break

    def insert(self, rows, columns):
        query = f"""
            Insert {self.name} ({', '.join(columns)})
            values ({', '.join('?' * len(columns))});
        """

        with self._connection.cursor() as cursor:
            cursor.fast_executemany = True
            cursor.executemany(query, rows)
            if not self._connection.autocommit:
                cursor.commit()

    def mergeStatement(self, columns):

        if self._mergeQueries:
            return self._mergeQueries

        selfPk = ", ".join(self.pkColumns)

        selfColSchema = ", ".join([f"[{col}] {TypeMap.typeFor(attr)}"
                                   for col, attr in self.schema.items()])

        # Generate a temporary table for staging the data
        tempTableName = self._schemaName + self._tableName
        tempTableCreate = f"""
                IF OBJECT_ID('tempdb..#{tempTableName}') IS NOT NULL
                    DROP TABLE #{tempTableName};

                CREATE TABLE #{tempTableName}(
                    {selfColSchema},
                    primary key clustered ({selfPk})
                );
            """
        tempTableCreate = dedent(tempTableCreate)

        # Insert rows into the staging table
        # Use the columns from the source data.
        insertCols = ', '.join(columns)
        tempTableInsert = f"""
                Insert #{tempTableName} ({insertCols})
                values ({', '.join('?' * len(columns))});
            """
        tempTableInsert = dedent(tempTableInsert)

        # Output table for capturing updated keys
        selfPkColShema = [f'{key} {TypeMap.typeFor(attr)}'
                          for key, attr in self.schema.items()
                          if f'[{key}]' in self.pkColumns]

        outTempTableName = f'{tempTableName}_updated'
        outTempTableCreate = f"""
                if object_id('tempdb..#{outTempTableName}') is not null
                    drop table #{outTempTableName};

                Create Table #{outTempTableName} (
                    {', '.join(selfPkColShema)},
                    primary key({selfPk})
                );
            """
        outTempTableCreate = dedent(outTempTableCreate)

        # Update Existing records
        # Exclude the pk columns from the update.
        updateCols = [
            f"t.{col} = s.{col}"
            for col in columns
            if col not in self.pkColumns]

        joinCols = [f's.{col} = t.{col}' for col in self.pkColumns]

        outputCols = [f'inserted.{col}' for col in self.pkColumns]

        updateTable = f"""
                    update t
                    set {', '.join(updateCols)}
                    output {', '.join(outputCols)}
                    into #{outTempTableName} ({', '.join(self.pkColumns)})
                    from {self.name} t
                    inner join #{tempTableName} s
                    on {' and '.join(joinCols)};
                """
        updateTable = dedent(updateTable)

        # Insert new rows
        insertTable = f"""
                insert {self.name} ({insertCols})
                select {', '.join([f's.{col}' for col in columns])}
                from #{tempTableName} s
                left join #{outTempTableName} t
                on {' and '.join(joinCols)}
                where t.{self.pkColumns[0]} is null;
            """
        insertTable = dedent(insertTable)

        self._mergeQueries = {
            'tempTableCreate': tempTableCreate,
            'outTempTableCreate': outTempTableCreate,
            'tempTableInsert': tempTableInsert,
            'updateTable': updateTable,
            'insertTable': insertTable
        }

        return self._mergeQueries

    def merge(self, rows, columns):
        mergeStatements = self.mergeStatement(columns)

        with self._connection.cursor() as cursor:
            # cursor.fast_executemany = True
            cursor.execute(mergeStatements['tempTableCreate'])
            cursor.execute(mergeStatements['outTempTableCreate'])
            cursor.executemany(mergeStatements['tempTableInsert'], rows)
            cursor.execute(mergeStatements['updateTable'])
            cursor.execute(mergeStatements['insertTable'])

            if not self._connection.autocommit:
                cursor.commit()


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
