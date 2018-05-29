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
        self._connection = connection
        self._tableName = tableName
        self._schemaName = schemaName

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

    @property
    def name(self):
        return f'[{self._schemaName}].[{self._tableName}]'

    @property
    def schema(self):
        """Dictionary of mssql schema"""

        query = templates['tableSchema']
        with self._connection.cursor() as cursor:
            cursor.execute(query, self._schemaName, self._tableName)
            cols = cursor.fetchall()

            return {
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

    @property
    def columns(self):
        """Returns a tuple of columns"""
        query = templates['tableColumns']
        with self._connection.cursor() as cursor:
            cursor.execute(query, self._schemaName, self._tableName)
            cols = cursor.fetchall()
            return tuple([f"[{col.COLUMN_NAME}]" for col in cols])

    @property
    def pkColumns(self):
        """Returns tuple representing the pk columns"""
        query = templates['tablePk']
        with self._connection.cursor() as cursor:
            cursor.execute(query, self._schemaName, self._tableName)
            cols = cursor.fetchall()
            return tuple([f"[{col.COLUMN_NAME}]" for col in cols])

    @property
    def tempTable(self):
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
        return dedent(query)


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

    tableGen = Table(genConn, 'dbo', 'animal')
    tableSink = Table(sinkConn, 'dbo', 'animal_pk')

    print(tableGen == tableSink)
