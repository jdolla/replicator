"""
    Provides helper functions for working with odbc schema
"""
from os import path
import hashlib


def getSqlTemplate(templateName):
    workingDir = path.dirname(path.realpath(__file__))
    templatePath = path.join(workingDir, 'mssql', f'''{templateName}.sql''')
    with open(templatePath, 'r') as f:
        return f.read()


def getTableSchema(connection, schemaName, tableName):
    query = getSqlTemplate('tableSchema')

    with connection.cursor() as cursor:
        cursor.execute(query, (schemaName, tableName))
        return cursor.fetchall()


def getTableColumns(tableSchema):
    """Returns a tuple of row names"""
    columns = []
    for row in tableSchema:
        columns.append(row.COLUMN_NAME)

    return tuple(columns)


def getTablePk(connection, schemaName, tableName):
    query = getSqlTemplate('tablePk')

    with connection.cursor() as cursor:
        cursor.execute(query, (schemaName, tableName))
        rows = cursor.fetchall()

    pkCols = []
    for row in rows:
        pkCols.append(row.name)

    return tuple(pkCols)


def schemaMatches(schema1, schema2):
    pass
