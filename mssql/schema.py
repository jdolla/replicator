"""
    Provides helper functions for working with odbc schema
"""


def describeOdbcTable(connection, tableSchema, tableName):
    for column in connection.cursor().columns(schema=tableSchema, table=tableName):
        print(column)
