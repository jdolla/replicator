from textwrap import dedent


class MssqlRowSink:
    """
        Manages sending data to an mssql target
    """

    def __init__(self, connection, schemaName, tableName, pkCols, tableColumns):
        self._connection = connection
        self._cursor = self._connection.cursor()

        self._insert = MssqlRowSink.getInsertQuery(
            schemaName, tableName, tableColumns)

        self._update = MssqlRowSink.getUpdateQuery(
            schemaName, tableName, tableColumns, pkCols)

    @staticmethod
    def quoteNames(cols):
        """Accepts a tuple of column names and adds square brackets."""
        quoteCols = ("[" + col + "]" for col in cols)
        return tuple(quoteCols)

    @staticmethod
    def getInsertQuery(schemaName, tableName, tableColumns):
        table = f'[{schemaName}].[{tableName}]'
        columns = ", ".join(MssqlRowSink.quoteNames(tableColumns))
        tokens = ", ".join("?" * len(tableColumns))

        insert = f"""
                insert {table} ({columns})
                values({tokens});
            """

        return dedent(insert)

    @property
    def insertStatement(self):
        return self._insert

    @staticmethod
    def getUpdateQuery(schemaName, tableName, tableColumns, pkCols):
        table = f'[{schemaName}].[{tableName}]'

        setCols = tuple(filter(lambda col: col not in pkCols, tableColumns))
        assignments = " = ?, ".join(
            MssqlRowSink.quoteNames(setCols)) + " = ?"

        pk = " = ? and ".join(
            MssqlRowSink.quoteNames(pkCols)) + " = ?"

        update = f"""
                update {table}
                set {assignments}
                where {pk};
            """

        return dedent(update)

    @property
    def updateStatement(self):
        return self._update

    def insert(self, rows):
        pass

    def update(self, rows):
        pass

    def merge(self, rows):
        pass
