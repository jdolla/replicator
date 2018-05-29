from table import Table
import pyodbc


class MssqlRowGenerator:
    """
        A generator that produces rows from a specified table.
        Rows are obtained based on the 'rowver' column.
    """

    def __init__(self, connStr, schemaName, tableName, **kwargs):
        self._connection = pyodbc.connect(connStr)
        self._rowCursor = self._connection.cursor()
        self._table = f'[{schemaName}].[{tableName}]'
        self._rowver = (kwargs['rowver']
                        if 'rowver' in kwargs else 0)

        self._batch = (kwargs['batch']
                       if 'batch' in kwargs else 10000)

        self._retCount = (kwargs['retCount']
                          if 'retCount' in kwargs else 1000)

    def __iter__(self):
        query = f"""
            Select top (?) *
            From {self._table}
            Where rowver > ?
            Order by rowver asc
        """

        while True:
            self._rowCursor.execute(query, (self._batch, self._rowver))
            rows = self._rowCursor.fetchmany(self._retCount)

            if not rows:
                break

            while True:
                yield rows
                self._rowver = max(row.rowver for row in rows)
                rows = self._rowCursor.fetchmany(self._retCount)

                if not rows:
                    break
