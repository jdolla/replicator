import pyodbc

pyodbc.pooling = True


class RowFinder:
    """A Generator that yeilds rows obtained from sql query"""

    def __init__(self, cursor, tableConfig):
        self._cursor = cursor
        self._table = tableConfig['table']
        self._rowver = (tableConfig['rowver']
                        if 'rowver' in tableConfig else 0)

        self._batch = (tableConfig['batch']
                       if 'batch' in tableConfig else 10000)

        self._retCount = (tableConfig['retCount']
                          if 'retCount' in tableConfig else 1000)

    def __iter__(self):
        query = """
            Select top (?) *
            From dbo.animal
            Where rowver > ?
            Order by rowver asc
        """

        while True:
            self._cursor.execute(query, (self._batch, self._rowver))
            rows = self._cursor.fetchmany(self._retCount)

            if not rows:
                break

            while True:
                yield rows
                self._rowver = max(row.rowver for row in rows)
                rows = self._cursor.fetchmany(self._retCount)

                if not rows:
                    break


class ConnMgr:
    """Manages Connections."""

    def __init__(self, connStr):
        self._cnxn = pyodbc.connect(connStr, autocommit=False)

    def getRowFinder(self, tableConfig):
        return RowFinder(self._cnxn.cursor(), tableConfig)


if __name__ == '__main__':
    connStr = "DRIVER={ODBC Driver 17 for SQL Server};" \
        "SERVER=127.0.0.1,1433;" \
        "DATABASE=replicatorDemo;" \
        "UID=sa;" \
        "PWD=p@ssword123!"

    demoDb = ConnMgr(connStr)
    rf = demoDb.getRowFinder(
        {"table": "dbo.animal", "retCount": 1, "batch": 5})

    total = 0
    for i in rf:
        print(i)
        total += 1

    print('total', total)
