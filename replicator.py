import connectionManager
import datagen
import schema.mssql as sql
from datasink.odbcDataSink import MssqlRowSink

constr = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=replicatorDemo;UID=sa;PWD=p@ssword123!"

mgr = connectionManager.ConnectionManager()
mgr.addOdbcConnection('source', constr)

conn = mgr.getOdbcConnection('source')

srcTableSchema = sql.getTableSchema(conn, 'dbo', 'animal')
trgtTableSchema = sql.getTableSchema(conn, 'dbo', 'animal_pk')

cols = sql.getTableColumns(srcTableSchema)
# print('Cols:', cols)

pk = sql.getTablePk(conn, 'dbo', 'animal_pk')
# print('PK:', pk)

sink = MssqlRowSink(conn, 'dbo', 'animal_pk', pk, cols)
# print(sink.insertStatement)
# print(sink.updateStatement)

cursor = conn.cursor()
cursor.execute('create table #temp (col1 int)')

cursor.fast_executemany = True

cursor.executemany(
    'insert #temp (col1) values(?)', [(1,), (3,), (5,)])

print(conn.cursor().execute('select * from #temp').fetchall())
