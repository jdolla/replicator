import connectionManager
import datagen
from mssql import schema


constr = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=replicatorDemo;UID=sa;PWD=p@ssword123!"

mgr = connectionManager.ConnectionManager()
mgr.addOdbcConnection('source', constr)

conn = mgr.getOdbcConnection('source')

schema.describeOdbcTable(conn, 'dbo', 'animal')
