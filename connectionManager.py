from enum import Enum
import pyodbc

pyodbc.pooling = True


class ConnectionType(Enum):
    odbc = 1


class ConnectionManager:
    """
        Manages Connections.
        Currently handles ODBC Only.
        TODO:
            Extend to other connection types.
    """

    def __init__(self):
        self._connections = {}

    def addOdbcConnection(self, name, ConnStr):
        """Adds an odbc connection type."""

        self._connections[name] = {
            "connection": pyodbc.connect(ConnStr),
            "type": ConnectionType.odbc
        }

    def getOdbcCursor(self, connection):
        odbcConn = self._connections[connection]['connection']
        return odbcConn.cursor()

    def getOdbcConnection(self, connection):
        return self._connections[connection]['connection']
