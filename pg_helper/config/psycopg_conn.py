import psycopg2

from psycopg2.extensions import connection

class PostgrePsycopgConnection:
    """
    PostgrePsycopgConnection is a wrapper class around psycopg2 connection.
    This class provides a context manager interface to handle
    connection to postgresql server.

    Attributes:
        host (str): Hostname of the postgresql server.
        port (int): Port number of the postgresql server.
        dbname (str): Database name.
        username (str): Username to connect to the database.
        password (str): Password to connect to the database.
    """
    def __init__(self, **kwargs):
        self.__host: str = kwargs.get("host")
        self.__port: int = kwargs.get("port")
        self.__dbname: str = kwargs.get("dbname")
        self.__username: str = kwargs.get("username")
        self.__password: str = kwargs.get("password")
        self.__conn: connection = None
    
    def __enter__(self) -> connection:
        self.__conn = psycopg2.connect(
            host=self.__host,
            port=self.__port,
            dbname=self.__dbname,
            user=self.__username,
            password=self.__password
        )
        return self.__conn

    def __exit__(self, exc_type, exc_value, tb):
        self.__conn.close()

    def close(self):
        self.__conn.close()

    