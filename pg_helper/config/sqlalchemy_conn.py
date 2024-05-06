from sqlalchemy import create_engine
from sqlalchemy import URL


class PostgreSqlAlchemyConnection:
    """
    PostgreSqlAlchemyConnection is a wrapper class around SQLAlchemy engine connection.
    This class provides a context manager interface to handle
    connection to postgresql server.

    Attributes:
        __host (str): Hostname of the postgresql server.
        __port (int): Port number of the postgresql server.
        __dbname (str): Database name.
        __username (str): Username to connect to the database.
        __password (str): Password to connect to the database.
    """

    def __init__(self, **kwargs):
        self.__host: str = kwargs.get("host")
        self.__port: int = kwargs.get("port")
        self.__dbname: str = kwargs.get("dbname")
        self.__username: str = kwargs.get("username")
        self.__password: str = kwargs.get("password")
        self.__conn = None

    def __enter__(self):
        self.__conn = create_engine(
            URL.create(
                "postgresql",
                username=self.__username,
                password=self.__password,
                host=self.__host,
                port=self.__port,
                database=self.__dbname
            )
        )
        return self.__conn

    def __exit__(self, exc_type, exc_value, tb):
        self.__conn.close()

    def close(self):
        self.__conn.close()