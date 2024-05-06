
from pg_helper.config.psycopg_conn import PostgrePsycopgConnection
from pg_helper.config.sqlalchemy_conn import PostgreSqlAlchemyConnection
from pg_helper.service.actions import (
    PostgreServiceActions, 
    connection
)

def bulk_upsert(
    client: connection,
    datas: list
):
    """
        Executes bulk upsert operation using provided connection and datas.
        
        :param client: psycopg2 connection object
        :param col_primary_key: primary key column name
        :param datas: list of data dictionaries
        :type client: connection
        :type col_primary_key: str
        :type datas: list

        {
            "_table_name": "name_table",
            "_primary_key": "name column primary key",
            "_source": {
                "title": "...",
                "body": "..."
            }
        }
    """
     
    return PostgreServiceActions().bulk_upsert(
        client=client,
        datas=datas
    )

def bulk_insert(
    client: connection,
    datas: list
):
    """
        Executes bulk insert operation using provided connection and datas.
        
        :param client: psycopg2 connection object
        :param col_primary_key: primary key column name
        :param datas: list of data dictionaries
        :type client: connection
        :type col_primary_key: str
        :type datas: list

        {
            "_table_name": "name_table",
            "_source": {
                "title": "...",
                "body": "..."
            }
        }
    """
    return PostgreServiceActions().bulk_insert(
        client=client,
        datas=datas
    )
    
__all__ = [
    "PostgrePsycopgConnection",
    "PostgreSqlAlchemyConnection",
    "bulk_upsert"
]