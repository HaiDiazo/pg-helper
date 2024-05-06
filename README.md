
# PostgreSQL Helper 
This module for helping for make a bulk insert data into postgresql 
To install this, you can up into pypi server using poetry and then install into python environment

```
--index-url http://<IP-host>:6060/simple 
--trusted-host <IP-host>:6060

pg-helper
psycopg2==2.9.6
```
if you have error when install module <b>psycopg2</b>, you can use <b>psycopg2-binary</b>. 
After install, you can following for open connection the postgreSQL 

```
with PostgrePsycopgConnection(
        host="localhost",
        port=5432,
        username="postgres",
        password="postgres",
        dbname="postgres",
    ) as connect: 
        # the code 

# or you can save into variable like this
connect = PostgrePsycopgConnection(host="localhost", port=5432, username="postgres", password="postgres", dbname="postgres",)
# execute 
connect.close()
```

Then, for use a bulk function you must create a paramater in below

```
from pg_helper import bulk_upsert

# the params can be append into list
params = [{
    "_table_name": "tb_airport_departure",
    "_primary_key": "id",
    "_source": {
        "column1": "data",
        "column2": "data"
    }
}]

bulk_upsert(
    client=connect, 
    datas=list_bulk
)

```