from psycopg2.extensions import connection
from typing import Optional
from loguru import logger

class PostgreServiceActions:

    def _generate_columns(
        self, 
        datas: list, 
        primary_key: Optional[str] = "id"
    ) -> tuple[str, str, str]:
        sample_data = datas[0]
        columns = ', '.join(sample_data.keys())
        values_params = ', '.join(['%s' for key in sample_data.keys()])
        if primary_key is None: 
            exluded_params = ', '.join([f'{key} = excluded.{key}' for key in sample_data.keys()])
        exluded_params = ', '.join([f'{key} = excluded.{key}' for key in sample_data.keys() if key != primary_key])
        return columns, values_params, exluded_params

    def _query_upsert(
            self, 
            table_name: str, 
            datas: list,
            primary_key: Optional[str] = "id"
        ): 
        columns, value_params, exluded_params = self._generate_columns(datas, primary_key)
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({value_params}) ON CONFLICT ({primary_key}) DO UPDATE SET {exluded_params}"
        return query
    
    def _query_insert(
        self,
        table_name: str,
        datas: list
    ): 
        columns, value_params, _ = self._generate_columns(datas, primary_key=None)
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({value_params})"
        return query
    
    def _generate_datas(
        self,
        datas: list
    ): 
        load_data = {}
        for data in datas: 
            if data['_table_name'] in load_data: 
                load_data[data['_table_name']]['_source'].append(data['_source'])
            else: 
                load_data[data['_table_name']] = {
                    '_primary_key': data['_primary_key'],
                    '_source': [data['_source']]
                }
        list_table = list(load_data.keys())
        return list_table, load_data
    
    def _generate_tuple_value(self, datas: list):
        new_list = []
        for data in datas:
            tuple_data = [value for _, value in data.items()]
            new_list.append(tuple(tuple_data))
        return new_list
        
    def bulk_upsert(
        self, 
        client: connection,
        datas: list,
    ): 
        list_table, load_data = self._generate_datas(datas) 
        cursor = client.cursor()

        for table in list_table:
            primary_key = load_data[table]['_primary_key']
            datas_ready = load_data[table]['_source']
            
            query = self._query_upsert(table, datas_ready, primary_key)
            values = self._generate_tuple_value(datas_ready)

            cursor.executemany(query, values)
            logger.info(f"Upserted {table} data: {cursor.rowcount}")
            client.commit()
            
        cursor.close()

    def bulk_insert(
        self,
        client: connection,
        datas: list 
    ):
        list_table, load_data = self._generate_datas(datas) 
        cursor = client.cursor()

        for table in list_table:
            query = self._query_insert(table, load_data[table]['_source'])
            values = self._generate_tuple_value(load_data[table]['_source'])
            
            cursor.executemany(query, values)
            logger.info(f"Inserted {table} data: {cursor.rowcount}")
            client.commit()
        
        cursor.close()

        
        
