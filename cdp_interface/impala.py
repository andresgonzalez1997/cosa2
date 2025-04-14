from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import pathlib

class Impala:
    def __init__(self, env):
        self.env = env
        
    def select(self, query): 
        try:
            query = self.replace_variables(query)

            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute("SET SYNC_DDL=1")
            cursor.execute(query)
            return as_pandas(cursor)
        except Exception as ex:
            print(ex)
            return pd.DataFrame()

    def execute(self, query):
        try:
            query = self.replace_variables(query)
            
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute("SET SYNC_DDL=1")
            cursor.execute(query)
            return True
        except Exception as ex:
            print(ex)
            return False
        
    def table_list(self):
        try:
            return self.select("SHOW TABLES IN @schema")["name"].tolist()
        except Exception as ex:
            print(ex)
            return pd.DataFrame()
        
    def column_list(self, table_name):
        result = self.select(f"DESCRIBE @schema.{table_name}")
        
        if result.empty: return []

        return list(
            zip( result["name"].to_list(), result["type"].to_list() )
        )
    
    def add_column(self, table_name, column_name, column_type):
        query = pathlib.Path("cdp_interface/sql_queries/impala_add_column.sql").read_text()
        query = query.replace("@table_name", table_name)
        query = query.replace("@column_name", column_name)
        query = query.replace("@column_type", column_type)
        
        return self.execute(query)
         
    def refresh_table(self, table_name):
        return self.execute(f"REFRESH @schema.{table_name}")
    
    def compute_stats(self, table_name):
        return self.execute(f"COMPUTE STATS @schema.{table_name}")
    
    def drop_table(self, table_name):
        return self.execute(f"DROP TABLE @schema.{table_name}")
    
    
    #######################################################

        
    def conn(self):
        return connect(
            host = self.env["impala_host"],
            port = self.env["port"],
            auth_mechanism = "GSSAPI",
            use_ssl = True
        )
    
    def replace_variables(self, query):
        for key, item in self.env.items():
            variable = f"@{key}"
            query = query.replace(variable, item)
        return query
