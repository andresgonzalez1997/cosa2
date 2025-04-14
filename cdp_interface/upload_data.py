import pathlib
import pyarrow.parquet as pq
import pyarrow as pa
import os

class DataUpload:
    PARQUET_FOLDER_PATH = "cdp_interface/exported_parquet_files"
  
    def __init__(self, file_system, database):
        self.fs = file_system
        self.db = database

    def upload_data(self, data, table_name, file_name):
        print(f"uploading data to {table_name}")
        file_path = self.export_data_to_parquet_file(data, table_name, file_name)
        if file_path is None:
            return False

        if not self.upload_parquet_file_to_hdfs(file_path, table_name, file_name):
            return False
        print("upload_parquet_file_to_hdfs done.")
        
        if not self.create_temp_table_from_parquet_file(table_name, file_name):
            return False
        print("temp table created from parquet file.")

        if not self.main_table_data_upload(table_name, file_name):
            return False
        print("data uploaded to main table.")

        if not self.main_table_refresh_metadata(table_name):
            return False
        print("main table refreshed.")

        if not self.drop_temp_table(table_name, file_name):
            return False
        print("temp table dropped.")
        
        self.delete_temp_parquet_file(file_path)
        return True

    def export_data_to_parquet_file(self, data, table_name, file_name):
        print("export_data_to_parquet_file")
        try:
            pathlib.Path(self.PARQUET_FOLDER_PATH).mkdir(exist_ok=True)
            
            new_file_path = pathlib.Path(self.PARQUET_FOLDER_PATH) / f"{table_name}_{file_name}.parquet"
            parquet_table = pa.Table.from_pandas(data, preserve_index=False)
            pq.write_table(parquet_table, where=new_file_path, version="1.0")
            return new_file_path
        except Exception as ex:
            print(f"[ERROR] --- export_data_to_parquet_file: {ex}")
            return None

    def upload_parquet_file_to_hdfs(self, file_path, table_name, file_name):
        print("upload_parquet_file_to_hdfs")
        # Ruta en HDFS. 
        # Ten en cuenta que se concatenará con self.environment["hdfs_root_folder"] internamente en FileSystemHDFS.
        hdfs_path = f"{table_name}_{file_name}"
        return self.fs.upload_file(file_path, hdfs_path)

    def create_temp_table_from_parquet_file(self, table_name, file_name):
        print("create_temp_table_from_parquet_file")
        try:
            temp_table = f"{table_name}_{file_name}"
            query = pathlib.Path("cdp_interface/sql_queries/temp_table.sql").read_text()
            query = query.replace("@temp_table", temp_table)

            if not self.db.execute(query):
                return False
            if not self.db.refresh_table(temp_table):
                return False

            cols_temp = self.db.column_list(temp_table)
            print(f"[DEBUG] Columns in temp table {temp_table} after creation:", cols_temp)
            print("#########################################################################")
            return True
        except Exception as ex:
            print(ex)
            return False

    def column_definition(self, table_name):
        table_columns = self.db.column_list(table_name)
        if not table_columns:
            return False
        # Construye un string con los nombres de columna separados por coma
        # Ej: "product_number, formula_code, product_name, ..."
        return ",".join(str(col[0]) for col in table_columns)

    def main_table_data_upload(self, table_name, file_name):
        try:
            temp_table_name = f"{table_name}_{file_name}"
            
            column_def = self.column_definition(temp_table_name)
            print(f"[DEBUG] column_def for temp table: {column_def}")
            print("#########################################################################")
            if not column_def:
                return False

            query = pathlib.Path("cdp_interface/sql_queries/data_upload.sql").read_text()
            query = query.replace("@table_name", table_name)
            query = query.replace("@temp_table_name", temp_table_name)
            query = query.replace("@column_definition", column_def)
            
            if not self.db.execute(query):
                return False

            return True
        except Exception as ex:
            print(ex)
            return False

    def main_table_refresh_metadata(self, table_name):
        print("main_table_refresh_metadata")
        try:
            if not self.db.refresh_table(table_name):
                return False
            # Si quieres compute stats
            # if not self.db.compute_stats(table_name):
            #     return False
            return True
        except Exception as ex:
            print(ex)
            return False

    def drop_temp_table(self, table_name, file_name):
        print("drop_temp_table")
        try:
            temp_table_name = f"{table_name}_{file_name}"
            self.db.drop_table(temp_table_name)
            return True
        except Exception as ex:
            print(ex)
            return False
        
    def delete_temp_parquet_file(self, file_path):
        print("deleting_temp_parquet_file")
        try:
            os.remove(file_path)
            return True
        except Exception as error:
            print(error)
            return False
