#Nuevo update con correcciones

import pathlib
import pyarrow.parquet as pq
import pyarrow as pa
import os

class DataUpload:
    PARQUET_FOLDER_PATH = "cdp_interface/exported_parquet_files"
  
    def __init__(self, file_system, database):
        """
        file_system = instancia de FileSystemHDFS
        database = instancia de Impala
        """
        self.fs = file_system
        self.db = database

    def upload_data(self, data, table_name, file_name):
        print(f"uploading data to {table_name} with temp suffix '{file_name}'")

        # 1) Exportar DataFrame a parquet local
        file_path = self.export_data_to_parquet_file(data, table_name, file_name)
        if file_path is None:
            return False

        # 2) (Opcional) Borrar tabla y carpeta de HDFS antes de continuar
        if not self.remove_temp_table_and_folder(table_name, file_name):
            return False

        # 3) Subir el parquet local a HDFS
        if not self.upload_parquet_file_to_hdfs(file_path, table_name, file_name):
            return False
        print("upload_parquet_file_to_hdfs done.")
        
        # 4) Crear tabla temporal (ahora carpeta limpia), refrescar y DESCRIBE
        if not self.create_temp_table_from_parquet_file(table_name, file_name):
            return False
        print("temp table created from parquet file.")

        # 5) Insertar datos en la tabla final
        if not self.main_table_data_upload(table_name, file_name):
            return False
        print("data uploaded to main table.")

        # 6) REFRESH (y opcional compute stats)
        if not self.main_table_refresh_metadata(table_name):
            return False
        print("main table refreshed.")

        # 7) Eliminar la tabla temporal (y su carpeta, si quieres)
        if not self.drop_temp_table(table_name, file_name):
            return False
        print("temp table dropped.")
        
        # 8) Borrar el parquet local
        self.delete_temp_parquet_file(file_path)
        return True

    # --------------------------------------------------------
    # Función nueva: borra la tabla en Impala y la carpeta HDFS
    # --------------------------------------------------------
    def remove_temp_table_and_folder(self, table_name, file_name):
        """
        1) Hace un DROP TABLE IF EXISTS en Impala.
        2) Elimina la carpeta en HDFS asociada a esa tabla temporal, si existe.
           (Así no quedan archivos Parquet antiguos que ensucien el nuevo esquema).
        """
        temp_table_name = f"{table_name}_{file_name}"
        try:
            print(f"[DEBUG] remove_temp_table_and_folder -> Dropping {temp_table_name} if exists")
            drop_query = f"DROP TABLE IF EXISTS @schema.{temp_table_name}"
            if not self.db.execute(drop_query):
                print("[WARN] Falló el drop table (posiblemente no existía).")

            # Construir la ruta en HDFS: 
            #   usualmente =  "/prd/internal/anh_customer_profitability/" + "comp_price_horizontal_files_etc"
            # en tu temp_table.sql -> LOCATION "@hdfs_root_folder/@temp_table"
            # => si @hdfs_root_folder = /prd/..., y @temp_table = comp_price_horizontal_files_fileName,
            #   resultará en "/prd/.../comp_price_horizontal_files_fileName"

            folder_path = f"/{temp_table_name}"  # se concatena con hdfs_root_folder internamente
            print(f"[DEBUG] remove_temp_table_and_folder -> Removing folder in HDFS: {folder_path}")
            self.fs.delete_file(folder_path)  # si no existe no pasa nada
            return True
        except Exception as ex:
            print(f"[ERROR] remove_temp_table_and_folder: {ex}")
            return False

    def export_data_to_parquet_file(self, data, table_name, file_name):
        print("export_data_to_parquet_file")
        try:
            pathlib.Path(self.PARQUET_FOLDER_PATH).mkdir(exist_ok=True)
            
            new_file_path = pathlib.Path(self.PARQUET_FOLDER_PATH) / f"{table_name}_{file_name}.parquet"
            parquet_table = pa.Table.from_pandas(data, preserve_index=False)
            pq.write_table(parquet_table, where=new_file_path, version="1.0")
            return new_file_path
        except Exception as ex:
            print(f"[ERROR] export_data_to_parquet_file: {ex}")
            return None

    def upload_parquet_file_to_hdfs(self, file_path, table_name, file_name):
        print("upload_parquet_file_to_hdfs")
        # Esto genera la carpeta en HDFS = "/{table_name}_{file_name}"
        # concatenado a self.environment["hdfs_root_folder"], 
        #   si no existe, la crea, y sube tu .parquet
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
        # Construye un string con nombres de columna. E.g. "product_number, formula_code, product_form, ..."
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
            # Si quieres compute stats también:
            # if not self.db.compute_stats(table_name):
            #     return False
            return True
        except Exception as ex:
            print(ex)
            return False

    def drop_temp_table(self, table_name, file_name):
        print("drop_temp_table")
        """
        Elimina la tabla temporal del metastore de Impala.
        Nota: la carpeta en HDFS la eliminamos antes (remove_temp_table_and_folder).
        """
        try:
            temp_table_name = f"{table_name}_{file_name}"
            drop_query = f"DROP TABLE IF EXISTS @schema.{temp_table_name}"
            self.db.execute(drop_query)
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
