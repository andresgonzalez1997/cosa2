from cdp_interface.impala import Impala
from cdp_interface.hdfs import FileSystemHDFS
from cdp_interface.upload_data import DataUpload

class CDPInterface():
    def __init__(self, env, credentials):
        self.env = env
        self.credentials = credentials

    def select(self, query):
        impala = Impala(self.env)
        return impala.select(query)

    def execute(self, query):
        impala = Impala(self.env)
        return impala.execute(query)
    
    def list_files(self, path):
        hdfs = FileSystemHDFS(self.env, self.credentials)
        return hdfs.list_files(path)
    
    def download_file(self, file_path, destination_path):
        hdfs = FileSystemHDFS(self.env, self.credentials)
        return hdfs.download_file(file_path, destination_path)
    
    def delete_file(self, file_path):
        hdfs = FileSystemHDFS(self.env, self.credentials)
        return hdfs.delete_file(file_path)
    
    def upload_data(self, data, table_name, file_name):
        uploader = DataUpload(FileSystemHDFS(self.env, self.credentials), Impala(self.env))
        return uploader.upload_data(data, table_name, file_name)
    