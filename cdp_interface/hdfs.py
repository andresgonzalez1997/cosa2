import pathlib
import pandas as pd
import pathlib

from hdfs.client import Client
from requests import Session
from requests.auth import HTTPBasicAuth

class FileSystemHDFS:
    def __init__(self, environment, credentials):
        self.environment = environment
        self.credentials = credentials
        
    def session(self):
        session = Session()
        session.trus_env = False
        session.auth = HTTPBasicAuth(
            self.credentials["username"],
            self.credentials["password"]
        )
        return session

    def client(self, session):
        return Client(
            url = self.environment["webhdfs"],
            root = self.environment["hdfs_root_folder"],
            session = session
        )
    
    def create_dir(self, fs, path):
        new_dir = pathlib.PurePosixPath(path)
        if not fs.status(new_dir, strict=False):
            fs.makedirs(new_dir)
        return True

    def list_files(self, folder_path = "."):
        with self.session() as s:
            hdfs = self.client(s)
            files = hdfs.list(folder_path)
            return files

    def download_file(self, file_path, destination_folder):
        _destination_folder = pathlib.Path(destination_folder)

        if not _destination_folder.exists(): _destination_folder.mkdir()
        
        with self.session() as s:
            hdfs = self.client(s)
            return hdfs.download(file_path, destination_folder, n_threads=0, overwrite=True)

    def upload_file(self, file_path, destination_path):
        with self.session() as s:
            fs = self.client(s)
            self.create_dir(fs, destination_path)
            destination_path = fs.upload(destination_path, file_path, n_threads=0, overwrite=True)
        return destination_path

            
    def delete_file(self, file_path):
        with self.session() as s:
            fs = self.client(s)
            fs.delete(file_path, recursive = True)
        return True
    
    def clear_dir(self, path):
        with self.session() as s:
            fs = self.client(s)
            
            files_in_dir = self.list_files(path)
            for x in files_in_dir: self.delete_file(x)
            
        return True
            