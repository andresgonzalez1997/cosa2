from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.sharepoint.files.move_operations import MoveOperations
import io
import pandas as pd
import pathlib

class SharePointFunctions:
    def __init__(self, credentials):
        self.client_id = credentials["client_id"]
        self.client_secret = credentials["client_secret"]
        self.sharepoint_url = credentials["sharepoint_url"]
        
    def get_context(self):
        ctx_auth = AuthenticationContext(self.sharepoint_url)
        token = ctx_auth.acquire_token_for_app(
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        if not token:
            raise Exception(f"Authentication error: {ctx_auth.get_last_error()}")
        
        ctx = ClientContext(self.sharepoint_url, ctx_auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        return ctx
    
    def files_in_folder(self, folder_path):
        """
        Retorna un listado con info de cada archivo en la carpeta de SharePoint.
        """
        ctx = self.get_context()
        folder = ctx.web.get_folder_by_server_relative_url(folder_path)
        files = folder.files
        ctx.load(files)
        ctx.execute_query()
        
        file_list = []
        for f in files:
            file = (
                ctx.web
                .get_file_by_server_relative_url(f.properties["ServerRelativeUrl"])
                .expand(["ModifiedBy", "Author", "TimeCreated", "TimeLastModified"])
                .get()
                .execute_query()
            )
            file_details = {
                "file_path": f.properties["ServerRelativeUrl"],
                "file_name": file.name,
                "modified_by": str(file.modified_by),
                "modified_by_email": file.modified_by.email,
                "last_modified": file.time_last_modified
            }
            file_list.append(file_details)
            
        return file_list
    
    def read_excel_file(self, file_path, sheet_name=None):
        """
        Ejemplo para leer un Excel directamente desde SharePoint a un DataFrame.
        """
        ctx = self.get_context()
        response = File.open_binary(ctx, file_path)
        bytes_file_obj = io.BytesIO()
        bytes_file_obj.write(response.content)
        bytes_file_obj.seek(0)
        
        if not sheet_name:
            df = pd.read_excel(bytes_file_obj, engine="openpyxl", header=0, sheet_name=0)
        else:
            df = pd.read_excel(bytes_file_obj, engine="openpyxl", header=0, sheet_name=sheet_name)
        return df
    
    def move_file(self, file_path, destination_path):
        """
        Mueve el archivo a otra carpeta en SharePoint (ej: para archivar).
        """
        try:
            ctx = self.get_context()
            file = ctx.web.get_file_by_server_relative_url(file_path)
            file_to = file.move_to_using_path(destination_path, MoveOperations.overwrite).execute_query()
            return True
        except Exception as e:
            print(e)
            return False
    
    def delete_file(self, file_path):
        """
        Envía el archivo a la papelera de reciclaje de SharePoint (no lo borra permanentemente).
        """
        try:
            ctx = self.get_context()
            file = ctx.web.get_file_by_server_relative_url(file_path)
            file.recycle().execute_query()
            return True
        except Exception as e:
            print(e)
            return False
    
    def download_file(self, file_path, destination_folder):
        """
        Descarga el archivo a la carpeta local especificada.
        """
        file_name = pathlib.Path(file_path).name
        destination_file_path = pathlib.Path(destination_folder) / file_name
        destination = str(destination_file_path)
        
        try:
            ctx = self.get_context()
            with open(destination, "wb") as local_file:
                (
                    ctx.web
                    .get_file_by_server_relative_url(file_path)
                    .download(local_file)
                    .execute_query()
                )
            return destination_file_path
        except Exception as e:
            print(e)
            return None
