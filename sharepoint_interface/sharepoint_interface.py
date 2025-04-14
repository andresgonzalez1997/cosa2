import json
from sharepoint_interface.sharepoint import SharePointFunctions
import os

def get_sharepoint_interface(sharepoint_name):
    """
    Carga credenciales según el nombre 'retailpricing' u otros que quieras manejar.
    Retorna una instancia de SharePointFunctions o None/False si no hay credenciales.
    """
    credentials = None
    
    # Ejemplo: si quieres más casos, agrégalos
    if str(sharepoint_name).lower() == "retailpricing":
        with open("sharepoint_interface/credentials/retailpricing_sharepoint.json", "r") as f:
            credentials = json.load(f)
    
    if not credentials:
        return False

    sp = SharePointFunctions(credentials)
    return sp

def download_pdf_from_sharepoint(repository_path, local_folder):
    """
    1. Crea la instancia con get_sharepoint_interface("retailpricing").
    2. Lista archivos en 'repository_path' de SharePoint.
    3. Busca el primer PDF disponible.
    4. Si existe, lo descarga a 'local_folder'.
    5. Retorna ruta local del PDF o None si falla.
    """
    sp = get_sharepoint_interface("retailpricing")
    if not sp:
        print("[ERROR] No se pudo obtener la interfaz de SharePoint.")
        return None
    
    try:
        files = sp.files_in_folder(repository_path)
    except Exception as e:
        print(f"[ERROR] Ocurrió un problema listando archivos de SharePoint: {e}")
        return None

    if not files:
        print(f"[INFO] No hay archivos en la carpeta: {repository_path}")
        return None

    # Filtrar PDF
    pdf_files = [f for f in files if f["file_name"].lower().endswith(".pdf")]
    if not pdf_files:
        print(f"[INFO] No se encontraron PDF en: {repository_path}")
        return None

    # Tomar el primero
    pdf_to_download = pdf_files[0]
    pdf_path = pdf_to_download["file_path"]

    # Crear carpeta local si no existe
    if not os.path.exists(local_folder):
        os.makedirs(local_folder, exist_ok=True)

    # Descargar
    try:
        local_file_path = sp.download_file(pdf_path, local_folder)
        print(f"[INFO] Archivo descargado en: {local_file_path}")
        return str(local_file_path)
    except Exception as e:
        print(f"[ERROR] Al descargar PDF: {e}")
        return None
