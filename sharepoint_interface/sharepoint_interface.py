import json
from sharepoint_interface.sharepoint import SharePointFunctions
import os

def get_sharepoint_interface(sharepoint_name):
    credentials = None
    
    if str(sharepoint_name).lower() == "retailpricing":
        with open("sharepoint_interface/credentials/retailpricing_sharepoint.json") as f:
            credentials = json.load(f)
    
    if not credentials: return False
    sp = SharePointFunctions(credentials)
    return sp

def download_pdf_from_sharepoint(repository_path, local_folder):
    """
    1. Crea la instancia de SharePointFunctions con get_sharepoint_interface("retailpricing").
    2. Lista los archivos en la carpeta 'repository_path' de SharePoint.
    3. Busca el primer archivo PDF disponible.
    4. Si existe, lo descarga al 'local_folder'.
    5. Retorna la ruta completa del PDF guardado localmente o None si algo falla.
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

    # Filtrar archivos PDF
    pdf_files = [f for f in files if f["file_name"].lower().endswith(".pdf")]
    if not pdf_files:
        print(f"[INFO] No se encontraron PDF en: {repository_path}")
        return None

    # Tomar el primero (si necesitas el más reciente, aquí puedes ordenar por 'last_modified')
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