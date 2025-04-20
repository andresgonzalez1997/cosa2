# Competitor‑Pricing Pipeline — **Complete Developer Documentation**

> **Project root:** **`comp_price_p2/`**  
> **Created by:** Andrés González 
---

# Writing the full documentation to a markdown file for user download
content = """# Competitor‑Pricing Pipeline — Developer Documentation

## Table of Contents

1. [High‑level overview](#high-level-overview)  
2. [Quick‑start](#quick-start)  
3. [Project layout (Structure)](#project-layout-structure)  
4. [Runtime configuration](#runtime-configuration)  
5. [Data‑flow](#data-flow)  
6. [Script by script references](#script-by-script-references)  
   1. [`exe_process_pdf_files.py`](#exe_process_pdf_filespy)  
   2. [`competitor_data/purina_file_horizontal.py`](#competitor_data-purina_file_horizontalpy)  
   3. [`cdp_interface/hdfs.py`](#cdp_interface-hdfspy)  
   4. [`cdp_interface/impala.py`](#cdp_interface-impalapy)  
   5. [`cdp_interface/upload_data.py`](#cdp_interface-upload_datapy)  
   6. [`sharepoint_interface/sharepoint.py`](#sharepoint_interface-sharepointpy)  
   7. [`sharepoint_interface/sharepoint_interface.py`](#sharepoint_interface-sharepoint_interfacepy)  
7. [Extending the pipeline](#extending-the-pipeline)  
8. [Troubleshooting & FAQ](#troubleshooting--faq)  
9. [requirements.txt](#requirements-txt)  


---
<a id="high-level-overview"></a>
## High‑level overview
- **Source**: Purina horizontal price‑list PDFs on SharePoint  
- **ETL**: Tabula‑py + Pandas (`competitor_data/purina_file_horizontal.py`)  
- **Storage**: Parquet on HDFS → loaded into Impala  
- **Entry point**: `exe_process_pdf_files.py`

---
<a id="quick-start"></a>
## Quick‑start
1. Go to Cludera Data Science Workbench → comp_price_p2
2. Open new Session using Workbench & Python 3.7
3. Run in terminal: `pip install -r requirements.txt`  
3. Verify Java install for tabula dependencies
4. Fill in JSON files (if not filled yet):  
   - `credentials/process_account.json` (ps account in charge of running the process)  
   - `sharepoint_interface/credentials/*.json` (sharepoint credentials with permissions)  
5. Run the project:  
   ```bash
   python exe_process_pdf_files.py
   ```
<a id="project-layout-structure"></a>   
## Project layout (Structure)
```bash
comp_price_p2/
├─ exe_process_pdf_files.py
├─ competitor_data/
│  └─ purina_file_horizontal.py
├─ cdp_interface/
│  ├─ hdfs.py
│  ├─ impala.py
│  └─ upload_data.py
├─ sharepoint_interface/
│  ├─ sharepoint.py
│  └─ sharepoint_interface.py
├─ environments/
├─ credentials/
└─ requirements.txt
```
<a id="project-layout-structure"></a>
## Runtime configuration

- **environments/*.json**  
  Cluster hostnames, Impala schema name, and HDFS root directory.

- **credentials/process_account.json**  
  Service account credentials (username/password) for Impala and WebHDFS.

- **sharepoint_interface/credentials/*.json**  
  OAuth2 details (`client_id`, `client_secret`) and the SharePoint site URL.

<a id="data-flow"></a>
## Data‑flow

1. **List** PDFs in SharePoint.  
2. **Download** each PDF locally.  
3. **Parse & clean** with `read_file()` → DataFrame (19 columns).  
4. **Cast** numeric and text columns to correct types.  
5. **Write** the DataFrame to a Parquet file.  
6. **Upload** the Parquet file to HDFS.  
7. **Load** into Impala via temp table + `INSERT OVERWRITE`.  
8. **Cleanup** local and HDFS temp files.  

<a id="script-by-script-references"></a>
## Script by script references

### `exe_process_pdf_files.py`
- **Constants**  
  - `REPOSITORY`: SharePoint folder path to scan  
  - `LOCAL_REPOSITORY`: Local directory for downloaded PDFs  
- **Functions**  
  - `correct_file_name(fname)`  
    Sanitize a filename (remove spaces, accents, special chars).  
  - `set_column_types(df)`  
    Enforce DataFrame dtypes (floats for numeric columns, strings for text).  
  - `excecute_process()`  
    Main orchestration:  
    1. List PDFs in SharePoint  
    2. Download each PDF locally  
    3. Parse & clean into a DataFrame  
    4. Export to Parquet + upload to HDFS  
    5. Load into Impala  

---

### `competitor_data/purina_file_horizontal.py`
- **Reader**  
  - `read_file(pdf_path) → pd.DataFrame`  
    Reads a Purina PDF, cleans & standardizes it, returns a DataFrame with 19 columns.
- **Helpers**  
  - `_read_tables(pdf_path)`  
    Use Tabula to extract all table fragments from each page.  
  - `_standardize_table(tbl)`  
    Normalize column names, strip whitespace, drop empty rows.  
  - `_is_header_row(row)`  
    Detect and remove repeated header rows.  
  - `_fix_numeric(df, columns=None)`  
    Remove thousands separators, convert `(123)` → `-123`.  
  - `extract_effective_date(pdf_path)`  
    Scan PDF text for the date string and parse it.  
  - `extract_plant_location(pdf_path)`  
    Scan PDF text for plant location keywords (e.g. STATESVILLE).  

---

### `cdp_interface/hdfs.py`
- **Class** `FileSystemHDFS`  
  Wrapper around WebHDFS REST API:
  - `list_files(path)`: list files in an HDFS directory  
  - `upload_file(local_path, remote_path)`: PUT + APPEND to HDFS  
  - `download_file(remote_path, local_path)`: GET from HDFS  
  - `delete_file(path)`: delete an HDFS file  
  - `create_dir(path)`, `clear_dir(path)`: manage HDFS directories  
---

### `cdp_interface/impala.py`
- **Class** `Impala`  
  Thin wrapper for Impala SQL via `impyla`:
  - `select(sql, params=None) → pandas.DataFrame`  
  - `execute(sql, params=None)`: run DDL/DML statements  
  - `table_list(schema)`, `column_list(schema, table)`: metadata queries  
  - `add_column(schema, table, column_def)`: alter table  
  - `table_exists(schema, table) → bool`

---

### `cdp_interface/upload_data.py`
- **Functions**  
  - `export_data_to_parquet_file(df, output_path)`  
    Write DataFrame to a local Parquet file.  
  - `upload_parquet_file_to_hdfs(local_path, hdfs_path)`  
    Upload the Parquet file to HDFS.  
  - `create_temp_table(impala, schema, table)`  
    CREATE TABLE `tmp_<table>` LIKE `<table>`.  
  - `load_parquet_to_temp_table(impala, hdfs_path)`  
    LOAD DATA INPATH into the temp table.  
  - `insert_temp_to_main_table(impala, schema, table)`  
    INSERT OVERWRITE main table from temp.  
  - `upload_data(df, table, file_id)`  
    High‑level orchestrator that calls all of the above and handles errors.

---

### `sharepoint_interface/sharepoint.py`
- **Class** `SharePointFunctions`  
  Wrapper around Office 365 REST:
  - `files_in_folder(path)`: list files in a SharePoint folder  
  - `download_file(sp_path, local_dir)`: download a file  
  - `move_file(src, dest)`: move/rename in SharePoint  
  - `delete_file(path)`: delete from SharePoint  
  - `read_excel_file(path)`: load an Excel file into memory  

---

### `sharepoint_interface/sharepoint_interface.py`
- `get_sharepoint_interface(name)`  
  Return a configured `SharePointFunctions` instance.  
- `download_pdf_from_sharepoint(repo, dest_folder)`  
  Convenience wrapper to fetch a PDF and return its local path.  

<a id="extending-the-pipeline"></a>
## Extending the pipeline

- **Vertical PDFs**: create `competitor_data/purina_file_vertical.py` (to handle a different table layout) and in `exe_process_pdf_files.py` choose between horizontal or vertical based on the filename.  
- **Automated tests**: add a simple pytest test that calls `read_file()` and asserts the presence of key columns and at least one row.  
- **Scheduling**: schedule `python exe_process_pdf_files.py` with cron, Oozie, or Airflow so it runs automatically at your chosen interval.  
- **Alternate storage**: replace the HDFS upload helper with one that uses `boto3` to push the Parquet file to an S3 bucket.  


<a id="troubleshooting--faq"></a>
## Troubleshooting & FAQ

| Symptom| Possible Cause| Recommended Fix|
|--------------------------------------------|------------------------------------------------------|----------------------------------------------------------|
| **ModuleNotFoundError: jpype**             | • Virtual environment not activated<br>• Dependencies missing | 1. Activate your venv (`source .venv/bin/activate`)<br>2. Run `pip install -r requirements.txt` |
| **tabula.errors.JavaNotFoundError**        | • Java JDK (≥ 11) or Tabula‑java not installed<br>• Not on your system PATH | 1. Install Java 11+<br>2. Install Tabula‑java (e.g. `brew install tabula` on macOS)<br>3. Verify with `java -version` |
| **SharePoint authentication errors**       | • OAuth credentials invalid or missing               | • Update `sharepoint_interface/credentials/retailpricing_sharepoint.json` with correct `client_id` / `client_secret` |
| **Impala AuthorizationException**          | • Service account lacks SQL privileges               | • Grant the service account `SELECT` and `INSERT` on the target schema/table |
| **Build errors compiling `sasl`**          | • No C compiler or Kerberos headers (macOS/Windows)  | • Pin pure‑Python SASL: add `pure-sasl==0.6.2` to `requirements.txt` |
| **Duplicate rows after re‑run**            | • Loader used `INSERT INTO` instead of overwrite     | • Change loader logic to use `INSERT OVERWRITE` so old data is replaced |

<a id="requirements-txt"></a>
## requirements.txt
We have to run the next command to be sure we have the entire requirements installed and running in our environment: **`pip install -r requirements.txt`** 

- `tabula-py`  
  Biblioteca que envuelve la herramienta Java Tabula para **extraer tablas de archivos PDF**. En nuestro pipeline usamos tabula-py para leer las hojas de precios.

- `pandas`  
  Biblioteca principal para **manipulación y análisis de datos** en Python. Usamos `pandas.DataFrame` para limpiar, transformar y concatenar las tablas extraídas de los PDFs.

- `numpy`  
  Biblioteca de álgebra lineal y arrays multidimensionales. `pandas` la usa internamente para operaciones numéricas eficientes (por ejemplo, conversión de tipos).

- `JPype1`  
  Motor que crea un **puente (bridge) entre Python y Java**. `tabula-py` lo necesita para invocar la librería Java de Tabula desde Python.

- `pyarrow`  
  Implementación de Apache Arrow en Python, usada para **leer y escribir archivos Parquet** de forma rápida y eficiente.

- `hdfs`  
  Cliente Python para **WebHDFS**, que nos permite subir y descargar archivos (como Parquet) a HDFS mediante peticiones HTTP.

- `impyla`  
  Cliente Python para **Impala**, nos facilita conectarnos al clúster, ejecutar consultas SQL y gestionar tablas en Impala desde Python.

- `thrift-sasl`  
  Extensión de SASL para Apache Thrift, necesaria para **autenticación segura** al hablar con Impala a través de Thrift/SASL.

- `sasl`  
  Implementación de SASL (Simple Authentication and Security Layer). La requieren `thrift-sasl` e `impyla` para manejar mecanismos de autenticación (Kerberos, PLAIN, etc.).

- `Office365-REST-Python-Client`  
  Cliente que envuelve la API REST de Office 365. Lo usamos para **listar, descargar y mover** archivos en SharePoint.

- `requests`  
  Biblioteca HTTP muy popular en Python, empleada por `Office365-REST-Python-Client` (y en otras partes) para **hacer peticiones GET/POST**.

- `python-dateutil`  
  Extensión para manejar fechas y horas. Ayuda a **parsear cadenas de fecha** extraídas del PDF y a manipular objetos `datetime`.

- `openpyxl`  
  Biblioteca para **leer y escribir archivos Excel (.xlsx)**. Es opcional en este pipeline, pero útil si luego se quiere exportar resultados a Excel.
