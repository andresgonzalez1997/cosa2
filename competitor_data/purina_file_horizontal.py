import datetime
import pandas as pd
import tabula
import re

# --------------------------------------------------------------------------------
# 1) Funciones para fecha y ubicación (parte superior de la página 1)
# --------------------------------------------------------------------------------

def effective_date(file_path):
    """
    Extrae la fecha (ej. 10/07/2024) desde un área pequeña en la página 1.
    La retorna en formato YYYY-MM-DD o None si no se encuentra.
    """
    try:
        df_date = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=False,       # No usa líneas, solo stream
            stream=True,
            guess=True,
            area=[50, 0, 130, 600]  # (top=50, left=0, bottom=130, right=600)
        )
        if not df_date:
            return None

        text = str(df_date[0])
        date_pat = re.compile(r"\d{1,2}/\d{1,2}/(\d{4}|\d{2})")
        match = date_pat.search(text)
        if not match:
            return None

        date_str = match.group(0)
        # Probar parsear con 2 formatos: mm/dd/yyyy y mm/dd/yy
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                dt = datetime.datetime.strptime(date_str, fmt).date()
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
    except:
        pass
    return None

def plant_location(file_path):
    """
    Extrae la ubicación (ej. "HUDSON'S") desde un área pequeña en la página 1.
    Retorna la primera línea, en mayúsculas, o None.
    """
    try:
        df_loc = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=False,
            stream=True,
            guess=True,
            area=[0, 0, 50, 600]  # (top=0, left=0, bottom=50, right=600)
        )
        if not df_loc:
            return None

        text = str(df_loc[0]).upper()
        if "HUDSON'S" in text:
            return "HUDSON'S"

        first_line = text.split("\n")[0].strip()
        return first_line.replace(",", "").strip()
    except:
        return None

# --------------------------------------------------------------------------------
# 2) Función para extraer la tabla principal de TODAS las páginas
# --------------------------------------------------------------------------------

def extract_main_table(file_path):
    """
    Extrae la tabla principal en TODAS las páginas,
    omitiendo ~100 pt superiores para no incluir cabeceras.
    """
    try:
        # Ajuste principal: area=[100, 0, 792, 612] (Carta=612x792)
        #  - top=100, left=0, bottom=792, right=612
        # lattice=True para usar líneas de la tabla, guess=False para no 'adivinar'.
        tables = tabula.read_pdf(
            file_path,
            pages="all",
            lattice=True,
            guess=True,
            multiple_tables=True
            # stream = True
            # area=[180, 20, 195.40, 765.20]
        )
        return tables
    except Exception as e:
        print(f"[ERROR extract_main_table]: {e}")
        return []

# --------------------------------------------------------------------------------
# 3) Corrección de valores negativos con guion
# --------------------------------------------------------------------------------

def correct_negative_value(value):
    """
    Convierte "100-" en -100 (float). Si no se puede convertir, retorna el valor original.
    """
    text = str(value).strip()
    if text.endswith("-"):
        num = text[:-1]
        try:
            return float(num) * -1
        except ValueError:
            return value
    else:
        try:
            return float(text)
        except ValueError:
            return value

# --------------------------------------------------------------------------------
# 4) Función principal: unifica todo, ajusta schema, extrae datos finales
# --------------------------------------------------------------------------------

def read_file(file_path):
    """
    - Extrae la tabla principal (todas las páginas) omitiendo ~100 pt superiores.
    - Concatena en un DF.
    - Corrige valores negativos en las columnas numéricas.
    - Crea/ajusta columnas según 'schema' (string/double).
    - Inserta 'plant_location', 'date_inserted' y 'source'='pdf'.
    - Retorna el DF final con las columnas en el orden del 'schema'.
    """

    # Esquema EXACTO para tu tabla en Hive/CDP
    schema = {
        "product_number": "string",
        "formula_code": "string",
        "product_name": "string",
        "product_form": "string",
        "unit_weight": "double",
        "pallet_quantity": "double",
        "stocking_status": "string",
        "min_order_quantity": "double",
        "days_lead_time": "double",
        "fob_or_dlv": "string",
        "price_change": "double",
        "list_price": "double",
        "full_pallet_price": "double",
        "half_load_full_pallet_price": "double",
        "full_load_full_pallet_price": "double",
        "full_load_best_price": "double",
        "plant_location": "string",
        "date_inserted": "string",
        "source": "string"
    }

    # Columnas numéricas
    numeric_cols = [col for col, ctype in schema.items() if ctype == "double"]

    # (a) Extrae todas las tablas (páginas)
    main_tables = extract_main_table(file_path)
    if not main_tables:
        print("[WARN] No se extrajo ninguna tabla principal: regresamos DF vacío.")
        df_empty = pd.DataFrame()
        for col, col_type in schema.items():
            if col_type == "double":
                df_empty[col] = pd.Series(dtype="float")
            else:
                df_empty[col] = pd.Series(dtype="object")
        return df_empty

    # Concatena todos los DataFrames
    df_main = pd.concat(main_tables, ignore_index=True)

    # (b) Corrige negativos en columnas numéricas
    for col in numeric_cols:
        if col in df_main.columns:
            df_main[col] = df_main[col].apply(correct_negative_value)

    # (c) Asegurar que TODAS las columnas del 'schema' existan y tengan el tipo
    for col, col_type in schema.items():
        if col not in df_main.columns:
            # Crear la columna vacía si no existe
            if col_type == "double":
                df_main[col] = pd.Series(dtype="float")
            else:
                df_main[col] = pd.Series(dtype="object")
        else:
            # Convertir a float o string según sea el caso
            if col_type == "double":
                df_main[col] = pd.to_numeric(df_main[col], errors="coerce")
            else:
                df_main[col] = df_main[col].astype(str)

    # (d) Agrega 'plant_location', 'date_inserted' y 'source'
    loc = plant_location(file_path) or ""
    eff_date = effective_date(file_path) or ""

    df_main["plant_location"] = str(loc)
    df_main["date_inserted"] = str(eff_date)
    df_main["source"] = "pdf"

    # Aseguramos tipo string
    df_main["plant_location"] = df_main["plant_location"].astype(str)
    df_main["date_inserted"] = df_main["date_inserted"].astype(str)
    df_main["source"] = df_main["source"].astype(str)

    # (e) Reordenar columnas al orden exacto del 'schema'
    final_cols = list(schema.keys())  # el orden definido en schema
    # Si Tabula crea columnas extra, las ignoramos
    final_cols = [c for c in final_cols if c in df_main.columns]
    df_final = df_main[final_cols]

    return df_final

----------------









import fitz  # PyMuPDF
import datetime
import re
import pandas as pd

# --------------------------------------------------------------------------------
# 1) Funciones para fecha y ubicación (parte superior de la página 1)
# --------------------------------------------------------------------------------

def effective_date(file_path):
    """
    Extrae la fecha (p.ej. 10/07/2024) desde una zona definida en la página 1.
    Devuelve la fecha en formato 'YYYY-MM-DD' o None si no la encuentra.
    """
    try:
        doc = fitz.open(file_path)
        page = doc.load_page(0)
        # Recorta entre y=50 y y=130 pt para aislar la zona de fecha
        rect = fitz.Rect(0, 50, page.rect.width, 130)
        text = page.get_text("text", clip=rect)
        # Busca mm/dd/yyyy o mm/dd/yy
        m = re.search(r"\b\d{1,2}/\d{1,2}/(?:\d{4}|\d{2})\b", text)
        if not m:
            return None
        date_str = m.group(0)
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                dt = datetime.datetime.strptime(date_str, fmt).date()
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return None

def plant_location(file_path):
    """
    Extrae la ubicación (p.ej. "HUDSON'S") desde una zona definida en la página 1.
    Devuelve la primera línea encontrada (en mayúsculas), o None si falla.
    """
    try:
        doc = fitz.open(file_path)
        page = doc.load_page(0)
        # Recorta entre y=0 y y=50 pt para aislar la zona de ubicación
        rect = fitz.Rect(0, 0, page.rect.width, 50)
        text = page.get_text("text", clip=rect).upper()
        if "HUDSON'S" in text:
            return "HUDSON'S"
        primera = text.split("\n")[0].strip()
        return primera.replace(",", "").strip()
    except Exception:
        return None

# --------------------------------------------------------------------------------
# 2) Función para extraer la “tabla” de TODAS las páginas
# --------------------------------------------------------------------------------

def extract_main_table(file_path, header_offset=100):
    """
    Lee cada página recortando los primeros header_offset pt para omitir cabeceras,
    luego divide cada línea por 2+ espacios y normaliza las filas.
    Devuelve lista con un solo DataFrame resultante.
    """
    rows = []
    doc = fitz.open(file_path)
    for page in doc:
        clip = fitz.Rect(0, header_offset, page.rect.width, page.rect.height)
        text = page.get_text("text", clip=clip)
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            # Divide donde hay 2 o más espacios consecutivos
            cols = re.split(r"\s{2,}", line)
            rows.append(cols)

    if not rows:
        return []

    # Cabecera en la primera fila
    header = rows[0]
    n_cols = len(header)
    # Diagnóstico de distribuciones de columnas
    lengths = sorted(set(len(r) for r in rows))
    print(f"[DEBUG] Longitudes de fila encontradas: {lengths}")

    # Normaliza datos: igual número de columnas por fila
    data = []
    for r in rows[1:]:
        if len(r) < n_cols:
            r = r + [""] * (n_cols - len(r))
        elif len(r) > n_cols:
            # Agrupa el exceso en la última columna
            r = r[: n_cols - 1] + [" ".join(r[n_cols - 1 :])]
        data.append(r)

    # Normaliza nombres de columnas
    cols_norm = [h.lower().strip().replace(" ", "_") for h in header]
    df = pd.DataFrame(data, columns=cols_norm)
    return [df]

# --------------------------------------------------------------------------------
# 3) Corrección de valores negativos con guion
# --------------------------------------------------------------------------------

def correct_negative_value(value):
    """
    Convierte "100-" en -100 (float). Si falla, retorna el valor original.
    """
    txt = str(value).strip()
    if txt.endswith("-"):
        num = txt[:-1]
        try:
            return float(num) * -1
        except ValueError:
            return value
    try:
        return float(txt)
    except ValueError:
        return value

# --------------------------------------------------------------------------------
# 4) Función principal: unifica todo, ajusta esquema, extrae datos finales
# --------------------------------------------------------------------------------

def read_file(file_path):
    """
    - Extrae la tabla principal con extract_main_table()
    - Concatena en un DataFrame
    - Corrige valores negativos
    - Asegura columnas según schema (tipos y existencia)
    - Agrega plant_location, date_inserted y source
    - Reordena columnas al orden del schema
    - Devuelve el DataFrame final
    """
    schema = {
        "product_number": "string",
        "formula_code": "string",
        "product_name": "string",
        "product_form": "string",
        "unit_weight": "double",
        "pallet_quantity": "double",
        "stocking_status": "string",
        "min_order_quantity": "double",
        "days_lead_time": "double",
        "fob_or_dlv": "string",
        "price_change": "double",
        "list_price": "double",
        "full_pallet_price": "double",
        "half_load_full_pallet_price": "double",
        "full_load_full_pallet_price": "double",
        "full_load_best_price": "double",
        "plant_location": "string",
        "date_inserted": "string",
        "source": "string",
    }
    numeric_cols = [c for c, t in schema.items() if t == "double"]

    # a) Extraer tablas
    tables = extract_main_table(file_path)
    if not tables:
        print("[WARN] No se extrajo ninguna tabla; devolviendo DataFrame vacío.")
        return pd.DataFrame({
            col: pd.Series(dtype="float" if typ=="double" else "object")
            for col,typ in schema.items()
        })

    # b) Concatenar
    df = pd.concat(tables, ignore_index=True)

    # c) Corregir negativos
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(correct_negative_value)

    # d) Asegurar todas las columnas y sus tipos
    for col, typ in schema.items():
        if col not in df.columns:
            df[col] = pd.Series(dtype="float" if typ=="double" else "object")
        else:
            if typ == "double":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(str)

    # e) Agregar metadatos
    df["plant_location"] = plant_location(file_path) or ""
    df["date_inserted"]  = effective_date(file_path) or ""
    df["source"]         = "pdf"

    # f) Reordenar columnas según el schema
    final_order = [c for c in schema.keys() if c in df.columns]
    return df[final_order]


# --------------------------------------------------------------------------------
# 5) Bloque principal de ejecución
# --------------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Uso: python purina_parser.py <ruta_al_pdf>")
        sys.exit(1)

    pdf_path = sys.argv[1]
    df_final = read_file(pdf_path)

    # Salida de diagnóstico
    print("\n--- TIPOS DEL DATAFRAME ---")
    print(df_final.dtypes, "\n")
    print("--- INFO DEL DATAFRAME FINAL ---")
    print(df_final.info(), "\n")
    print("--- MUESTRA DE FILAS ---")
    print(df_final.head())
