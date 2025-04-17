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










import datetime
import re
import pandas as pd
import camelot

# --------------------------------------------------------------------------------
# 1) Funciones para fecha y ubicación (parte superior de la página 1)
# --------------------------------------------------------------------------------

def effective_date(file_path):
    """
    Extrae la fecha (ej. 10/07/2024) desde un área pequeña en la página 1.
    La retorna en formato YYYY-MM-DD o None si no se encuentra.
    """
    try:
        # Coordenadas adaptadas de Tabula [top, left, bottom, right] 
        # a Camelot ["x1,y1,x2,y2"] (origin en la esquina inferior izquierda).
        # Para página carta (792 pt de altura):
        #   área Tabula [50, 0, 130, 600] → Camelot "0,662,600,742"
        tables = camelot.read_pdf(
            file_path,
            pages="1",
            flavor="stream",
            table_areas=["0,662,600,742"],
            strip_text="\n"
        )
        if len(tables) == 0:
            return None

        # Tomamos la primera tabla y la convertimos a texto plano
        df_date = tables[0].df
        text = "\n".join(df_date.fillna("").astype(str).values.flatten())

        # Buscamos patrón de fecha mm/dd/yyyy o mm/dd/yy
        date_pat = re.compile(r"\d{1,2}/\d{1,2}/(\d{4}|\d{2})")
        match = date_pat.search(text)
        if not match:
            return None

        date_str = match.group(0)
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
    Extrae la ubicación (ej. "HUDSON'S") desde un área pequeña en la página 1.
    Retorna la primera línea en mayúsculas o None.
    """
    try:
        # Área Tabula [0, 0, 50, 600] → Camelot "0,742,600,792"
        tables = camelot.read_pdf(
            file_path,
            pages="1",
            flavor="stream",
            table_areas=["0,742,600,792"],
            strip_text="\n"
        )
        if len(tables) == 0:
            return None

        df_loc = tables[0].df
        text = "\n".join(df_loc.fillna("").astype(str).values.flatten()).upper()

        if "HUDSON'S" in text:
            return "HUDSON'S"

        first_line = text.split("\n")[0].strip()
        return first_line.replace(",", "").strip()
    except Exception:
        return None

# --------------------------------------------------------------------------------
# 2) Función para extraer la tabla principal de TODAS las páginas
# --------------------------------------------------------------------------------

def extract_main_table(file_path):
    """
    Extrae la tabla principal en TODAS las páginas usando líneas de cuadrícula.
    Retorna una lista de DataFrames, uno por cada tabla detectada.
    """
    try:
        tables = camelot.read_pdf(
            file_path,
            pages="1-end",
            flavor="lattice",
            strip_text="\n"
        )
        # Devolvemos solo los DataFrames
        return [t.df for t in tables]
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
        # Generar DF vacío con schema
        df_empty = pd.DataFrame({
            col: pd.Series(dtype="float" if typ=="double" else "object")
            for col, typ in schema.items()
        })
        return df_empty

    # (b) Concatena todos los DataFrames
    df_main = pd.concat(main_tables, ignore_index=True)

    # (c) Corrige negativos en columnas numéricas
    for col in numeric_cols:
        if col in df_main.columns:
            df_main[col] = df_main[col].apply(correct_negative_value)

    # (d) Asegurar que TODAS las columnas del 'schema' existan y tengan el tipo correcto
    for col, col_type in schema.items():
        if col not in df_main.columns:
            # Crear la columna vacía si no existe
            df_main[col] = pd.Series(dtype="float" if col_type=="double" else "object")
        else:
            # Convertir a float o string según sea el caso
            if col_type == "double":
                df_main[col] = pd.to_numeric(df_main[col], errors="coerce")
            else:
                df_main[col] = df_main[col].astype(str)

    # (e) Agrega 'plant_location', 'date_inserted' y 'source'
    loc = plant_location(file_path) or ""
    eff_date = effective_date(file_path) or ""
    df_main["plant_location"] = str(loc)
    df_main["date_inserted"]   = str(eff_date)
    df_main["source"]          = "pdf"

    # (f) Reordenar columnas al orden exacto del 'schema'
    final_cols = [c for c in schema.keys() if c in df_main.columns]
    df_final = df_main[final_cols]

    return df_final
