import datetime
import pandas as pd
import tabula
import re

# ----------------------------------------------------------------------
# 1) Funciones para fecha y ubicación (parte superior)
# ----------------------------------------------------------------------

def effective_date(file_path):
    """
    Extrae la fecha (ej. 10/07/2024) desde un área pequeña en la página 1.
    La retorna en formato YYYY-MM-DD o None.
    """
    try:
        # Área pequeña (en puntos) donde suele aparecer "Effective Date ...".
        # Ajustado para la parte superior de la primera página.
        df_date = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=False,
            stream=True,
            guess=True,
            area=[50, 0, 130, 600]  # top=50, left=0, bottom=130, right=600
        )
        if not df_date:
            return None
        
        text = str(df_date[0])
        # Buscar algo como mm/dd/yyyy o mm/dd/yy
        date_pat = re.compile(r"\d{1,2}/\d{1,2}/(\d{4}|\d{2})")
        match = date_pat.search(text)
        if not match:
            return None
        date_str = match.group(0)
        # Intentar parsear con 2 formatos
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
    Extrae la ubicación (por ejemplo, "HUDSON'S") desde un área pequeña (top) en la página 1.
    """
    try:
        df_loc = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=False,
            stream=True,
            guess=True,
            area=[0, 0, 50, 600]  # top=0, left=0, bottom=50, right=600
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

# ----------------------------------------------------------------------
# 2) Función para extraer la tabla principal
# ----------------------------------------------------------------------

def extract_main_table(file_path):
    """
    Extrae la tabla principal de TODAS las páginas (1-13),
    usando lattice=True y un área que omite el texto superior (hasta ~140 pt).
    """
    try:
        # Para un PDF de tamaño carta (~612 x 792 pt),
        # definimos un area que empieza en y=140 y termina en y=792.
        # left=0 y right=612 para tomar todo el ancho.
        tables = tabula.read_pdf(
            file_path,
            pages="all",        # Todo el PDF
            lattice=True,       # Usa las líneas de la tabla
            guess=False,        # No adivines las separaciones
            multiple_tables=True,
            area=[140, 0, 792, 612]  # [top, left, bottom, right]
        )
        return tables
    except Exception as e:
        print(f"[ERROR extract_main_table]: {e}")
        return []

# ----------------------------------------------------------------------
# 3) Corrección de valores negativos con guion
# ----------------------------------------------------------------------

def correct_negative_value(value):
    """
    Convierte "100-" en -100 (float). Si no se puede convertir, retorna el valor original.
    """
    text = str(value).strip()
    if text.endswith("-"):
        text_sin_guion = text[:-1]
        try:
            return float(text_sin_guion) * -1
        except ValueError:
            return value
    else:
        try:
            return float(text)
        except ValueError:
            return value

# ----------------------------------------------------------------------
# 4) Función principal: unifica todo, forza tipos, etc.
# ----------------------------------------------------------------------

def read_file(file_path):
    """
    1) Extrae la tabla principal usando extract_main_table (omitiendo cabecera).
    2) Extrae la fecha y la ubicación por separado.
    3) Unifica todo en un DataFrame con el esquema de Hive/CDP.
    4) Retorna el DataFrame final.
    """

    # Esquema EXACTO que quieres en Hive/CDP
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

    numeric_cols = [c for c, t in schema.items() if t == "double"]

    # (a) Extraer las tablas "grandes" (todas las páginas, omitiendo la parte superior)
    main_tables = extract_main_table(file_path)
    if not main_tables:
        print("[WARN] No se extrajo ninguna tabla principal.")
        # Retorno un DF vacío con las columnas definidas en el schema
        df_empty = pd.DataFrame()
        for col, col_type in schema.items():
            if col_type == "double":
                df_empty[col] = pd.Series(dtype="float")
            else:
                df_empty[col] = pd.Series(dtype="object")
        return df_empty

    # Concatenar
    df_main = pd.concat(main_tables, ignore_index=True)

    # (b) Corregir valores negativos en las columnas numéricas
    for col in numeric_cols:
        if col in df_main.columns:
            df_main[col] = df_main[col].apply(correct_negative_value)

    # (c) Forzar tipos EXACTOS
    for col, col_type in schema.items():
        if col not in df_main.columns:
            # crear la columna vacía si no existe
            if col_type == "double":
                df_main[col] = pd.Series(dtype="float")
            else:
                df_main[col] = pd.Series(dtype="object")
        else:
            if col_type == "double":
                df_main[col] = pd.to_numeric(df_main[col], errors="coerce")
            else:
                df_main[col] = df_main[col].astype(str)

    # (d) Extraer 'plant_location' y 'date_inserted'
    loc = plant_location(file_path) or ""
    eff_date = effective_date(file_path) or ""
    df_main["plant_location"] = str(loc)
    df_main["date_inserted"] = str(eff_date)
    df_main["source"] = "pdf"

    # Asegurar que sean string
    df_main["plant_location"] = df_main["plant_location"].astype(str)
    df_main["date_inserted"] = df_main["date_inserted"].astype(str)
    df_main["source"] = df_main["source"].astype(str)

    return df_main

# ----------------------------------------------------------------------
# 5) Uso (main)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    pdf_path = "2024.10.07 Statesville.pdf"  # Ajusta tu ruta
    final_df = read_file(pdf_path)

    print("\n--- TIPOS DEL DATAFRAME ---")
    print(final_df.dtypes)

    print("\n--- MUESTRA DE FILAS ---")
    print(final_df.head(20))

    # Exportar a CSV
    final_df.to_csv("output_statesville.csv", index=False, encoding="utf-8-sig")
    print("\nArchivo 'output_statesville.csv' generado.")
