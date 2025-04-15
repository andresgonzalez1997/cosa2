import datetime
import pandas as pd
import tabula
import re

# ----------------------------------------------------------------------
# 1) Funciones para fecha y ubicación (parte superior de la pág. 1)
# ----------------------------------------------------------------------

def effective_date(file_path):
    """
    Extrae la fecha (ej. 10/07/2024) de la parte superior de la página 1,
    devolviéndola en formato YYYY-MM-DD.
    """
    try:
        df_date = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=False,
            stream=True,
            guess=True,
            area=[50, 0, 130, 600]  # Ajustar si hace falta
        )
        if not df_date:
            return None
        text = str(df_date[0])
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
                pass
    except:
        pass
    return None

def plant_location(file_path):
    """
    Extrae la ubicación (por ejemplo, "HUDSON'S") de la parte superior de la página 1.
    """
    try:
        df_loc = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=False,
            stream=True,
            guess=True,
            area=[0, 0, 50, 600]  # Ajustar si hace falta
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
# 2) Dos funciones para extraer la tabla principal:
#    a) Para la página 1 (donde la tabla empieza más abajo)
#    b) Para las páginas 2 a 13 (tabla puede empezar un poco más arriba)
# ----------------------------------------------------------------------

def extract_table_page1(file_path):
    """
    Extrae SOLO la tabla de la página 1, usando lattice=True y un area
    que empiece ~240 pt (bastante abajo para saltar el texto 'AQUACULTURE').
    
    Ajusta '240' si aún se cuela la cabecera, o si recorta la tabla.
    """
    try:
        tables = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=True,
            guess=False,
            multiple_tables=True,
            area=[240, 0, 792, 612]  # top=240, left=0, bottom=792, right=612
        )
        return tables
    except Exception as e:
        print(f"[ERROR extract_table_page1]: {e}")
        return []

def extract_table_pages2plus(file_path):
    """
    Extrae la tabla de las páginas 2 a 13 (o 'all' si crees que va a 13+).
    La parte superior suele ser menor, así que top=100 podría servir.
    """
    try:
        tables = tabula.read_pdf(
            file_path,
            pages="2-13",         # Ajustar si son 13 páginas exactas o "2-all"
            lattice=True,
            guess=False,
            multiple_tables=True,
            area=[100, 0, 792, 612]
        )
        return tables
    except Exception as e:
        print(f"[ERROR extract_table_pages2plus]: {e}")
        return []

# ----------------------------------------------------------------------
# 3) Corrección de valores negativos con guion
# ----------------------------------------------------------------------

def correct_negative_value(value):
    """
    Convierte "100-" en -100 (float). Si no se puede convertir, deja el valor.
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
# 4) read_file: unifica todo y fuerza tipos
# ----------------------------------------------------------------------

def read_file(file_path):
    # Esquema EXACTO de la tabla en Hive/CDP
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

    numeric_cols = [col for col, ctype in schema.items() if ctype == "double"]

    # (A) Extraer DF de la página 1 (omitiendo cabecera)
    tables_p1 = extract_table_page1(file_path)

    # (B) Extraer DF de páginas 2+
    tables_p2 = extract_table_pages2plus(file_path)

    # Unimos todos en un solo DF
    all_tables = tables_p1 + tables_p2
    if not all_tables:
        # Si no salió nada, devolvemos un DF vacío con el esquema
        print("[WARN] No se extrajo ninguna tabla.")
        df_empty = pd.DataFrame()
        for col, col_type in schema.items():
            df_empty[col] = pd.Series(dtype="float" if col_type == "double" else "object")
        return df_empty

    df_main = pd.concat(all_tables, ignore_index=True)

    # (C) Corregir valores negativos en columnas numéricas
    for col in numeric_cols:
        if col in df_main.columns:
            df_main[col] = df_main[col].apply(correct_negative_value)

    # (D) Forzar tipos EXACTOS
    for col, col_type in schema.items():
        if col not in df_main.columns:
            # Creamos la columna si no existe
            df_main[col] = pd.Series(dtype="float" if col_type == "double" else "object")
        else:
            if col_type == "double":
                df_main[col] = pd.to_numeric(df_main[col], errors="coerce")
            else:
                df_main[col] = df_main[col].astype(str)

    # (E) Añadir "plant_location", "date_inserted", "source"
    loc = plant_location(file_path) or ""
    eff_date = effective_date(file_path) or ""
    df_main["plant_location"] = loc
    df_main["date_inserted"] = eff_date
    df_main["source"] = "pdf"

    # Forzar a string
    df_main["plant_location"] = df_main["plant_location"].astype(str)
    df_main["date_inserted"] = df_main["date_inserted"].astype(str)
    df_main["source"] = df_main["source"].astype(str)

    return df_main

# ----------------------------------------------------------------------
# 5) Uso (main)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    pdf_path = "2024.10.07 Statesville.pdf"  # Ajustar la ruta
    final_df = read_file(pdf_path)

    print("\n--- TIPOS DEL DATAFRAME ---")
    print(final_df.dtypes)

    print("\n--- MUESTRA DE FILAS ---")
    print(final_df.head(20))

    # Exportar a CSV
    final_df.to_csv("output_statesville.csv", index=False, encoding="utf-8-sig")
    print("\nArchivo 'output_statesville.csv' generado.")
