import datetime
import pandas as pd
import tabula
import re

# --------------------------------------------------------------------------------
# 1) Funciones para fecha y ubicación (parte superior)
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
            lattice=False,     # sin usar las líneas
            stream=True,       # modo 'stream'
            guess=True,        # Tabula "adivina" separaciones
            area=[50, 0, 130, 600]  # [top, left, bottom, right] en pts
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
    Retorna la primera línea en mayúsculas, o None.
    """
    try:
        df_loc = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=False,
            stream=True,
            guess=True,
            area=[0, 0, 50, 600]  # [top, left, bottom, right]
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
# 2) Función para extraer la tabla principal (todas las páginas)
# --------------------------------------------------------------------------------

def extract_main_table(file_path):
    """
    Extrae la tabla principal de TODAS las páginas (1-13, o "all"),
    omitiendo ~140 pt superiores para no incluir cabeceras.
    Usa lattice=True para aprovechar líneas de la tabla.
    """
    try:
        # 'area' = [top, left, bottom, right]
        tables = tabula.read_pdf(
            file_path,
            pages="all",
            lattice=True,
            guess=False,        # sin adivinar separaciones (usa las líneas)
            multiple_tables=True,
            area=[140, 0, 792, 612]  # Carta: 612x792, quitamos 140 pt de arriba
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
        num = text[:-1]  # quita el "-"
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
# 4) Función principal: une todo, ajusta schema, extrae datos finales
# --------------------------------------------------------------------------------

def read_file(file_path):
    """
    - Extrae la tabla principal (todas las páginas) omitiendo cabecera.
    - Concatena en un DF.
    - Corrige valores negativos en las columnas numéricas.
    - Crea o ajusta columnas faltantes según 'schema'.
    - Fuerza tipos EXACTOS (string/double).
    - Inserta 'plant_location', 'date_inserted' y 'source'='pdf'.
    - Retorna el DF final con las columnas en el orden del 'schema'.
    """

    # Esquema EXACTO para la tabla en Hive/CDP
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

    # Columnas numéricas (double) para hacer correct_negative_value
    numeric_cols = [col for col, ctype in schema.items() if ctype == "double"]

    # (a) Extraer todas las tablas omitiendo la parte superior
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

    # (c) Forzar que TODAS las columnas del 'schema' existan y tengan el tipo correcto
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

    # (d) Agregamos 'plant_location', 'date_inserted' y 'source'
    loc = plant_location(file_path) or ""
    eff_date = effective_date(file_path) or ""

    df_main["plant_location"] = str(loc)
    df_main["date_inserted"] = str(eff_date)
    df_main["source"] = "pdf"

    # Asegurar que sean string
    df_main["plant_location"] = df_main["plant_location"].astype(str)
    df_main["date_inserted"] = df_main["date_inserted"].astype(str)
    df_main["source"] = df_main["source"].astype(str)

    # (e) Reordenar columnas al orden exacto del schema (para no tener extras en el DF)
    final_cols = list(schema.keys())
    # Si Tabula detectó columnas 'Unnamed: 0' u otras no definidas en el schema,
    # las ignoraremos. Filtramos final_cols que existan realmente:
    final_cols = [c for c in final_cols if c in df_main.columns]

    # Tomamos solo ésas en el orden
    df_final = df_main[final_cols]

    return df_final

# ------------------------------------------------------------------------------
# Ejemplo de uso si quieres probar directo
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    pdf_path = "2024.10.07 Statesville.pdf"  # Ajusta tu ruta
    final_df = read_file(pdf_path)

    print("\n--- INFO DEL DATAFRAME FINAL ---")
    print(final_df.info())
    print("\n--- MUESTRA DE FILAS ---")
    print(final_df.head(20))

    # Si quieres exportar a CSV:
    final_df.to_csv("output_statesville.csv", index=False, encoding="utf-8-sig")
    print("\nGenerado el archivo 'output_statesville.csv'.")
