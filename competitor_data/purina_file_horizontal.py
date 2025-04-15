import datetime
import pandas as pd
import tabula
import re

def correct_negative_value(value):
    """
    Convierte valores con guión al final en negativos (por ejemplo, "100-") a -100.
    Si no se puede convertir a float, regresa el valor original.
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

def effective_date(file_path):
    """
    Extrae la fecha de efectividad (mm/dd/yyyy o mm/dd/yy) de la página 1 y la devuelve en formato YYYY-MM-DD.
    """
    try:
        df_date = tabula.read_pdf(
            file_path,
            pages=1,
            area=[50, 0, 200, 400],
            guess=True,
            stream=True
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
    Extrae la ubicación (p. ej. "HUDSON'S") de la parte superior izquierda de la página 1.
    """
    try:
        df_loc = tabula.read_pdf(
            file_path,
            pages=1,
            area=[0, 0, 50, 250],
            guess=True,
            stream=True
        )
        if not df_loc:
            return None

        text = str(df_loc[0]).upper()
        if "HUDSON'S" in text:
            return "HUDSON'S"
        # Retornamos la primera línea como fallback
        first_line = text.split("\n")[0].strip()
        return first_line.replace(",", "").strip()
    except:
        return None

def find_tables_in_pdf(file_path):
    """
    Extrae todas las tablas del PDF con Tabula (múltiples tablas).
    """
    try:
        tables = tabula.read_pdf(
            file_path,
            pages="all",
            stream=True,   # o lattice=True si el PDF tiene líneas bien definidas
            guess=True,
            multiple_tables=True
        )
        return tables
    except Exception as e:
        print(f"[ERROR find_tables_in_pdf]: {e}")
        return []

def read_file(file_path):
    """
    1) Lee y concatena todas las tablas del PDF.
    2) Corrige valores negativos en las columnas numéricas.
    3) Asigna los tipos de datos (string/double) según la definición EXACTA de la tabla en CDP.
    4) Agrega 'plant_location', 'date_inserted' y 'source'.
    5) Retorna el DataFrame final.
    """

    # Definimos las columnas con sus tipos, tal cual tu DESCRIBE TABLE en CDP:
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

    # Con esto identificamos las columnas numéricas (double)
    numeric_cols = [col for col, col_type in schema.items() if col_type == "double"]
    # Y las columnas string
    string_cols = [col for col, col_type in schema.items() if col_type == "string"]

    # 1) Extraer todas las tablas
    table_list = find_tables_in_pdf(file_path)
    if not table_list:
        print("[WARN] No se encontraron tablas en el PDF.")
        return pd.DataFrame()

    # 2) Concatenar
    df = pd.concat(table_list, ignore_index=True)

    # 3) Corregir valores negativos en las columnas definidas como double
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(correct_negative_value)

    # 4) Forzar los tipos según el esquema
    #    - Si la columna existe, la convertimos al tipo requerido.
    for col, col_type in schema.items():
        if col in df.columns:
            if col_type == "double":
                # Convertir a numérico (float)
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                # Convertir a string
                df[col] = df[col].astype(str)
        else:
            # Si no existe la columna, la creamos vacía con el tipo correspondiente
            if col_type == "double":
                df[col] = pd.Series(dtype="float")
            else:
                df[col] = pd.Series(dtype="object")

    # 5) Añadir plant_location, date_inserted, source (forzando también su tipo a string si no existe)
    loc = plant_location(file_path)
    eff_date = effective_date(file_path)

    df["plant_location"] = str(loc) if loc else ""
    df["date_inserted"] = str(eff_date) if eff_date else ""
    df["source"] = "pdf"

    # Forzamos de nuevo a string (por si las creamos vacías)
    df["plant_location"] = df["plant_location"].astype(str)
    df["date_inserted"] = df["date_inserted"].astype(str)
    df["source"] = df["source"].astype(str)

    # 6) Retornamos el DataFrame final
    return df

# Ejemplo de uso (main)
if __name__ == "__main__":
    pdf_path = "2024.10.07 Statesville (1).pdf"  # Ajusta tu ruta
    final_df = read_file(pdf_path)

    print("\n--- Info del DataFrame final ---")
    print(final_df.info())
    print("\n--- Ejemplo de filas ---")
    print(final_df.head())

    # Exportar CSV/Parquet
    final_df.to_csv("output_statesville.csv", index=False, encoding="utf-8-sig")
    print("Archivo CSV generado: output_statesville.csv")
