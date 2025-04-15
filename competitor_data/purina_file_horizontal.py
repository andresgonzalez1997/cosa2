import datetime
import pandas as pd
import tabula
import re

def correct_negative_value(value):
    """
    Convierte valores con guión al final en negativos (p. ej. "100-" se vuelve -100),
    si no es convertible a float, regresa None.
    """
    text = str(value).strip()
    if text.endswith("-"):
        text = text.replace("-", "")
        try:
            return float(text) * -1
        except ValueError:
            return None
    else:
        try:
            return float(text)
        except ValueError:
            return None

def correct_negative_value_in_price_list(df):
    """
    Aplica correct_negative_value a las columnas numéricas relevantes.
    """
    numeric_cols = [
        "price_change",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(correct_negative_value)
    return df

def effective_date(file_path):
    """
    Intenta extraer la fecha (mm/dd/yyyy o mm/dd/yy) de la primera página.
    Devuelve 'YYYY-MM-DD' o None si no se encuentra.
    """
    try:
        effective_date_table = tabula.read_pdf(
            file_path,
            pages=1,
            area=[50, 0, 200, 400],  # Ajustar bounding box según PDF
            guess=True,
            stream=True
        )
        if not effective_date_table:
            return None

        text = str(effective_date_table[0])
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
    Extrae alguna ubicación (ej. "HUDSON'S") desde la parte superior de la primera página.
    Retorna el texto en mayúsculas o None si no se identifica.
    """
    try:
        loc_table = tabula.read_pdf(
            file_path,
            pages=1,
            area=[0, 0, 50, 250],  # Ajustar bounding box según PDF
            guess=True,
            stream=True
        )
        if not loc_table:
            return None

        text = str(loc_table[0]).upper()
        if "HUDSON'S" in text:
            return "HUDSON'S"
        else:
            # Fallback: primera línea
            first_line = text.split("\n")[0].strip()
            return first_line.replace(",", "").strip()
    except:
        return None

def default_columns(df):
    """
    Ajusta el orden/columnas finales del DataFrame antes de exportar.
    """
    desired_cols = [
        "product_number",
        "formula_code",
        "product_name",
        "product_form",
        "unit_weight",
        "pallet_quantity",
        "stocking_status",
        "min_order_quantity",
        "days_lead_time",
        "fob_or_dlv",
        "price_change",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price",
        "plant_location",
        "date_inserted",
        "source"
    ]
    existing = [c for c in desired_cols if c in df.columns]
    return df[existing]

def find_tables_in_pdf(file_path):
    """
    Usa tabula.read_pdf para extraer todas las tablas (pag=all).
    Ajustar stream/lattice según el PDF.
    """
    try:
        table_list = tabula.read_pdf(
            file_path,
            pages="all",
            stream=True,
            guess=True
        )
        return table_list
    except Exception as e:
        print(f"[ERROR in find_tables_in_pdf]: {e}")
        return []

def read_file(file_path):
    # 1) Extraer tablas
    table_list = find_tables_in_pdf(file_path)
    if not table_list:
        print("[WARN] No se encontraron tablas en el PDF.")
        return pd.DataFrame()

    # 2) Filtrar DataFrames que tengan 16 columnas
    valid_tables = []
    for i, tbl in enumerate(table_list):
        print(f"[DEBUG] Table {i} shape: {tbl.shape}")
        if tbl.shape[1] == 16:
            # Renombrar columnas a un estándar
            tbl.columns = [
                "product_number",
                "formula_code",
                "product_name",
                "product_form",
                "unit_weight",
                "pallet_quantity",
                "stocking_status",
                "min_order_quantity",
                "days_lead_time",
                "fob_or_dlv",
                "price_change",
                "list_price",
                "full_pallet_price",
                "half_load_full_pallet_price",
                "full_load_full_pallet_price",
                "full_load_best_price"
            ]
            valid_tables.append(tbl)
        else:
            print(f"[DEBUG] Se omite la tabla {i} por no tener 16 columnas.")

    if not valid_tables:
        print("[WARN] No hay tablas con 16 columnas tras el filtrado.")
        return pd.DataFrame()

    # 3) Concatenar
    price_list = pd.concat(valid_tables, ignore_index=True)

    # 4) Limpiar cabeceras repetidas
    remove_keywords = {"PRODUCT", "FORMULA", "NUMBER", "CODE", "UNIT", "PALLET", "WEIGHT"}
    def looks_like_header(row):
        values = [str(x).strip().upper() for x in row]
        for cell_text in values:
            for kw in remove_keywords:
                if kw in cell_text:
                    return True
        return False
    mask = price_list.apply(looks_like_header, axis=1)
    price_list = price_list[~mask]

    # 5) Quitar filas completamente vacías
    price_list.dropna(how="all", inplace=True)

    # 6) Enriquecer con location/date
    price_list["plant_location"] = plant_location(file_path)
    price_list["date_inserted"] = effective_date(file_path)

    # 7) Arreglar valores negativos
    price_list = correct_negative_value_in_price_list(price_list)

    # 8) Marcar el origen
    price_list["source"] = "pdf"

    # 9) Orden final
    price_list = default_columns(price_list)
    return price_list

#--------------------------------------------------------




import datetime
import pandas as pd
import tabula
import re

def correct_negative_value(value):
    """
    Convierte valores con guión al final en negativos (por ejemplo, "100-" se vuelve -100).
    Si el valor no es convertible a float, retorna None.
    """
    text = str(value).strip()
    if text.endswith("-"):
        text = text.replace("-", "")
        try:
            return float(text) * -1
        except ValueError:
            return None
    else:
        try:
            return float(text)
        except ValueError:
            return None

def correct_negative_value_in_price_list(df):
    """
    Aplica correct_negative_value a columnas numéricas de precios.
    """
    numeric_cols = [
        "price_change",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(correct_negative_value)
    return df

def effective_date(file_path):
    """
    Extrae la fecha de efectividad (mm/dd/yyyy) de la página 1 y la devuelve en formato YYYY-MM-DD.
    """
    try:
        effective_date_table = tabula.read_pdf(
            file_path,
            pages=1,
            area=[50, 0, 200, 400],
            guess=True,
            stream=True
        )
        if not effective_date_table:
            return None

        text = str(effective_date_table[0])
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
    Extrae la ubicación (por ejemplo, "HUDSON'S") de la parte superior izquierda de la página 1.
    """
    try:
        loc_table = tabula.read_pdf(
            file_path,
            pages=1,
            area=[0, 0, 50, 250],
            guess=True,
            stream=True
        )
        if not loc_table:
            return None

        text = str(loc_table[0]).upper()
        if "HUDSON'S" in text:
            return "HUDSON'S"
        else:
            first_line = text.split("\n")[0].strip()
            return first_line.replace(",", "").strip()
    except:
        return None

def default_columns(df):
    """
    Selecciona y ordena las columnas finales deseadas.
    """
    desired_cols = [
        "product_number",
        "formula_code",
        "product_name",
        "product_form",
        "unit_weight",
        "pallet_quantity",
        "stocking_status",
        "min_order_quantity",
        "days_lead_time",
        "fob_or_dlv",
        "price_change",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price",
        "plant_location",
        "date_inserted",
        "source"
    ]
    existing = [c for c in desired_cols if c in df.columns]
    return df[existing]

def find_tables_in_pdf(file_path):
    """
    Extrae todas las tablas del PDF usando tabula.
    """
    try:
        table_list = tabula.read_pdf(
            file_path,
            pages="all",
            stream=True,
            guess=True
        )
        return table_list
    except Exception as e:
        print(f"[ERROR in find_tables_in_pdf]: {e}")
        return []

def read_file(file_path):
    # Definición del encabezado esperado (16 columnas)
    expected_columns = [
        "product_number",
        "formula_code",
        "product_name",
        "product_form",
        "unit_weight",
        "pallet_quantity",
        "stocking_status",
        "min_order_quantity",
        "days_lead_time",
        "fob_or_dlv",
        "price_change",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price"
    ]
    
    # 1) Extraer tablas crudas del PDF
    table_list = find_tables_in_pdf(file_path)
    if not table_list:
        print("[WARN] No se encontraron tablas en el PDF.")
        return pd.DataFrame()

    valid_tables = []
    for i, tbl in enumerate(table_list):
        print(f"[DEBUG] Table {i} shape: {tbl.shape}")
        # Si el número de columnas es 15 (faltando la última), agregar una columna vacía al final
        if tbl.shape[1] == 15:
            tbl[15] = ""
        # Si tiene 16 columnas, asignar los nombres esperados
        if tbl.shape[1] == 16:
            tbl.columns = expected_columns
            valid_tables.append(tbl)
        else:
            print(f"[DEBUG] Se omite la tabla {i}: no tiene 16 columnas tras intentar ajustar.")
    
    if not valid_tables:
        print("[WARN] No hay tablas válidas de 16 columnas tras filtrar.")
        return pd.DataFrame()

    # 2) Concatenar las tablas válidas
    price_list = pd.concat(valid_tables, ignore_index=True)

    # 3) Eliminar filas que sean encabezados repetidos
    # Definimos el encabezado esperado en minúsculas para la comparación exacta
    header_row = [col.lower().strip() for col in expected_columns]
    def is_exact_header(row):
        row_vals = [str(x).lower().strip() for x in row]
        return row_vals == header_row

    mask = price_list.apply(is_exact_header, axis=1)
    removed_count = mask.sum()
    if removed_count > 0:
        print(f"[DEBUG] Se eliminaron {removed_count} filas de encabezado repetido.")
    price_list = price_list[~mask]

    # 4) Eliminar filas completamente vacías en columnas críticas
    price_list = price_list.dropna(subset=["product_number", "product_name"], how="all")

    # 5) Agregar la ubicación y la fecha extraídas del PDF
    price_list["plant_location"] = plant_location(file_path)
    price_list["date_inserted"] = effective_date(file_path)

    # 6) Corregir valores negativos en las columnas de precio
    price_list = correct_negative_value_in_price_list(price_list)

    # 7) Agregar la columna 'source'
    price_list["source"] = "pdf"

    # 8) Reordenar las columnas finales
    price_list = default_columns(price_list)

    return price_list

#--------------------------------------------------
#--------------------------------------------------
#--------------------------------------------------







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
            return value  # si no se puede convertir, devolvemos el valor original
    else:
        try:
            return float(text)
        except ValueError:
            return value

def correct_negative_value_in_price_list(df, numeric_cols):
    """
    Aplica correct_negative_value exclusivamente a las columnas numéricas definidas en numeric_cols.
    """
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(correct_negative_value)
    return df

def effective_date(file_path):
    """
    Extrae la fecha de efectividad (mm/dd/yyyy o mm/dd/yy) de la página 1 y la devuelve en formato YYYY-MM-DD.
    """
    try:
        effective_date_table = tabula.read_pdf(
            file_path,
            pages=1,
            area=[50, 0, 200, 400],
            guess=True,
            stream=True
        )
        if not effective_date_table:
            return None

        text = str(effective_date_table[0])
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
    Extrae la ubicación (por ejemplo, "HUDSON'S") de la parte superior izquierda de la página 1.
    """
    try:
        loc_table = tabula.read_pdf(
            file_path,
            pages=1,
            area=[0, 0, 50, 250],
            guess=True,
            stream=True
        )
        if not loc_table:
            return None

        text = str(loc_table[0]).upper()
        if "HUDSON'S" in text:
            return "HUDSON'S"
        # Tomamos la primera línea como fallback
        first_line = text.split("\n")[0].strip()
        return first_line.replace(",", "").strip()
    except:
        return None

def find_tables_in_pdf(file_path):
    """
    Extrae todas las tablas del PDF usando tabula (múltiples tablas, modo stream).
    """
    try:
        table_list = tabula.read_pdf(
            file_path,
            pages="all",
            stream=True,
            guess=True,
            multiple_tables=True
        )
        return table_list
    except Exception as e:
        print(f"[ERROR in find_tables_in_pdf]: {e}")
        return []

def read_file(file_path):
    # Definimos las columnas que deben ser numéricas (por ejemplo, precios)
    numeric_cols = [
        "price_change",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price",
    ]

    # 1) Extraer tablas crudas del PDF
    table_list = find_tables_in_pdf(file_path)
    if not table_list:
        print("[WARN] No se encontraron tablas en el PDF.")
        return pd.DataFrame()

    # 2) Procesar cada tabla:
    #    - Forzamos las columnas 'numeric_cols' a la corrección de negativos,
    #    - El resto de columnas se convierten a string.
    processed_tables = []
    for tbl in table_list:
        # Aplica corrección de negativos en las columnas definidas
        for col in tbl.columns:
            if col in numeric_cols:
                # Corrección de valor negativo
                tbl[col] = tbl[col].apply(correct_negative_value)
            else:
                # Forzamos a texto
                tbl[col] = tbl[col].astype(str)
        processed_tables.append(tbl)

    # 3) Concatenar todas las tablas procesadas
    df_final = pd.concat(processed_tables, ignore_index=True)

    # 4) Agregamos 'plant_location' y 'date_inserted'
    df_final["plant_location"] = plant_location(file_path)
    df_final["date_inserted"] = effective_date(file_path)

    # 5) Agregamos la columna 'source'
    df_final["source"] = "pdf"

    return df_final

# EJEMPLO DE USO
if __name__ == "__main__":
    pdf_path = "2024.10.07 Statesville (1).pdf"  # tu PDF de ejemplo
    final_df = read_file(pdf_path)

    print("\n--- Info del DataFrame final ---")
    print(final_df.info())

    print("\n--- Ejemplo de filas ---")
    print(final_df.head(10))

    # Puedes exportar a CSV, parquet, etc.
    final_df.to_csv("output_final.csv", index=False, encoding="utf-8-sig")
    print("\nSe generó el archivo: output_final.csv")
