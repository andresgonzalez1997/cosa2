
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

# ---------------------------------------------------------------------
