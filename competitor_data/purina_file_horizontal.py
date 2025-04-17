import datetime
import pandas as pd
import tabula
import re

# ————————————————————————————————————————————————————————————
# 1) Columnas finales, igual que tu esquema horizontal
# ————————————————————————————————————————————————————————————
H_COLUMNS = [
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

# ————————————————————————————————————————————————————————————
# 2) Función única para leer todas las tablas horizontalmente
# ————————————————————————————————————————————————————————————
def find_tables_in_pdf(file_path):
    """
    Lee TODAS las páginas con un área fija para layout Statesville.
    Devuelve lista de DataFrames, uno por cada tabla que detecte.
    """
    try:
        # Área extraída de Tabula GUI: [top, left, bottom, right]
        AREA = [178.7,  16.3, 599.4, 767.7]
        tables = tabula.read_pdf(
            file_path,
            pages="all",
            lattice=True,         # usa las líneas del PDF
            guess=False,          # no “adivina” columnas extras
            multiple_tables=True,
            area=AREA
        )
        return tables
    except Exception as e:
        print(f"[ERROR find_tables_in_pdf] {e}")
        return []

# ————————————————————————————————————————————————————————————
# 3) Ejemplo simple de validación y concatenación
# ————————————————————————————————————————————————————————————
def raw_price_list(table_list):
    price_list = pd.DataFrame()
    for tbl in table_list:
        # solo tablas con suficiente ancho (más de 10 columnas)
        if isinstance(tbl, pd.DataFrame) and tbl.shape[1] >= len(H_COLUMNS):
            price_list = pd.concat([price_list, tbl], ignore_index=True)
    return price_list

# ————————————————————————————————————————————————————————————
# 4) Renombra cabeceras a tu esquema
# ————————————————————————————————————————————————————————————
def set_column_names(df):
    df.columns = H_COLUMNS
    return df

# ————————————————————————————————————————————————————————————
# 5) Extrae “species” de las filas que no comienzan en dígito
# ————————————————————————————————————————————————————————————
def add_species_column(df):
    df["species"] = None
    current = None
    drop = []
    for i, v in df["product_number"].astype(str).items():
        if not v.strip().isdigit():
            current = v.strip().upper()
            drop.append(i)
        else:
            df.at[i, "species"] = current
    df = df.drop(drop).reset_index(drop=True)
    return df

# ————————————————————————————————————————————————————————————
# 6) Fecha y ubicación, como tu código vertical
# ————————————————————————————————————————————————————————————
def effective_date(file_path):
    area = [ 54,  10,  82, 254 ]
    df = tabula.read_pdf(file_path, pages=1, area=area)
    text = str(df[0]) if df else ""
    m = re.search(r"\d\d/\d\d/\d\d", text)
    if not m: return ""
    return datetime.datetime.strptime(m.group(), "%m/%d/%y").strftime("%Y-%m-%d")

def plant_location(file_path):
    area = [0, 500, 40, 700]
    df = tabula.read_pdf(file_path, pages=1, area=area)
    txt = str(df[0]).split("\n")[0] if df else ""
    return txt.replace(",", "").strip().upper()

def add_meta(df, file_path):
    df["date_inserted"]  = effective_date(file_path)
    df["plant_location"] = plant_location(file_path)
    df["source"]         = "pdf"
    return df

# ————————————————————————————————————————————————————————————
# 7) Corrige guiones en negativos
# ————————————————————————————————————————————————————————————
def correct_negative_value(v):
    s = str(v).strip()
    if s.endswith("-"):
        return -float(s[:-1].replace(",", ""))
    return float(s.replace(",", ""))

def correct_negative_value_in_price_list(df):
    for col in ["price_change","list_price","full_pallet_price",
                "half_load_full_pallet_price","full_load_full_pallet_price",
                "full_load_best_price"]:
        df[col] = df[col].apply(correct_negative_value)
    return df

# ————————————————————————————————————————————————————————————
# 8) Función principal que junta todo
# ————————————————————————————————————————————————————————————
def read_file(file_path):
    tables     = find_tables_in_pdf(file_path)
    price_list = raw_price_list(tables)
    price_list = set_column_names(price_list)
    price_list = add_species_column(price_list)
    price_list = add_meta(price_list, file_path)
    price_list = correct_negative_value_in_price_list(price_list)
    return price_list

# ————————————————————————————————————————————————————————————
# 9) Prueba local
# ————————————————————————————————————————————————————————————
if __name__ == "__main__":
    import sys
    pdf = sys.argv[1]
    df = read_file(pdf)
    print(df.head(20))
