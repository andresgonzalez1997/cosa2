import datetime
import pandas as pd
import tabula
import re

def default_columns(df):
    return df[[
        "product_number",
        "formula_code",
        "product_name",
        "ref_col",
        "unit_weight",
        "product_form",
        "fob_or_dlv",
        "price_change",
        "single_unit_list_price",
        "full_pallet_list_price",
        "pkg_bulk_discount",
        "best_net_list_price",
        "species",
        "plant_location",
        "date_inserted",
        "source"
    ]]

def source_columns(df):
    df["source"] = "pdf"
    return df

def find_unit_weight(df):
    """
    Si en 'unit_weight' no aparece el texto 'LB',
    se busca en la 'product_name' y se mueve a 'unit_weight'.
    """
    for index, row in df.iterrows():
        if not re.search("LB", str(row["unit_weight"])):
            search_result = re.findall(r"\d*\s*LB", str(row["product_name"]))
            if len(search_result) > 0:
                df.at[index, "unit_weight"] = search_result[0]
    return df

def correct_negative_value(value):
    """
    Convierte valores con guión final en negativos:
      Ej: '100-' -> -100
    """
    if str(value).endswith("-"):
        return float(str(value).replace("-","")) * -1
    else:
        return float(value)

def correct_negative_value_in_price_list(df):
    """
    Aplica correct_negative_value a las columnas de precios,
    asumiendo que están en las columnas [7..12).
    """
    for col in df.columns[7:12]:
        df[col] = df[col].apply(correct_negative_value)
    return df

def effective_date(file_path):
    """
    Lee la fecha del PDF, buscando en el área [54,10,82,254] (pág.1).
    Devuelve la fecha en formato 'YYYY-MM-DD'.
    """
    effective_date_value = None
    effective_date_table = tabula.read_pdf(file_path, pages=1, area=[54,10,82,254])
    # Buscar con regex: dd/dd/dd
    results = re.findall(r"[0-9][0-9]/[0-9][0-9]/[0-9][0-9]", str(effective_date_table[0]))
    if len(results) > 0:
        effective_date_value = datetime.datetime.strptime(results[0], "%m/%d/%y").date()
        return effective_date_value.strftime("%Y-%m-%d")
    return None

def plant_location(file_path):
    """
    Lee la 'plant location' del PDF, buscando en el área [0,500,40,700] (pág.1).
    """
    location_table = tabula.read_pdf(file_path, pages=1, area=[0,500,40,700])
    location = str(location_table[0]).split("\n")[0].strip().replace(",", "").upper()
    return location

def add_effective_date(df, file_path):
    df["date_inserted"] = effective_date(file_path)
    return df

def add_plant_location(df, file_path):
    df["plant_location"] = plant_location(file_path)
    return df

def add_species_column(df):
    """
    Toma las filas que no inicien con un dígito (regex) como 'species',
    las elimina de la tabla y las usa como 'species' para las siguientes filas.
    """
    species = None
    df["species"] = None
    for index, row in df.iterrows():
        if re.match(r"\d", str(row[0])) is None:
            species = str(row[0]).replace(",", "").upper()
            df = df.drop(index, axis=0)
        else:
            df.loc[index, "species"] = species
    df = df.reset_index(drop=True)
    return df

def set_column_names(df):
    """
    Renombra columnas a los nombres deseados.
    """
    df.columns = [
        "product_number",
        "formula_code",
        "product_name",
        "ref_col",
        "unit_weight",
        "product_form",
        "fob_or_dlv",
        "price_change",
        "single_unit_list_price",
        "full_pallet_list_price",
        "pkg_bulk_discount",
        "best_net_list_price"
    ]
    return df

def valid_table(df):
    """
    Chequeo simple: si un DataFrame tiene más de 5 columnas, lo consideramos tabla válida.
    """
    if not isinstance(df, pd.DataFrame):
        return False
    if not df.shape[1] > 5:
        return False
    return True

def raw_price_list(table_list):
    """
    Concatena todos los DataFrames válidos (más de 5 columnas).
    """
    price_list = pd.DataFrame()
    for tbl in table_list:
        if valid_table(tbl):
            price_list = pd.concat([price_list, tbl], ignore_index=True)
    return price_list

def find_tables_in_pdf(file_path):
    """
    Lee todas las tablas del PDF (todas las páginas) usando:
    - area=[89,10,800,650],
    - lattice=True (aprovecha las líneas si el PDF las tiene claras).
    """
    try:
        table_list = tabula.read_pdf(file_path, pages="all", area=[89,10,800,650], lattice=True)
        return table_list
    except Exception as error:
        print(f"[ERROR find_tables_in_pdf]: {error}")
        return []

def read_file(file_path):
    # 1) Extrae listas de tablas
    table_list = find_tables_in_pdf(file_path)

    # 2) Concatena las tablas válidas
    price_list = raw_price_list(table_list)

    # 3) Renombra columnas a las definitivas
    price_list = set_column_names(price_list)

    # 4) Ajusta la columna species
    price_list = add_species_column(price_list)

    # 5) Agrega ubicación y fecha
    price_list = add_plant_location(price_list, file_path)
    price_list = add_effective_date(price_list, file_path)

    # 6) Corrige valores negativos en columnas de precios
    price_list = correct_negative_value_in_price_list(price_list)

    # 7) Ajusta 'unit_weight' si no contiene "LB"
    price_list = find_unit_weight(price_list)

    # 8) Indica que la fuente es 'pdf'
    price_list = source_columns(price_list)

    # 9) Finalmente, filtra (y ordena) las columnas definidas en default_columns
    price_list = default_columns(price_list)

    # Retorna el DataFrame final
    return price_list

# Si quisieras probar directo:
if __name__ == "__main__":
    df_final = read_file("ruta/a/tu_archivo.pdf")
    print(df_final.head(20))
    df_final.to_csv("output.csv", index=False, encoding="utf-8-sig")
