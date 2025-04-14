import datetime
import pandas as pd
import tabula
import re

def default_columns(df):
    return df[[
        "product_number",
        "formula_code",
        "product_desc",
        "product_form",
        "unit_weight",
        "pallet_quantity",
        "stocking_status",
        "min_order_quantity",
        "days_lead_time",
        "fob_or_dlv",
        "change_in_price",
        "list_price",
        "full_pallet_price",
        "half_load_full_pallet_price",
        "full_load_full_pallet_price",
        "full_load_best_price",
        "species",
        "plant_location",
        "date_inserted",
        "source"
    ]]

def source_columns(df):
    df["source"] = "pdf"
    return df

def find_unit_weight(df):
    for index, row in df.iterrows():
        if not re.search("LB", row["unit_weight"]):
            search_result = re.findall("\d*\s*LB", str(row["product_desc"]))
            if len(search_result) > 0:
                df.at[index, "unit_weight"] = search_result[0]
    return df

def correct_negative_value(value):
    if str(value).endswith("-"):
        return float(str(value).replace("-", "")) * -1
    else:
        return float(value)

def correct_negative_value_in_price_list(df):
    for col in df.columns[7:12]:
        df[col] = df[col].apply(correct_negative_value)
    return df

def effective_date(file_path):
    effective_date = None
    effective_date_table = tabula.read_pdf(file_path, pages=1, area=[54, 10, 82, 254])
    results = re.findall("[0-9][0-9]/[0-9][0-9]/[0-9][0-9]", str(effective_date_table[0]))
    if len(results) > 0:
        effective_date = datetime.datetime.strptime(results[0], "%m/%d/%y").date()
    return effective_date.strftime("%Y-%m-%d")

def plant_location(file_path):
    location_table = tabula.read_pdf(file_path, pages=1, area=[0, 500, 40, 700])
    location = str(location_table[0]).split("\n")[0].strip().replace(",", "").upper()
    return location

def add_effective_date(df, file_path):
    df["date_inserted"] = effective_date(file_path)
    return df

def add_plant_location(df, file_path):
    df["plant_location"] = plant_location(file_path)
    return df

def add_species_column(df):
    species = None
    df["species"] = None
    for index, row in df.iterrows():
        if re.match("\d", row[0]) is None:
            species = str(row[0]).replace(",", "").upper()
            df = df.drop(index, axis=0)
        else:
            df.loc[index, "species"] = species
    df = df.reset_index(drop=True)
    return df

def set_column_names(df):
    df.columns = [
        "product_number",
        "formula_code",
        "product_desc",  # Cambiado de "product_name" a "product_desc"
        "product_form",
        "unit_weight",
        "pallet_quantity",  # Añadido "pallet_quantity"
        "stocking_status",  # Añadido "stocking_status"
        "min_order_quantity",  # Añadido "min_order_quantity"
        "days_lead_time",  # Añadido "days_lead_time"
        "fob_or_dlv",
        "change_in_price",  # Cambiado de "price_change" a "change_in_price"
        "list_price",  # Cambiado de "single_unit_list_price" a "list_price"
        "full_pallet_price",  # Cambiado de "full_pallet_list_price" a "full_pallet_price"
        "half_load_full_pallet_price",  # Añadido "half_load_full_pallet_price"
        "full_load_full_pallet_price",  # Añadido "full_load_full_pallet_price"
        "full_load_best_price",  # Cambiado de "best_net_list_price" a "full_load_best_price"
        "species",
        "plant_location",
        "date_inserted",
        "source"
    ]
    return df

def valid_table(df):
    """
    #For now, to validate that the table is valid, an assumption is made that tables that have more than 5 columns are valid.
    #Using the second value in the .shape attribute in DataFrame (columns) to check # of columns.
    """
    if not isinstance(df, pd.DataFrame): return False
    if not df.shape[1] > 5: return False
    return True

def raw_price_list(table_list):
    """
    #Returns a single DataFrame with all valid price list tables merged.
    """
    price_list = pd.DataFrame()
    for tbl in table_list:
        if valid_table(tbl):
            price_list = pd.concat([price_list, tbl], ignore_index=True)
    return price_list

def find_tables_in_pdf(file_path):
    try:
        table_list = tabula.read_pdf(file_path, pages="all", area=[89, 10, 800, 650], lattice=True)
        return table_list
    except Exception as error:
        return False

def read_file(file_path):
    table_list = find_tables_in_pdf(file_path)
    price_list = raw_price_list(table_list)
    price_list = set_column_names(price_list)
    price_list = add_species_column(price_list)
    price_list = add_plant_location(price_list, file_path)
    price_list = add_effective_date(price_list, file_path)
    price_list = correct_negative_value_in_price_list(price_list)
    price_list = find_unit_weight(price_list)
    price_list = source_columns(price_list)
    price_list = default_columns(price_list)
    return price_list

def effective_date_horizontal(file_path):
    effective_date = None
    effective_date_table = tabula.read_pdf(file_path, pages=1, area=[60, 100, 80, 350])
    results = re.findall("[0-9][0-9]/[0-9][0-9]/[0-9][0-9]", str(effective_date_table[0]))
    if len(results) > 0:
        effective_date = datetime.datetime.strptime(results[0], "%m/%d/%y").date()
    return effective_date.strftime("%Y-%m-%d")

def plant_location_horizontal(file_path):
    location_table = tabula.read_pdf(file_path, pages=1, area=[10, 400, 40, 700])
    location = str(location_table[0]).split("\n")[0].strip().replace(",", "").upper()
    return location

def add_effective_date_horizontal(df, file_path):
    df["date_inserted"] = effective_date_horizontal(file_path)
    return df

def add_plant_location_horizontal(df, file_path):
    df["plant_location"] = plant_location_horizontal(file_path)
    return df

def read_file_horizontal(file_path):
    table_list = find_tables_in_pdf(file_path)
    price_list = raw_price_list(table_list)
    price_list = set_column_names(price_list)
    price_list = add_species_column(price_list)
    price_list = add_plant_location_horizontal(price_list, file_path)
    price_list = add_effective_date_horizontal(price_list, file_path)
    price_list = correct_negative_value_in_price_list(price_list)
    price_list = find_unit_weight(price_list)
    price_list = source_columns(price_list)
    price_list = default_columns(price_list)
    return price_list
