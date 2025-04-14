import datetime
import pandas as pd
import tabula
import re
import os
from sharepoint_interface import get_sharepoint_interface


# --------------------------------------------------
# 1. Utility functions
# --------------------------------------------------

def correct_negative_value(value):
    """
    Handle trailing-hyphen negative values like '100-' if your PDFs have them.
    Otherwise, returns float or None for non-numeric strings.
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
    Apply `correct_negative_value` to columns containing numeric price data.
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
    Attempt to locate 'Effective Date - mm/dd/yyyy' on page 1.
    Returns 'YYYY-MM-DD' or None.
    """
    try:
        # Slight bounding box that includes the "Effective Date" line
        effective_date_table = tabula.read_pdf(
            file_path,
            pages=1,
            area=[50, 0, 200, 400],  # Adjust as needed
            guess=True,
            stream=True
        )
        if not effective_date_table:
            return None

        text = str(effective_date_table[0])
        # Search for mm/dd/yyyy or mm/dd/yy
        date_pat = re.compile(r"\d{1,2}/\d{1,2}/(\d{4}|\d{2})")
        match = date_pat.search(text)
        if not match:
            return None

        date_str = match.group(0)
        # Attempt parse with mm/dd/yyyy or mm/dd/yy
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
    Extract the "HUDSON'S" name from the top-left portion of page 1.
    If found, return "HUDSON'S". Otherwise return first line of text or None.
    """
    try:
        loc_table = tabula.read_pdf(
            file_path,
            pages=1,
            # A bounding box near the top-left corner that should include "HUDSON'S"
            # (y1=0, x1=0, y2=50, x2=250) -> tweak if it doesn't capture the text
            area=[0, 0, 50, 250],
            guess=True,
            stream=True
        )
        if not loc_table:
            return None

        text = str(loc_table[0])

        # If "HUDSON'S" appears in the captured text, just return that
        if "HUDSON'S" in text.upper():
            return "HUDSON'S"

        # Fallback: use the first line
        first_line = text.split("\n")[0].strip()
        return first_line.upper().replace(",", "")

    except:
        return None


def default_columns(df):
    """
    Reorder or keep only these final columns in your output.
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

# --------------------------------------------------
# 2. Core logic to parse the PDF
# --------------------------------------------------

def find_tables_in_pdf(file_path):
    """
    Attempt to extract tables from all pages of the PDF.
    Consider removing 'area' altogether if lines are missing,
    or try 'lattice=True' if the PDF has strong ruling lines.
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
    # 1) Extract raw tables
    table_list = find_tables_in_pdf(file_path)
    if not table_list:
        print("[WARN] No tables found at all!")
        return pd.DataFrame()

    # 2) Identify and rename only those tables with exactly 16 columns
    valid_tables = []
    for i, tbl in enumerate(table_list):
        print(f"[DEBUG] Table {i} shape: {tbl.shape}")

        # If the table has 16 columns, rename them to your new structure
        if tbl.shape[1] == 16:
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
            print(f"  -> Skipping Table {i} (not 16 columns).")

    if not valid_tables:
        print("[WARN] No valid 16-col tables found after filtering.")
        return pd.DataFrame()

    # 3) Merge valid 16-col DataFrames
    price_list = pd.concat(valid_tables, ignore_index=True)

    # ----------------------------------------------------
    # 4) Remove repeated header rows
    # ----------------------------------------------------
    remove_keywords = {"PRODUCT", "FORMULA", "NUMBER", "CODE", "UNIT",
                       "PALLET", "WEIGHT", "EMPTY DATAFRAME"}

    def looks_like_header(row):
        """
        Checks if the row contains any known repeated-header keywords.
        We'll look at *all* columns, convert to uppercase, and see if
        any word in remove_keywords is found in a cell that should hold data.
        """
        values = [str(x).strip().upper() for x in row]
        for cell_text in values:
            for kw in remove_keywords:
                if kw in cell_text:
                    return True
        return False

    # Identify "junk" rows
    mask = price_list.apply(looks_like_header, axis=1)
    # Keep rows NOT flagged as junk
    price_list = price_list[~mask]

    # Optionally, drop rows with no product_number or product_name
    price_list = price_list.dropna(subset=["product_number", "product_name"], how="all")

    # ----------------------------------------------------
    # 5) Enrich DataFrame with location & date
    # ----------------------------------------------------
    # Now that we've cleaned up the table data, we fetch the location/date from the PDF's top section:
    price_list["plant_location"] = plant_location(file_path)
    price_list["date_inserted"] = effective_date(file_path)

    # ----------------------------------------------------
    # 6) Fix negative sign in numeric columns
    # ----------------------------------------------------
    price_list = correct_negative_value_in_price_list(price_list)

    # ----------------------------------------------------
    # 7) Mark the data source
    # ----------------------------------------------------
    price_list["source"] = "pdf"

    # ----------------------------------------------------
    # 8) Final reordering / columns
    # ----------------------------------------------------
    price_list = default_columns(price_list)

    return price_list

