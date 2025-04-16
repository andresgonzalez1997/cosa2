"""
purina_file_horizontal.py
-------------------------
Lectura y limpieza de PDFs “horizontales” de Purina (layout Statesville).

Expone:
    • read_file(pdf_path) -> pd.DataFrame
      (listo para subir a HDFS / Impala)
"""

from __future__ import annotations
import datetime as _dt
import re
from pathlib import Path
from typing import Dict, List

import pandas as pd
import tabula

# ------------------------------------------------------------------------------
# 1. Funciones auxiliares (fecha y planta en la cabecera de la página 1)
# ------------------------------------------------------------------------------

_DATE_RE = re.compile(r"\d{1,2}/\d{1,2}/(\d{4}|\d{2})")

def effective_date(file_path: str | Path) -> str | None:
    """Devuelve la fecha en formato YYYY‑MM‑DD o None si no la encuentra."""
    try:
        df = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=False,
            stream=True,
            guess=True,
            area=[50, 0, 130, 600]  # zona pequeñita donde va la fecha
        )
        if not df:
            return None
        m = _DATE_RE.search(str(df[0]))
        if not m:
            return None
        raw = m.group(0)
        # normaliza dd/mm/yy ó mm/dd/yy
        dt = _dt.datetime.strptime(raw, "%m/%d/%Y" if raw.split("/")[2] != raw[-2:] else "%m/%d/%y")
        return dt.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[WARN effective_date] {e}")
        return None


def plant_location(file_path: str | Path) -> str | None:
    """Devuelve la planta (ej. 'Statesville') o None."""
    try:
        df = tabula.read_pdf(
            file_path,
            pages=1,
            lattice=False,
            stream=True,
            guess=True,
            area=[130, 0, 180, 600]
        )
        if not df:
            return None
        txt = str(df[0])
        # asume que la palabra antes de “Price List” es la planta
        m = re.search(r"([A-Za-z ]+)\s+Price\s+List", txt)
        return m.group(1).strip() if m else None
    except Exception as e:
        print(f"[WARN plant_location] {e}")
        return None


# ------------------------------------------------------------------------------
# 2. Extracción de la tabla principal
# ------------------------------------------------------------------------------

# Coordenadas sacadas del JSON que enviaste desde Tabula GUI
AREA_PAGE1 = [178.695, 16.335, 599.445, 767.745]
AREA_PAGE_OTHERS = [81.675, 16.335, 599.445, 767.745]

def _read_page(file_path: str | Path, page: str | int, area: List[float]):
    return tabula.read_pdf(
        file_path,
        pages=page,
        lattice=True,      # usa líneas del PDF: más preciso
        guess=False,
        area=area
    )

def extract_main_tables(file_path: str | Path) -> List[pd.DataFrame]:
    """Lee página 1 y las demás por separado (coordenadas distintas)."""
    try:
        tables: List[pd.DataFrame] = []
        tables += _read_page(file_path, 1, AREA_PAGE1)
        tables += _read_page(file_path, "2-", AREA_PAGE_OTHERS)
        print(f"[INFO extract_main_tables] páginas leídas: {len(tables)}")
        return tables
    except Exception as e:
        print(f"[ERROR extract_main_tables] {e}")
        return []


# ------------------------------------------------------------------------------
# 3. Corrección de números negativos con guion
# ------------------------------------------------------------------------------

def _to_float_fix_neg(val):
    txt = str(val).strip()
    if txt.endswith("-"):
        txt = txt[:-1]
        try:
            return -float(txt.replace(",", ""))
        except ValueError:
            return None
    try:
        return float(txt.replace(",", ""))
    except ValueError:
        return None


# ------------------------------------------------------------------------------
# 4. Función principal
# ------------------------------------------------------------------------------

SCHEMA: Dict[str, str] = {
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
NUMERIC_COLS = [c for c, t in SCHEMA.items() if t == "double"]

def read_file(file_path: str | Path) -> pd.DataFrame:
    """Devuelve DataFrame listo según SCHEMA."""
    file_path = str(file_path)
    tables = extract_main_tables(file_path)
    if not tables:
        print("[WARN read_file] no se extrajo ninguna tabla")
        return pd.DataFrame({k: pd.Series(dtype=("float" if v == "double" else "object"))
                             for k, v in SCHEMA.items()})

    df = pd.concat(tables, ignore_index=True)
    print(f"[INFO read_file] filas totales crudo: {df.shape[0]}")

    # Limpieza básica de headers repetidos / filas vacías
    df = df[~df["PRODUCT NUMBER"].str.contains("PRODUCT", na=False)]
    df = df.dropna(how="all")
    print(f"[INFO read_file] filas tras limpiar headers: {df.shape[0]}")

    # Normaliza nombres a minúsculas y underscores
    df.columns = (
        df.columns
          .str.lower()
          .str.strip()
          .str.replace(r"[ /]+", "_", regex=True)
    )

    # Aplica conversión de negativos y cast a float
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_to_float_fix_neg)

    # Asegura que existan todas las columnas del SCHEMA
    for col, typ in SCHEMA.items():
        if col not in df.columns:
            df[col] = pd.Series(dtype="float" if typ == "double" else "object")

    # Metadatos
    df["plant_location"] = plant_location(file_path) or ""
    df["date_inserted"] = effective_date(file_path) or ""
    df["source"] = "pdf"

    # Orden final y tipos
    df_final = df[list(SCHEMA.keys())].copy()
    for col, typ in SCHEMA.items():
        if typ == "double":
            df_final[col] = pd.to_numeric(df_final[col], errors="coerce")
        else:
            df_final[col] = df_final[col].astype(str)

    print(f"[INFO read_file] DataFrame final → filas: {df_final.shape[0]}, columnas: {df_final.shape[1]}")
    return df_final


# ------------------------------------------------------------------------------
# 5. CLI simple para pruebas locales
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Uso: python purina_file_horizontal.py <ruta_pdf>")
        sys.exit(1)
    pdf = Path(sys.argv[1])
    if not pdf.exists():
        print(f"Archivo no encontrado: {pdf}")
        sys.exit(1)

    df_out = read_file(pdf)
    print(df_out.head())


------------------------


KeyError: 'PRODUCT NUMBER'
KeyError                                  Traceback (most recent call last)
~/.local/lib/python3.7/site-packages/pandas/core/indexes/base.py in get_loc(self, key, method, tolerance)
   3360             try:
-> 3361                 return self._engine.get_loc(casted_key)
   3362             except KeyError as err:

~/.local/lib/python3.7/site-packages/pandas/_libs/index.pyx in pandas._libs.index.IndexEngine.get_loc()

~/.local/lib/python3.7/site-packages/pandas/_libs/index.pyx in pandas._libs.index.IndexEngine.get_loc()

pandas/_libs/hashtable_class_helper.pxi in pandas._libs.hashtable.PyObjectHashTable.get_item()

pandas/_libs/hashtable_class_helper.pxi in pandas._libs.hashtable.PyObjectHashTable.get_item()

KeyError: 'PRODUCT NUMBER'

The above exception was the direct cause of the following exception:

KeyError                                  Traceback (most recent call last)
/tmp/ipykernel_110/1215835711.py in <module>
      1 if __name__ == "__main__":
----> 2     excecute_process()

/tmp/ipykernel_110/4117894532.py in excecute_process()
     35 
     36         # Parsear (horizontal)
---> 37         df = pfh.read_file(str(local_pdf_path))
     38         print("Aqui vienen los types:-----------------")
     39         print("\n--- TIPOS DEL DATAFRAME ---")

~/competitor_data/purina_file_horizontal.py in read_file(file_path)
    159 
    160     # Limpieza básica de headers repetidos / filas vacías
--> 161     df = df[~df["PRODUCT NUMBER"].str.contains("PRODUCT", na=False)]
    162     df = df.dropna(how="all")
    163     print(f"[INFO read_file] filas tras limpiar headers: {df.shape[0]}")

~/.local/lib/python3.7/site-packages/pandas/core/frame.py in __getitem__(self, key)
   3456             if self.columns.nlevels > 1:
   3457                 return self._getitem_multilevel(key)
-> 3458             indexer = self.columns.get_loc(key)
   3459             if is_integer(indexer):
   3460                 indexer = [indexer]

~/.local/lib/python3.7/site-packages/pandas/core/indexes/base.py in get_loc(self, key, method, tolerance)
   3361                 return self._engine.get_loc(casted_key)
   3362             except KeyError as err:
-> 3363                 raise KeyError(key) from err
   3364 
   3365         if is_scalar(key) and isna(key) and not self.hasnans:

KeyError: 'PRODUCT NUMBER'
