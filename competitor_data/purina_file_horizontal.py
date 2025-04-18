from __future__ import annotations

"""
purina_pdf_reader.py
--------------------
Lectura y limpieza de PDFs “horizontales” de Purina (Statesville layout).

Funciones principales
~~~~~~~~~~~~~~~~~~~~~
- read_file(pdf_path) -> pd.DataFrame
    Lee todas las tablas (≥ 16 columnas) en el PDF, conserva las filas de datos
    sin eliminar la primera de cada página, corrige números negativos y añade
    metadatos de planta/fecha.

Requisitos
~~~~~~~~~~
- tabula‑py   (y Java ≥ 8)
- pandas

Uso rápido
~~~~~~~~~~
>>> import purina_pdf_reader as pur
>>> df = pur.read_file("2024.10.07 Statesville (1).pdf")
>>> df.head()
"""

import datetime as _dt
import pathlib
import re
from typing import List

import pandas as pd
import tabula

# -----------------------------------------------------------------------------
# 1.  Configuración global
# -----------------------------------------------------------------------------
COLUMN_NAMES: List[str] = [
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
]

NUMERIC_COLS = COLUMN_NAMES[10:]

# -----------------------------------------------------------------------------
# 2.  Limpieza de valores numéricos
# -----------------------------------------------------------------------------

def _to_float(val: str | float | int | None):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    text = str(val).strip()
    if not text:
        return None
    if text.endswith("-"):
        text = text[:-1]
        sign = -1
    else:
        sign = 1
    try:
        return float(text) * sign
    except ValueError:
        return None


def _fix_numeric_cols(df: pd.DataFrame) -> pd.DataFrame:
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_to_float)
    return df

# -----------------------------------------------------------------------------
# 3.  Metadatos: fecha efectiva y ubicación de planta
# -----------------------------------------------------------------------------
DATE_RX = re.compile(r"\d{1,2}/\d{1,2}/(\d{4}|\d{2})")
LOC_RX = re.compile(r"([A-Z]+\s*'?[A-Z]*S?)")


def effective_date(pdf: str | pathlib.Path) -> str | None:
    try:
        tbls = tabula.read_pdf(pdf, pages=1, area=[50, 0, 200, 400], lattice=True, guess=False)
        if not tbls:
            return None
        text = str(tbls[0])
        m = DATE_RX.search(text)
        if not m:
            return None
        d = m.group(0)
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return _dt.datetime.strptime(d, fmt).date().isoformat()
            except ValueError:
                pass
    except Exception:
        pass
    return None


def plant_location(pdf: str | pathlib.Path) -> str | None:
    try:
        tbls = tabula.read_pdf(pdf, pages=1, area=[0, 0, 50, 250], lattice=True, guess=False)
        if not tbls:
            return None
        text = str(tbls[0]).upper()
        if "HUDSON'S" in text:
            return "HUDSON'S"
        m = LOC_RX.search(text)
        return m.group(1) if m else None
    except Exception:
        return None

# -----------------------------------------------------------------------------
# 4.  Extracción de tablas
# -----------------------------------------------------------------------------
HEADER_TOKENS = {
    "PRODUCT", "FORM", "UNIT", "WEIGHT", "PALLET", "MIN", "ORDER", "QUANTITY",
    "DAYS", "LEAD", "TIME", "STOCKING", "STATUS", "FOB", "DLV", "PRICE",
}


def _is_header_row(row: pd.Series) -> bool:
    """Detecta la cabecera completa y los fragmentos "MIN/DAYS/QUANTITY/TIME"."""
    # Fila‑cabecera clásica (dos primeras celdas)
    if (
        str(row.iloc[0]).strip().upper().startswith("PRODUCT") and
        str(row.iloc[1]).strip().upper().startswith("FORMULA")
    ):
        return True

    # Fragmentos repartidos en filas extra (no tienen precios)
    if row["list_price"] is None or pd.isna(row["list_price"]):
        joined = " ".join(str(x).upper() for x in row if x is not None)
        if any(tok in joined for tok in HEADER_TOKENS):
            return True
    return False


def _read_tables(pdf: str | pathlib.Path):
    try:
        return tabula.read_pdf(
            pdf,
            pages="all",
            lattice=True,
            guess=False,
            pandas_options={"dtype": str},
        )
    except Exception as exc:
        print(f"[ERROR _read_tables] {exc}")
        return []

# -----------------------------------------------------------------------------
# 5.  Lectura principal
# -----------------------------------------------------------------------------

def _standardize_table(tbl: pd.DataFrame) -> pd.DataFrame | None:
    """Recorta a 16 columnas y asigna nombres estándar; ignora tablas más pequeñas."""
    if tbl.shape[1] < 16:
        return None
    tbl = tbl.iloc[:, :16].copy()
    tbl.columns = COLUMN_NAMES
    return tbl


def default_columns(df: pd.DataFrame) -> pd.DataFrame:
    extra = ["plant_location", "date_inserted", "source"]
    cols = [*COLUMN_NAMES, *extra]
    return df[[c for c in cols if c in df.columns]]


def read_file(pdf: str | pathlib.Path) -> pd.DataFrame:
    tables = _read_tables(pdf)
    std_tables = filter(None, (_standardize_table(t) for t in tables))
    data = pd.concat(std_tables, ignore_index=True) if tables else pd.DataFrame()

    if data.empty:
        print("[WARN] No se encontraron tablas válidas.")
        return data

    # --- eliminar cabeceras y fragmentos ---
    data = data[~data.apply(_is_header_row, axis=1)].reset_index(drop=True)

    # --- descartar filas totalmente vacías ---
    data.dropna(how="all", inplace=True)

    # --- enriquecer con metadatos ---
    data["plant_location"] = plant_location(pdf)
    data["date_inserted"] = effective_date(pdf)
    data["source"] = "pdf"

    # --- corregir valores numéricos ---
    data = _fix_numeric_cols(data)

    return default_columns(data)

# -----------------------------------------------------------------------------
# 6.  CLI rápido para probar
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python purina_pdf_reader.py <archivo.pdf>")
        sys.exit(1)

    pdf_path = pathlib.Path(sys.argv[1])
    df = read_file(pdf_path)
    print(df.info())
    out = pdf_path.with_suffix(".parquet")
    df.to_parquet(out, index=False)
    print(f"Guardado → {out}")
