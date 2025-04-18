from __future__ import annotations

"""
purina_pdf_reader.py
--------------------
Lectura y limpieza de PDFs “horizontales” de Purina (Statesville layout).

Funciones principales
~~~~~~~~~~~~~~~~~~~~~
- read_file(pdf_path) -> pd.DataFrame
    Lee todas las tablas (16 columnas) en el PDF, conserva las filas de datos
    sin eliminar la primera de cada página, corrige números negativos y añade
    metadatos de planta/fecha.

- effective_date(pdf_path) -> str | None
    Extrae la fecha efectiva de la primera página (formato YYYY-MM-DD).

- plant_location(pdf_path) -> str | None
    Extrae la ubicación de la planta desde la cabecera.

Requisitos
~~~~~~~~~~
- tabula‑py
- Java Runtime >= 8 (para Tabula)
- pandas

Ejemplo rápido
~~~~~~~~~~~~~~
>>> import purina_pdf_reader as pur
>>> df = pur.read_file("2024.10.07 Statesville (1).pdf")
>>> df.head()
"""

import datetime as _dt
import pathlib
import re
from typing import Dict, List

import pandas as pd
import tabula

# -----------------------------------------------------------------------------
# 1.  Configuraciones generales
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

NUMERIC_COLS: List[str] = [
    "price_change",
    "list_price",
    "full_pallet_price",
    "half_load_full_pallet_price",
    "full_load_full_pallet_price",
    "full_load_best_price",
]

# -----------------------------------------------------------------------------
# 2.  Utilidades de limpieza
# -----------------------------------------------------------------------------

def correct_negative_value(value):
    """Convierte valores con guión final en números negativos."""
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("-"):
        text = text[:-1]
        try:
            return float(text) * -1
        except ValueError:
            return None
    try:
        return float(text)
    except ValueError:
        return None


def correct_negative_value_in_price_list(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica *correct_negative_value* a todas las columnas numéricas."""
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(correct_negative_value)
    return df


# -----------------------------------------------------------------------------
# 3.  Extracción de metadatos
# -----------------------------------------------------------------------------
DATE_PATTERN = re.compile(r"\d{1,2}/\d{1,2}/(\d{4}|\d{2})")


def effective_date(file_path: str | pathlib.Path) -> str | None:
    """Extrae la fecha efectiva (YYYY‑MM‑DD) de la primera página."""
    try:
        tables = tabula.read_pdf(
            file_path,
            pages=1,
            area=[50, 0, 200, 400],  # ajustar si es necesario
            lattice=True,
            guess=False,
        )
        if not tables:
            return None
        text = str(tables[0])
        match = DATE_PATTERN.search(text)
        if not match:
            return None
        date_str = match.group(0)
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                dt = _dt.datetime.strptime(date_str, fmt).date()
                return dt.isoformat()
            except ValueError:
                continue
    except Exception:
        pass
    return None


LOCATION_PATTERN = re.compile(r"([A-Z]+\s*'?[A-Z]*S?)")


def plant_location(file_path: str | pathlib.Path) -> str | None:
    """Extrae la ubicación de la planta desde la cabecera de la página 1."""
    try:
        tables = tabula.read_pdf(
            file_path,
            pages=1,
            area=[0, 0, 50, 250],
            lattice=True,
            guess=False,
        )
        if not tables:
            return None
        text = str(tables[0]).upper()
        # Ejemplo específico "HUDSON'S"
        if "HUDSON'S" in text:
            return "HUDSON'S"
        # Fallback: primera coincidencia en MAYÚSCULAS
        match = LOCATION_PATTERN.search(text)
        return match.group(1) if match else None
    except Exception:
        return None


# -----------------------------------------------------------------------------
# 4.  Lectura de tablas
# -----------------------------------------------------------------------------

def _is_repeated_header(row: pd.Series) -> bool:
    """Detecta la fila‑cabecera repetida por Tabula en cada página."""
    return (
        str(row[0]).strip().upper().startswith("PRODUCT")
        and str(row[1]).strip().upper().startswith("FORMULA")
    )


def find_tables_in_pdf(file_path: str | pathlib.Path):
    """Extrae todas las tablas de 16 columnas del PDF."""
    try:
        return tabula.read_pdf(
            file_path,
            pages="all",
            lattice=True,        # mejor para PDFs con cuadrículas
            guess=False,
            pandas_options={"dtype": str},
        )
    except Exception as exc:
        print(f"[ERROR find_tables_in_pdf] {exc}")
        return []


# -----------------------------------------------------------------------------
# 5.  Función principal
# -----------------------------------------------------------------------------

def default_columns(df: pd.DataFrame) -> pd.DataFrame:
    desired = COLUMN_NAMES + ["plant_location", "date_inserted", "source"]
    return df[[c for c in desired if c in df.columns]]


def read_file(file_path: str | pathlib.Path) -> pd.DataFrame:
    """Devuelve la lista de precios limpia y enriquecida."""
    table_list = find_tables_in_pdf(file_path)
    if not table_list:
        print("[WARN] No se encontraron tablas en el PDF.")
        return pd.DataFrame()

    valid: List[pd.DataFrame] = []
    for tbl in table_list:
        if tbl.shape[1] == 16:
            tbl.columns = COLUMN_NAMES
            valid.append(tbl)

    if not valid:
        print("[WARN] No se hallaron tablas de 16 columnas.")
        return pd.DataFrame()

    price_list = pd.concat(valid, ignore_index=True)

    # --- eliminar únicamente la cabecera repetida ---
    price_list = price_list[~price_list.apply(_is_repeated_header, axis=1)]

    # --- eliminar filas totalmente vacías ---
    price_list.dropna(how="all", inplace=True)

    # --- enriquecer con metadatos ---
    price_list["plant_location"] = plant_location(file_path)
    price_list["date_inserted"] = effective_date(file_path)
    price_list["source"] = "pdf"

    # --- corregir números negativos ---
    price_list = correct_negative_value_in_price_list(price_list)

    # --- reorden final ---
    return default_columns(price_list)


# -----------------------------------------------------------------------------
# 6.  CLI mínima para pruebas rápidas
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
    print(f"Archivo guardado en {out}")
