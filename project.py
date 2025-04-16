"""
purina_file_horizontal.py
-------------------------
Lectura y limpieza de PDFs “horizontales” de Purina (Statesville layout).

Expone:
    • extract_main_tables(pdf_path) -> pd.DataFrame
    • fix_negative_numbers(df, columns=None) -> pd.DataFrame
    • extract_effective_date(pdf_path) -> datetime.date
    • extract_plant_location(pdf_path) -> str
"""

from __future__ import annotations

import datetime as _dt
import pathlib
import re
from typing import Dict, List, Sequence

import pandas as pd
import tabula

# -----------------------------------------------------------------------------
# 1.  Configuraciones generales
# -----------------------------------------------------------------------------
COLUMN_MAP: Dict[str, str] = {
    "PRODUCT NUMBER": "product_number",
    "FORMULA CODE": "formula_code",
    "PRODUCT DESC.": "product_name",
    "PRODUCT FORM": "product_form",
    "UNIT  WEIGHT": "unit_weight",
    "PALLET  QUANTITY": "pallet_quantity",
    "STOCKING  STATUS": "stocking_status",
    "MIN  ORDER  QUANTITY": "min_order_quantity",
    "DAYS  LEAD  TIME": "lead_time_days",
    "FOB OR  DLV": "fob_or_dlv",
    "CHANGE  IN  PRICE": "price_change",
    "LIST  PRICE": "list_price",
    "FULL  PALLET  PRICE": "full_pallet_price",
    "HALF  LOAD FULL  PALLET  PRICE": "half_load_full_pallet_price",
    "FULL   LOAD FULL  PALLET  PRICE": "full_load_full_pallet_price",
    "FULL  LOAD  BEST  PRICE": "full_load_best_price",
}

# áreas (y1, x1, y2, x2) para Tabula
AREA_BY_PAGE = {1: [178.695, 16.335, 599.445, 767.745]}       # portada
DEFAULT_AREA = [81.675, 16.335, 599.445, 767.745]             # resto

_RE_NON_NUMERIC = re.compile(r"[^0-9.\-]")

# patrones para fecha y planta
_RE_DATE = re.compile(r"(\d{2}/\d{2}/\d{4})")
_RE_PLANT = re.compile(r"\d{3,4}\s*-\s*([A-Z][A-Z0-9\s]+?)\s+[A-Z]{2}\b")


# -----------------------------------------------------------------------------
# 2.  Función principal – extracción de la tabla
# -----------------------------------------------------------------------------
def extract_main_tables(pdf_path: str | pathlib.Path) -> pd.DataFrame:
    pdf_path = pathlib.Path(pdf_path).expanduser().resolve()

    # cuántas páginas tiene
    total_pages = tabula.metadata(pdf_path)["total_pages"]
    dfs: List[pd.DataFrame] = []

    for page in range(1, total_pages + 1):
        area = AREA_BY_PAGE.get(page, DEFAULT_AREA)
        print(f"⏩  Página {page}/{total_pages}  área={area}")

        try:
            df_page = tabula.read_pdf(
                pdf_path,
                pages=page,
                area=area,
                lattice=True,
                guess=False,
                pandas_options={"dtype": str},
            )[0]
        except (IndexError, ValueError):
            print(f"⚠️  Página {page}: sin tabla")
            continue

        df_page.columns = [c.strip() for c in df_page.columns]
        hdr_mask = df_page.iloc[:, 0].str.contains("PRODUCT NUMBER", na=False)
        df_page = df_page.loc[~hdr_mask]

        dfs.append(df_page)
        print(f"✅  {df_page.shape[0]} filas")

    if not dfs:
        raise ValueError("No se extrajeron tablas")

    df = pd.concat(dfs, ignore_index=True)
    df = df.rename(columns=COLUMN_MAP)
    df = df[[v for v in COLUMN_MAP.values() if v in df.columns]]

    # numéricos
    num_cols = [c for c in df.columns if any(k in c for k in ("price", "weight", "quantity"))]
    for col in num_cols:
        df[col] = (
            df[col].astype(str)
            .str.replace(_RE_NON_NUMERIC, "", regex=True)
            .replace("", pd.NA)
            .astype(float)
        )

    return df


# -----------------------------------------------------------------------------
# 3.  Funciones auxiliares
# -----------------------------------------------------------------------------
def fix_negative_numbers(df: pd.DataFrame, columns: Sequence[str] | None = None) -> pd.DataFrame:
    """
    Convierte strings como "(12.34)" o "12.34-" en ‑12.34.
    Si *columns* es None, procesa todas las numéricas.
    """
    cols = columns or df.select_dtypes("number").columns
    for col in cols:
        series = df[col].astype(str)

        mask_paren = series.str.match(r"\(.*\)")
        df.loc[mask_paren, col] = (
            "-" + series[mask_paren].str.strip("()")
        ).astype(float)

        mask_trail = series.str.endswith("-")
        df.loc[mask_trail, col] = (
            "-" + series[mask_trail].str.rstrip("-")
        ).astype(float)

        # Asegura tipo float
        df[col] = pd.to_numeric(df[col], errors="ignore")
    return df


def _extract_header_text(pdf_path: pathlib.Path, area: list[float] | None = None) -> str:
    """
    Devuelve las primeras 3–4 líneas del encabezado de la página 1 como string.
    """
    header_area = area or [0, 0, 120, 800]
    try:
        tbl = tabula.read_pdf(
            pdf_path,
            pages=1,
            area=header_area,
            stream=True,
            guess=False,
            pandas_options={"header": None, "dtype": str},
        )[0]
    except Exception:
        return ""
    return " ".join(tbl.astype(str).fillna("").agg(" ".join, axis=1).tolist())


def extract_effective_date(pdf_path: str | pathlib.Path) -> _dt.date | None:
    """
    Devuelve la fecha *Effective Date* encontrada en la cabecera.
    """
    text = _extract_header_text(pathlib.Path(pdf_path).expanduser().resolve())
    m = _RE_DATE.search(text)
    if not m:
        return None
    return _dt.datetime.strptime(m.group(1), "%m/%d/%Y").date()


def extract_plant_location(pdf_path: str | pathlib.Path) -> str | None:
    """
    Extrae la sede/planta (ej. 'STATESVILLE NC') del encabezado.
    """
    text = _extract_header_text(pathlib.Path(pdf_path).expanduser().resolve())
    m = _RE_PLANT.search(text)
    return m.group(1).title() if m else None


# -----------------------------------------------------------------------------
# 4.  Exportaciones públicas
# -----------------------------------------------------------------------------
__all__ = [
    "extract_main_tables",
    "fix_negative_numbers",
    "extract_effective_date",
    "extract_plant_location",
]

