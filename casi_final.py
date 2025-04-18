"""
Purina – Horizontal price‑list reader (Statesville layout)
=========================================================
Reads *horizontal* PDF price sheets from Purina, cleans the tables and
returns a single, tidy **pandas.DataFrame** ready for further processing.

Fixes included in this version
------------------------------
1. **Keeps the first real data row of every page** (e.g. `5555 AQUAMAX FINGERLING 300`).
2. **Removes the spurious placeholder row** that sometimes appears at
   the very top with only one non‑null value (usually the string
   ``"PRICE IN US DOLLAR"``).
3. Normalises numeric columns (removes commas, fixes negative numbers
   shown in parentheses).
4. Adds debug `print()` statements so you can trace execution step by
   step (as requested by Andy).

Exposed helpers
---------------
* ``read_file(pdf_path: str | pathlib.Path) -> pd.DataFrame`` – main entry point.
* ``extract_effective_date(pdf_path) -> datetime.date`` – stub; fill if needed.
* ``extract_plant_location(pdf_path) -> str`` – stub; fill if needed.
"""

from __future__ import annotations

import datetime as _dt
import pathlib
import re
from typing import Iterable, List, Optional

import pandas as pd
import tabula

# -----------------------------------------------------------------------------
# 1.  Constants and configuration
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

NUMERIC_COLS: List[str] = COLUMN_NAMES[10:]

# -----------------------------------------------------------------------------
# 2.  Low‑level helpers
# -----------------------------------------------------------------------------

def _read_tables(pdf_path: str | pathlib.Path) -> List[pd.DataFrame]:
    """Read *all* tables from every page using **lattice** mode (robust for
    Statesville). Returns the raw list of DataFrames straight from
    *tabula‑py*.
    """
    print("→ Reading tables with tabula…")
    tables = tabula.read_pdf(
        str(pdf_path),
        pages="all",
        lattice=True,
        multiple_tables=True,
        guess=False,
        pandas_options={"header": None},
    )
    print(f"   {len(tables)} table fragments detected")
    return tables


def _standardise_table(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Clean a single *tabula* DataFrame:
    * Drop completely‑empty columns.
    * Strip whitespace from strings.
    * Coerce the header row (now at *df.iloc[0]*?) to real columns.
    * Return **None** if shape is clearly not a data table (e.g. ≤ 2 columns).
    """
    if df.shape[1] < 3:
        return None

    # Remove columns that are all NaN
    df = df.dropna(axis=1, how="all")

    # Reset columns either from the first row (old PDF flavour) or use
    # pre‑defined constant list
    if df.iloc[0].str.contains("PRODUCT", case=False, na=False).any():
        df.columns = df.iloc[0].str.strip().str.lower().str.replace(" ", "_")
        df = df.iloc[1:].reset_index(drop=True)
    else:
        df.columns = COLUMN_NAMES[: df.shape[1]]

    # Strip all strings
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    return df


def _fix_negative(value: str | float | int | None) -> str | float | int | None:
    """Convert (1,234.56) → -1234.56 and strip commas."""
    if isinstance(value, str):
        value = value.replace(",", "")
        match = re.match(r"^\(([-+]?\d*\.?\d+)\)$", value)
        if match:
            return -float(match.group(1))
        try:
            return float(value)
        except ValueError:
            return value  # leave as‑is (probably empty or text)
    return value


def _clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_fix_negative)
    return df


def _remove_placeholder_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the annoying first row that only contains something like
    ``"PRICE IN US DOLLAR"`` in a single cell.
    We detect rows that have **exactly one** non‑null value *and* the rest
    are null/NaN.
    """
    sentinel_mask = df.notna().sum(axis=1) == 1
    if sentinel_mask.any():
        print("→ Removing placeholder rows: ", sentinel_mask.sum())
    return df.loc[~sentinel_mask].reset_index(drop=True)

# -----------------------------------------------------------------------------
# 3.  Public helpers
# -----------------------------------------------------------------------------

def read_file(pdf_path: str | pathlib.Path) -> pd.DataFrame:
    """Main helper – returns a clean DataFrame for *pdf_path*."""
    pdf_path = pathlib.Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(pdf_path)

    tables = _read_tables(pdf_path)
    std_tables = [_t for t in (_standardise_table(t) for t in tables) if _t is not None]
    if not std_tables:
        raise ValueError("No usable tables found – check the PDF or the coordinates.")

    data = pd.concat(std_tables, ignore_index=True)
    print(f"→ Combined DataFrame shape: {data.shape}")

    # Remove placeholder/header fragment rows first
    data = _remove_placeholder_rows(data)

    # Normalise numeric columns
    data = _clean_numeric(data)

    # Final sanity sort (by product_number if present)
    if "product_number" in data.columns:
        data = data.sort_values("product_number", ignore_index=True)

    print("→ Final shape after cleaning: ", data.shape)
    return data


# -----------------------------------------------------------------------------
# 4.  Metadata extractors (stubs – implement for your project)
# -----------------------------------------------------------------------------

def extract_effective_date(pdf_path: str | pathlib.Path) -> _dt.date:
    """Return the *effective date* printed on the PDF (stub)."""
    return _dt.date.today()


def extract_plant_location(pdf_path: str | pathlib.Path) -> str:
    """Return the *plant location* printed on the PDF (stub)."""
    return "Statesville"


# -----------------------------------------------------------------------------
# 5.  Quick‑and‑dirty test harness
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python purina_file_horizontal.py <file.pdf>")
        sys.exit(1)

    pdf = pathlib.Path(sys.argv[1])
    df = read_file(pdf)
    print("\n--- DataFrame preview ---")
    print(df.head())
    # Optional: save to CSV for manual inspection
    csv_out = pdf.with_suffix(".csv")
    df.to_csv(csv_out, index=False)
    print(f"✅ Saved CSV → {csv_out}")
