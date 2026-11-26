"""Convert the UCI Online Retail II workbook into one CSV per sheet.

Why: Spark on Databricks serverless has no native .xlsx reader, so we convert
locally and upload CSVs to a Unity Catalog volume. Every cell is kept as text
(no type inference, no cleaning) so bronze receives the source values as-is.

Usage:
    python scripts/prepare_data.py data/raw/online_retail_II.xlsx data/raw/
"""

import hashlib
import sys
from pathlib import Path

import pandas as pd


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(xlsx: str, out_dir: str) -> None:
    src, out = Path(xlsx), Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    print(f"source: {src.name} sha256={sha256(src)}")

    sheets = pd.read_excel(src, sheet_name=None, dtype=str, engine="openpyxl")
    for name, df in sheets.items():
        # "Year 2009-2010" -> online_retail_ii_2009_2010.csv
        years = name.replace("Year", "").strip().replace("-", "_")
        target = out / f"online_retail_ii_{years}.csv"
        df.to_csv(target, index=False)
        print(f"{name!r}: {len(df):,} rows -> {target.name}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
