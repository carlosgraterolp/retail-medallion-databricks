"""Prepare the files that simulate data arriving over time (run locally, output is gitignored).

1. Sales: each source sheet is split into weekly CSV files (one file per sheet and ISO week),
   values unchanged and kept as text. The two sheets overlap (2010-12-01 to 2010-12-09), so those
   rows arrive twice, as in the original source, and silver deduplicates them.
2. Product catalog (semi-structured JSON, JSON Lines): one document per merchandise product.
   - Derived from the real transactions: `name`, `aliases` (distinct descriptions), `price_history`
     (distinct positive unit prices with the first date each was seen), `first_sold_date`.
   - SYNTHETIC (keyword rules on the description, see CATEGORY_RULES): `attributes.category`,
     `attributes.is_seasonal`, and `tags` (only in batch 3, to demonstrate schema evolution).
   A product goes into the batch of its first sale date.

Batches (by week start date): 1 = weeks before 2010-07-01, 2 = weeks before 2011-04-01, 3 = the rest.

usage: python scripts/make_landing_batches.py data/raw data/landing
"""

import json
import re
import sys
from pathlib import Path

import pandas as pd

SHEETS = {
    "2009_2010": "online_retail_ii_2009_2010.csv",
    "2010_2011": "online_retail_ii_2010_2011.csv",
}
BATCH_ENDS = [pd.Timestamp("2010-07-01"), pd.Timestamp("2011-04-01")]  # exclusive upper bounds

NON_MERCHANDISE = {
    "POST", "DOT", "M", "C2", "D", "S", "B", "BANK CHARGES", "ADJUST", "ADJUST2",
    "AMAZONFEE", "PADS", "CRUK", "TEST001", "TEST002",
}

# SYNTHETIC category rules: first matching rule wins (order matters).
CATEGORY_RULES = [
    ("Christmas & Seasonal", r"CHRISTMAS|XMAS|ADVENT|SANTA|EASTER|HALLOWEEN|VALENTINE|REINDEER|SNOWFLAKE"),
    ("Lighting & Candles", r"LIGHT|LANTERN|CANDLE|T-LIGHT|TEALIGHT|LAMP"),
    ("Bags & Storage", r"\bBAG|LUNCH BOX|STORAGE|BASKET|\bTIN\b|\bTINS\b|\bJAR\b|\bBOX\b|HAMPER"),
    ("Kitchen & Dining", r"\bMUG|\bCUP|PLATE|BOWL|CAKE|\bTEA\b|TEAPOT|NAPKIN|CUTLERY|SPOON|BAKING|JUG|TRAY|COASTER"),
    ("Stationery & Party", r"CARD|PAPER|PENCIL|NOTEBOOK|\bWRAP|BUNTING|PARTY|STICKER|CHALK|BALLOON|ENVELOPE"),
    ("Toys & Kids", r"\bTOY|DOLLY|CHILDREN|\bKIDS|GAME|SPACEBOY|\bDOLL|JIGSAW|SKIPPING"),
    ("Garden", r"GARDEN|PLANT|FLOWER POT|BIRD|WATERING|PLANTER"),
    ("Home Decor", r"SIGN|FRAME|CLOCK|MIRROR|CUSHION|DOORSTOP|HOOK|WREATH|HEART|ORNAMENT|DECORATION|DOORMAT"),
]
SEASONAL = r"CHRISTMAS|XMAS|ADVENT|SANTA|EASTER|HALLOWEEN|VALENTINE|REINDEER|SNOWFLAKE"
# SYNTHETIC tags vocabulary (batch 3 only): colours and materials found in the description.
TAG_WORDS = ["RED", "PINK", "BLUE", "GREEN", "WHITE", "BLACK", "IVORY", "GOLD", "SILVER",
             "GLASS", "METAL", "WOODEN", "CERAMIC", "ENAMEL", "FELT", "ZINC", "VINTAGE", "RETROSPOT"]


def batch_of(ts: pd.Timestamp) -> int:
    for i, end in enumerate(BATCH_ENDS, start=1):
        if ts < end:
            return i
    return len(BATCH_ENDS) + 1


def category(desc: str) -> str:
    for name, pattern in CATEGORY_RULES:
        if re.search(pattern, desc):
            return name
    return "Other"


def main(raw_dir: str, out_dir: str) -> None:
    raw, out = Path(raw_dir), Path(out_dir)
    frames = []
    for sheet, filename in SHEETS.items():
        df = pd.read_csv(raw / filename, dtype=str, keep_default_na=False)
        ts = pd.to_datetime(df["InvoiceDate"], format="%Y-%m-%d %H:%M:%S")
        week = (ts - pd.to_timedelta(ts.dt.day_of_week, unit="D")).dt.normalize()
        for wk, part in df.groupby(week, sort=True):
            b = batch_of(wk)
            target = out / f"batch_{b}" / "sales" / f"online_retail_ii_{sheet}_week_{wk:%Y-%m-%d}.csv"
            target.parent.mkdir(parents=True, exist_ok=True)
            part.to_csv(target, index=False)
        frames.append(df.assign(_ts=ts))

    # Catalog from all sales lines with a real sale (positive quantity and price, not cancelled).
    sales = pd.concat(frames)
    sales = sales[~sales["Invoice"].str.startswith("C")]
    sales["code"] = sales["StockCode"].str.strip().str.upper()
    sales["price"] = pd.to_numeric(sales["Price"], errors="coerce")
    sales["qty"] = pd.to_numeric(sales["Quantity"], errors="coerce")
    sales["desc"] = sales["Description"].str.strip().str.replace(r"\s+", " ", regex=True)
    sales = sales[(sales["price"] > 0) & (sales["qty"] > 0) & (sales["desc"] != "")]
    sales = sales[~sales["code"].isin(NON_MERCHANDISE) & ~sales["code"].str.startswith("GIFT_")]

    counts = {1: 0, 2: 0, 3: 0}
    files = {b: (out / f"batch_{b}" / "catalog" / f"product_catalog_batch_{b}.json") for b in counts}
    for f in files.values():
        f.parent.mkdir(parents=True, exist_ok=True)
    handles = {b: open(f, "w") for b, f in files.items()}
    for code, g in sales.sort_values("_ts").groupby("code", sort=True):
        latest_name = g["desc"].iloc[-1]
        prices = g.groupby("price")["_ts"].min().sort_values()
        doc = {
            "stock_code": code,
            "name": latest_name,
            "aliases": sorted(g["desc"].unique().tolist()),
            "first_sold_date": f"{g['_ts'].min():%Y-%m-%d}",
            "attributes": {  # SYNTHETIC
                "category": category(latest_name),
                "is_seasonal": bool(re.search(SEASONAL, latest_name)),
            },
            "price_history": [
                {"valid_from": f"{ts:%Y-%m-%d}", "unit_price": round(float(p), 3)} for p, ts in prices.items()
            ],
        }
        b = batch_of(g["_ts"].min())
        if b == 3:  # schema evolution: a new top-level field appears only in the last batch
            doc["tags"] = [w.lower() for w in TAG_WORDS if re.search(rf"\b{w}\b", latest_name)]  # SYNTHETIC
        handles[b].write(json.dumps(doc) + "\n")
        counts[b] += 1
    for h in handles.values():
        h.close()

    for b in (1, 2, 3):
        n_files = len(list((out / f"batch_{b}" / "sales").glob("*.csv")))
        n_rows = sum(len(pd.read_csv(p, dtype=str)) for p in (out / f"batch_{b}" / "sales").glob("*.csv"))
        print(f"batch {b}: {n_files} sales files, {n_rows:,} sales rows, {counts[b]:,} catalog documents")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
