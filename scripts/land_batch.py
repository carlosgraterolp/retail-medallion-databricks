"""Land one prepared batch into the Unity Catalog landing volume (simulated arrival of new files).

Uploads data/landing/batch_<n>/sales/*.csv and data/landing/batch_<n>/catalog/*.json with the
Databricks CLI and stops at the first failed upload (non-zero exit code).

usage: python scripts/land_batch.py <batch number> --profile <databricks cli profile>
"""

import argparse
import subprocess
import sys
from pathlib import Path

LANDING = "dbfs:/Volumes/workspace/retail_bronze/landing"
TARGETS = {"sales": f"{LANDING}/online_retail_ii_weekly", "catalog": f"{LANDING}/product_catalog"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch", type=int)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--root", default="data/landing")
    args = parser.parse_args()

    total = 0
    for kind, target in TARGETS.items():
        files = sorted((Path(args.root) / f"batch_{args.batch}" / kind).glob("*"))
        mkdir_cmd = ["databricks", "fs", "mkdir", target, "-p", args.profile]
        mkdir = subprocess.run(mkdir_cmd, capture_output=True, text=True)
        if mkdir.returncode != 0:
            print(f"FAILED ({mkdir.returncode}) creating {target}: {mkdir.stderr.strip()}")
            return mkdir.returncode
        for f in files:
            cmd = ["databricks", "fs", "cp", str(f), f"{target}/{f.name}", "-p", args.profile]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"FAILED ({result.returncode}) uploading {f.name}: {result.stderr.strip()}")
                return result.returncode
            total += 1
        print(f"batch {args.batch}: {len(files)} {kind} files landed in {target}")
    print(f"batch {args.batch}: {total} files landed, all uploads exited 0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
