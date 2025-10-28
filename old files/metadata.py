#!/usr/bin/env python3
import argparse
import math
import os
from pathlib import Path
import pandas as pd

def read_parquet(path: str, columns=None, use_pyarrow: bool = True) -> pd.DataFrame:
    # pandas.read_parquet uses pyarrow if available; columns filters projection
    df = pd.read_parquet(path, columns=columns, engine="pyarrow" if use_pyarrow else None)
    return df

def write_excel(df: pd.DataFrame, out_path: str, sheet_name: str = "Sheet1") -> None:
    out_path = str(out_path)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

def chunk_and_write_excel(
    df: pd.DataFrame,
    out_dir: str,
    base_name: str,
    chunk_size: int = 20000,
    sheet_name: str = "Sheet1",
) -> list[str]:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    n = len(df)
    if n == 0:
        return []
    num_chunks = math.ceil(n / chunk_size)
    paths = []
    for i in range(num_chunks):
        start = i * chunk_size
        stop = min((i + 1) * chunk_size, n)
        chunk = df.iloc[start:stop]
        out_path = os.path.join(out_dir, f"{base_name}_part{i+1:03d}_{start+1}-{stop}.xlsx")
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            chunk.to_excel(writer, index=False, sheet_name=sheet_name)
        paths.append(out_path)
    return paths

def main():
    ap = argparse.ArgumentParser(description="Convert Parquet to Excel and split into 20k-row Excel files.")
    ap.add_argument("--in", dest="in_path", required=True, help="Input Parquet file or directory")
    ap.add_argument("--out-xlsx", dest="out_xlsx", required=True, help="Output Excel file path for full dataset")
    ap.add_argument("--split-dir", dest="split_dir", required=True, help="Directory to write split Excel files")
    ap.add_argument("--base-name", dest="base_name", default="sentences", help="Base name for split files")
    ap.add_argument("--chunk-size", type=int, default=20000, help="Rows per split Excel (default 20000)")
    ap.add_argument("--columns", nargs="*", help="Optional list of columns to keep (projection)")
    ap.add_argument("--sheet", default="Sheet1", help="Excel sheet name (default Sheet1)")
    ap.add_argument("--filters", nargs="*", help="Optional pandas query string to filter rows, e.g., lang=='ne' and len(text)>0")
    args = ap.parse_args()

    df = read_parquet(args.in_path, columns=args.columns, use_pyarrow=True)  # fast via PyArrow [web:112][web:119]
    if args.filters:
        # Join tokens back into a single query string if split by spaces on CLI
        query_str = " ".join(args.filters)
        df = df.query(query_str, engine="python")

    # Write full dataset to one Excel workbook
    write_excel(df, args.out_xlsx, sheet_name=args.sheet)  # [web:120]

    # Split into 20k-row Excel workbooks
    paths = chunk_and_write_excel(
        df,
        out_dir=args.split_dir,
        base_name=args.base_name,
        chunk_size=args.chunk_size,
        sheet_name=args.sheet,
    )  # [web:117][web:111]

    print(f"Wrote full Excel: {args.out_xlsx}")
    print("Wrote split files:")
    for p in paths:
        print(p)

if __name__ == "__main__":
    main()
