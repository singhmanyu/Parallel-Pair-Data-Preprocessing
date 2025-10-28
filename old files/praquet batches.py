#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

def iter_batches(parquet_path: str, batch_size: int):
    pf = pq.ParquetFile(parquet_path)
    for rg in range(pf.num_row_groups):
        for batch in pf.iter_batches(row_groups=[rg], batch_size=batch_size):
            yield batch

def write_part(batches: List[pa.RecordBatch], out_path: str, compression: str = "snappy", row_group_size: int = 100_000):
    if not batches:
        return
    table = pa.Table.from_batches(batches)
    pq.write_table(table, out_path, compression=compression, row_group_size=row_group_size)

def split_parquet_by_rows(
    in_path: str,
    out_dir: str,
    base_name: str = "part",
    rows_per_file: int = 200_000,
    read_batch_rows: int = 100_000,
    write_row_group_rows: int = 100_000,
    compression: str = "snappy",
):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    part_idx = 1
    acc_batches: List[pa.RecordBatch] = []
    acc_rows = 0

    for batch in iter_batches(in_path, batch_size=read_batch_rows):
        b_rows = batch.num_rows
        # If the next batch would exceed rows_per_file, flush current accumulator first
        if acc_rows > 0 and acc_rows + b_rows > rows_per_file:
            out_path = os.path.join(out_dir, f"{base_name}_{part_idx:03d}.parquet")
            write_part(acc_batches, out_path, compression=compression, row_group_size=write_row_group_rows)
            part_idx += 1
            acc_batches = []
            acc_rows = 0

        acc_batches.append(batch)
        acc_rows += b_rows

        # If exactly hit or exceeded after adding batch, flush
        if acc_rows >= rows_per_file:
            out_path = os.path.join(out_dir, f"{base_name}_{part_idx:03d}.parquet")
            write_part(acc_batches, out_path, compression=compression, row_group_size=write_row_group_rows)
            part_idx += 1
            acc_batches = []
            acc_rows = 0

    # Flush any remainder
    if acc_rows > 0 and acc_batches:
        out_path = os.path.join(out_dir, f"{base_name}_{part_idx:03d}.parquet")
        write_part(acc_batches, out_path, compression=compression, row_group_size=write_row_group_rows)

def main():
    ap = argparse.ArgumentParser(description="Split a Parquet file into smaller Parquet files by row count.")
    ap.add_argument("--in", dest="in_path", required=True, help="Input .parquet file path")
    ap.add_argument("--out-dir", required=True, help="Output directory for parts")
    ap.add_argument("--base-name", default="part", help="Base name for output files (default: part)")
    ap.add_argument("--rows-per-file", type=int, default=200_000, help="Target number of rows per output file")
    ap.add_argument("--read-batch-rows", type=int, default=100_000, help="Rows per read batch when streaming")  # decreased batch size
    ap.add_argument("--write-row-group-rows", type=int, default=100_000, help="Rows per row group in output files")
    ap.add_argument("--compression", default="snappy", help="Compression codec: snappy, gzip, zstd, none")
    args = ap.parse_args()

    split_parquet_by_rows(
        in_path=args.in_path,
        out_dir=args.out_dir,
        base_name=args.base_name,
        rows_per_file=args.rows_per_file,
        read_batch_rows=args.read_batch_rows,
        write_row_group_rows=args.write_row_group_rows,
        compression=args.compression,
    )

if __name__ == "__main__":
    main()
