#!/usr/bin/env python3
"""kunai_to_parquet.py — Python port of kunai_to_parquet.nu (polars kept).

Convert a kunai events log (raw .jsonl ndjson or compressed .gz) to a
Parquet file. Lazy is the default: the ndjson parsing + schema inference
happen only once and a .gz is read directly, without any temporary file.

    python3 kunai_to_parquet.py kunai.jsonl
    python3 kunai_to_parquet.py kunai.jsonl.gz
    python3 kunai_to_parquet.py kunai.jsonl --output out.parquet
    python3 kunai_to_parquet.py kunai.jsonl --eager
    python3 kunai_to_parquet.py kunai.jsonl --noflat

Options:
- --infer-schema N   rows used to infer the schema (default 200000; below
  200000 the inference can fail on big batches)
- --eager            eager conversion (6x faster but very RAM hungry: it
  saturates memory on big gz; lazy is the default because it works everywhere)
- --noflat           do not flatten data/info (keep the raw structure)
- --output FILE      exact name of the output parquet (default: next to the
  source, unambiguous name, cf. default_output)

OUTPUT NAME: a .gz and its unzipped .jsonl converge to the SAME parquet:
    kunai.jsonl    -> kunai.jsonl.parquet
    kunai.jsonl.gz -> kunai.jsonl.parquet

NON DESTRUCTIVE: the .gz/.jsonl source is NEVER deleted nor overwritten. In
eager mode the .gz is unzipped to a temporary file (polars cannot read a
compressed gz in eager), converted, then the temporary is removed (even on
failure). In lazy mode (default) the gz is read directly, no temporary.
"""

import argparse
import gzip
import os
import shutil
import sys

import polars as pl


# Default output name: so that a .gz and its unzipped .jsonl converge to the
# SAME parquet, only strip an eventual `.gz` suffix, keep the rest of the
# name and add `.parquet`:
#   test.jsonl.gz -> test.jsonl.parquet
#   test.jsonl    -> test.jsonl.parquet
def default_output(eventslog: str) -> str:
    extension = os.path.splitext(eventslog)[1].lstrip(".")
    if extension == "gz":
        return eventslog[: -len(".gz")] + ".parquet"
    if extension == "parquet":
        return eventslog
    return eventslog + ".parquet"


def save_into_parquet(
    eventslog: str,
    eager_param: str,
    infer_schema_num: int,
    noflat_param: str,
    output: str,
) -> None:
    parquetfile = output
    print(
        f"converting  {eventslog} to {parquetfile} {eager_param} "
        f"infer-schema={infer_schema_num} flat={noflat_param}"
    )
    if eager_param == "--lazy":
        frame = pl.scan_ndjson(eventslog, infer_schema_length=infer_schema_num)
    else:
        frame = pl.read_ndjson(eventslog, infer_schema_length=infer_schema_num)
    if noflat_param == "flat":
        frame = frame.unnest(["data", "info"])
    if eager_param == "--lazy":
        frame.sink_parquet(parquetfile)
    else:
        frame.write_parquet(parquetfile)


def main(
    kunai_events_log_file: str,
    infer_schema: int = 200000,  # Number of rows to infer schema. under 200000 it failed
    eager: bool = False,         # eager is 6x faster but very RAM hungry; lazy is the default
    noflat: bool = False,        # do not flattenize at all, just convert to parquet
    output: str | None = None,   # exact output parquet name (default: next to the source)
) -> None:
    eager_param = "--eager" if eager else "--lazy"
    noflat_param = "noflat" if noflat else "flat"

    # the source file must exist
    if not os.path.exists(kunai_events_log_file):
        print(f"file {kunai_events_log_file} not found")
        return

    file_extension = os.path.splitext(kunai_events_log_file)[1].lstrip(".")
    if file_extension == "parquet":
        print(f"skipping parquet file {kunai_events_log_file} already converted")
        return

    out = output or default_output(kunai_events_log_file)

    try:
        # In eager mode, a compressed .gz cannot be read directly by polars.
        # Unzip it to a TEMPORARY file (the .gz source stays untouched),
        # convert the temp to the final parquet, then remove the temp — in
        # ALL cases (even if the conversion fails, e.g. polars memory
        # exhaustion).
        if file_extension == "gz" and eager_param == "--eager":
            temp_unzipped = kunai_events_log_file[: -len(".gz")] + ".unzip.tmp"
            try:
                with gzip.open(kunai_events_log_file, "rb") as fin, open(
                    temp_unzipped, "wb"
                ) as fout:
                    shutil.copyfileobj(fin, fout)
            except OSError as err:
                if os.path.exists(temp_unzipped):
                    os.remove(temp_unzipped)
                print(f"conversion failed: {err}")
                sys.exit(1)
            print(
                f"unzipped - non destructive - from {kunai_events_log_file} "
                f"to {temp_unzipped}"
            )
            try:
                save_into_parquet(
                    temp_unzipped, eager_param, infer_schema, noflat_param, out
                )
            finally:
                if os.path.exists(temp_unzipped):
                    os.remove(temp_unzipped)
        else:
            save_into_parquet(
                kunai_events_log_file, eager_param, infer_schema, noflat_param, out
            )
    except Exception as err:
        print(f"conversion failed: {err}")
        sys.exit(1)
    print(f"parquet saved: {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert a kunai events log (ndjson) to a parquet file"
    )
    parser.add_argument("kunai_events_log_file")
    parser.add_argument(
        "--infer-schema",
        type=int,
        default=200000,
        help="Number of rows to infer schema (default 200000)",
    )
    parser.add_argument(
        "--eager",
        action="store_true",
        help="eager mode is *6 faster than lazy mode but very RAM hungry "
        "(saturates memory on big gz); lazy is the default",
    )
    parser.add_argument(
        "--noflat",
        action="store_true",
        help="do not flattenize at all, just convert to parquet",
    )
    parser.add_argument(
        "--output",
        help="exact output parquet name (default: next to the source)",
    )
    args = parser.parse_args()
    main(
        args.kunai_events_log_file,
        args.infer_schema,
        args.eager,
        args.noflat,
        args.output,
    )
    sys.exit(0)
