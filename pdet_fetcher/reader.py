import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import polars as pl

from .constants import (
    BOOLEAN_COLUMNS,
    CAGED_COLUMNS,
    CAGED_AJUSTES_COLUMNS,
    CAGED_2020_EXC_COLUMNS,
    CAGED_2020_FOR_COLUMNS,
    CAGED_2020_MOV_COLUMNS,
    INTEGER_COLUMNS,
    NA_VALUES,
    NUMERIC_COLUMNS,
    RAIS_ESTABELECIMENTOS_COLUMNS,
    RAIS_VINCULOS_COLUMNS,
)


def parse_filename(f: Path) -> dict[str, str | int | None]:
    m = re.search(r"^([a-z0-9-]+)_([a-z0-9-]+)@(\d{8})\.(7z|zip)$", f.name)
    dataset, partition, modification, extension = m.groups()
    date_uf = partition.split("-")
    if len(date_uf) == 2:
        date, uf = date_uf
    else:
        date = date_uf[0]
        uf = None
    return {
        "filepath": f,
        "filename": f.name,
        "name": f.stem,
        "extension": extension,
        "modification": modification,
        "dataset": dataset,
        "date": int(date),
        "uf": uf,
    }


def convert_columns_dtypes(df: pl.DataFrame) -> pl.DataFrame:
    for column in df.columns:
        if column in INTEGER_COLUMNS:
            df = df.with_columns(
                pl.col(column)
                .str.replace(r" +", "")
                .str.replace(r"\.", "")
                .cast(pl.Int64)
            )
        elif column in NUMERIC_COLUMNS:
            df = df.with_columns(
                pl.col(column)
                .str.replace(r" +", "")
                .str.replace(r"\.", "")
                .str.replace(",", ".")
                .cast(pl.Float64)
            )
        elif column in BOOLEAN_COLUMNS:
            df = df.with_columns(pl.col(column).cast(pl.Int8).cast(pl.Boolean))
        else:  # Categorical
            df = df.with_columns(pl.col(column).cast(pl.Categorical))
    return df


def read_rais(filepath: Path, year: int, dataset: str, **read_csv_args) -> pl.DataFrame:
    if dataset == "vinculos":
        for y in RAIS_VINCULOS_COLUMNS:
            if year < y:
                break
            columns_names = RAIS_VINCULOS_COLUMNS[y]
    elif dataset == "estabelecimentos":
        for y in RAIS_ESTABELECIMENTOS_COLUMNS:
            if year < y:
                break
            columns_names = RAIS_ESTABELECIMENTOS_COLUMNS[y]
    print("Reading", dataset, filepath)
    df = pl.read_csv(
        filepath,
        has_header=True,
        new_columns=columns_names,
        separator=";",
        encoding="latin1",
        null_values=NA_VALUES,
        infer_schema_length=0,
        **read_csv_args,
    )
    df = convert_columns_dtypes(df)
    return df


def read_caged(
    filepath: Path, date: int, dataset: str, **read_csv_args
) -> pl.DataFrame:
    if dataset == "caged":
        encoding = "latin-1"
        for d in CAGED_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_COLUMNS[d]
    elif dataset == "caged-ajustes":
        encoding = "latin-1"
        for d in CAGED_AJUSTES_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_AJUSTES_COLUMNS[d]
    elif dataset == "caged-2020-exc":
        encoding = "utf-8"
        for d in CAGED_2020_EXC_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_2020_EXC_COLUMNS[d]
    elif dataset == "caged-2020-for":
        encoding = "utf-8"
        for d in CAGED_2020_FOR_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_2020_FOR_COLUMNS[d]
    elif dataset == "caged-2020-mov":
        encoding = "utf-8"
        for d in CAGED_2020_MOV_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_2020_MOV_COLUMNS[d]

    print("Reading", dataset, filepath)
    df = pl.read_csv(
        filepath,
        has_header=True,
        new_columns=columns_names,
        separator=";",
        encoding=encoding,
        null_values=NA_VALUES,
        infer_schema_length=0,
        **read_csv_args,
    )
    df = convert_columns_dtypes(df)
    return df


def write_parquet(df: pl.DataFrame, filepath: Path) -> Path:
    print("Writing data to", filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(filepath)
    return filepath


def decompress(file_metadata: dict[str, Any]) -> dict[str, Path]:
    compressed_filepath = file_metadata["filepath"]
    print(f"Decompressing {compressed_filepath}")
    tmp_dir = Path(tempfile.mkdtemp(prefix="pdet"))
    command = [
        "7z",
        "e",
        str(compressed_filepath),
        f"-o{tmp_dir}",
    ]
    subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    decompressed_filepath = next(tmp_dir.iterdir())
    return file_metadata | {
        "tmp_dir": tmp_dir,
        "decompressed_filepath": decompressed_filepath,
    }
