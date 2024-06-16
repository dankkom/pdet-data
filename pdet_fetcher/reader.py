from pathlib import Path

import numpy as np
import pandas as pd

from .constants import (
    RAIS_ESTABELECIMENTOS_COLUMNS,
    RAIS_VINCULOS_COLUMNS,
    INTEGER_COLUMNS,
    NA_VALUES,
    NUMERIC_COLUMNS,
    BOOLEAN_COLUMNS,
)


def parse_filename(f: Path) -> dict[str, str | int | None]:
    name = f.stem
    _, year_uf, _ = name.split("_")
    year_uf = year_uf.split("-")
    if len(year_uf) == 2:
        year, uf = year_uf
    else:
        year = year_uf[0]
        uf = None
    return {
        "filepath": f,
        "name": name,
        "year": int(year),
        "uf": uf,
    }


def to_int(series):
    def convert(x):
        try:
            return int(x)
        except:
            return np.nan

    return series.apply(convert)


def to_float(series):
    def convert(x):
        try:
            return float(x.replace(",", "."))
        except:
            return np.nan

    return series.apply(convert)


def to_bool(series):
    def convert(x):
        try:
            return bool(int(x))
        except:
            return None

    return series.apply(convert)


def convert_columns_dtypes(df, columns_dtypes):
    for column, dtype_func in columns_dtypes.items():
        if callable(dtype_func):
            df = df.assign(**{column: lambda x: dtype_func(x[column])})
        else:
            df[column] = df[column].astype("category")
    return df


def get_columns_dtypes(columns: tuple[str]):
    columns = {col: "TEXT" for col in columns}

    for col in columns:
        if col in INTEGER_COLUMNS:
            columns[col] = "INTEGER"
        elif col in NUMERIC_COLUMNS:
            columns[col] = "NUMERIC"
        elif col in BOOLEAN_COLUMNS:
            columns[col] = "BOOLEAN"

    columns_dtypes = {}
    for col, dtype in columns.items():
        if dtype == "TEXT":
            columns_dtypes[col] = None
        if dtype == "INTEGER":
            columns_dtypes[col] = to_int
        elif dtype == "NUMERIC":
            columns_dtypes[col] = to_float
        elif dtype == "BOOLEAN":
            columns_dtypes[col] = to_bool

    return columns, columns_dtypes


def reader_rais_vinculos(filepath: Path, year: int, **read_csv_args):
    for y in RAIS_VINCULOS_COLUMNS:
        if year < y:
            break
        columns_names = RAIS_VINCULOS_COLUMNS[y]
    dtypes = {}
    for col in columns_names:
        if col in INTEGER_COLUMNS:
            dtypes[col] = "Int64"
        elif col in NUMERIC_COLUMNS:
            dtypes[col] = "float"
        elif col in BOOLEAN_COLUMNS:
            dtypes[col] = "boolean"
        else:
            dtypes[col] = "category"
    print("Reading", filepath)
    return pd.read_csv(
        filepath,
        engine="python",
        sep="; *",
        decimal=",",
        encoding="latin1",
        skiprows=1,
        dtype=dtypes,
        names=columns_names,
        na_values=NA_VALUES,
        **read_csv_args,
    )


def reader_rais_estabelecimentos(filepath: Path, year: int, **read_csv_args):
    for y in RAIS_ESTABELECIMENTOS_COLUMNS:
        if year < y:
            break
        columns_names = RAIS_ESTABELECIMENTOS_COLUMNS[y]
    dtypes = {}
    for col in columns_names:
        if col in INTEGER_COLUMNS:
            dtypes[col] = "Int64"
        elif col in NUMERIC_COLUMNS:
            dtypes[col] = "float"
        elif col in BOOLEAN_COLUMNS:
            dtypes[col] = "boolean"
        else:
            dtypes[col] = "category"
    print("Reading", filepath)
    return pd.read_csv(
        filepath,
        engine="python",
        sep="; *",
        decimal=",",
        thousands=".",
        encoding="latin1",
        skiprows=1,
        dtype=dtypes,
        names=columns_names,
        na_values=NA_VALUES,
        **read_csv_args,
    )
