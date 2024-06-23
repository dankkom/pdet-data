from pathlib import Path

import polars as pl

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


def to_category(series: pl.Series):
    def convert(x):
        try:
            x = x.strip()
            if x in NA_VALUES:
                return None
            return x
        except:
            return None

    return series.apply(convert, return_dtype=pl.String).cast(pl.Categorical)


def to_int(series: pl.Series):
    def convert(x):
        try:
            return int(x)
        except:
            return None

    return series.apply(convert, return_dtype=pl.Int64)


def to_float(series: pl.Series):
    def convert(x):
        try:
            return float(x.replace(",", "."))
        except:
            return None

    return series.apply(convert, return_dtype=pl.Float64)


def to_bool(series: pl.Series):
    def convert(x):
        try:
            return bool(int(x))
        except:
            return None

    return series.apply(convert, return_dtype=pl.Boolean)


def convert_columns_dtypes(df, columns_dtypes):
    for column, dtype_func in columns_dtypes.items():
        if callable(dtype_func):
            print(f"Converting {column} to {dtype_func.__name__}")
            if column in INTEGER_COLUMNS:
                return_dtype = pl.Int64
            elif column in NUMERIC_COLUMNS:
                return_dtype = pl.Float64
            elif column in BOOLEAN_COLUMNS:
                return_dtype = pl.Boolean
            else:
                return_dtype = None
            df = df.with_columns(
                [pl.col(column).map_batches(dtype_func, return_dtype=return_dtype)]
            )
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
            columns_dtypes[col] = to_category
        if dtype == "INTEGER":
            columns_dtypes[col] = to_int
        elif dtype == "NUMERIC":
            columns_dtypes[col] = to_float
        elif dtype == "BOOLEAN":
            columns_dtypes[col] = to_bool

    return columns, columns_dtypes


def read_rais(filepath: Path, year: int, dataset: str, **read_csv_args):
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
    _, columns_dtypes = get_columns_dtypes(columns_names)
    df = convert_columns_dtypes(df, columns_dtypes)
    return df


def write_parquet(df: pl.DataFrame, filepath: Path) -> Path:
    print("Writing data to", filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(filepath)
    return filepath
