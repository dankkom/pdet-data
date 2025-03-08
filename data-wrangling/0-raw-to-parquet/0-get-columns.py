import argparse
import csv
import shutil
from pathlib import Path

import polars as pl

from pdet_data import reader

DIR = Path(__file__).parent.absolute()


def get_columns(filepath: Path, encoding: str = "latin-1") -> list[str]:
    df: pl.DataFrame = pl.read_csv(
        filepath,
        n_rows=1,
        encoding=encoding,
        separator=";",
        has_header=True,
        infer_schema_length=0,
    )
    return df.columns


def get_rais_estabelecimentos_columns(data_dir: Path):
    f = open(
        DIR / "rais-estabelecimentos-columns.csv", "w", encoding="utf-8", newline="\n"
    )
    writer = csv.DictWriter(f, fieldnames=["column", "order", "name", "date", "uf"])
    writer.writeheader()
    for file in data_dir.rglob("rais-*.*"):
        print(file)
        file_metadata = reader.parse_filename(file)
        file_metadata = reader.decompress(file_metadata)
        columns = get_columns(file_metadata["decompressed_filepath"])
        shutil.rmtree(file_metadata["tmp_dir"])
        print(file)
        print(columns)
        df_metadata = [
            {
                "column": column,
                "order": order,
                "name": file_metadata["name"],
                "date": file_metadata["date"],
                "uf": file_metadata["uf"],
            }
            for order, column in enumerate(columns)
        ]
        writer.writerows(df_metadata)
    f.close()


def get_rais_vinculos_columns(data_dir: Path):
    f = open(DIR / "rais-vinculos-columns.csv", "w", encoding="utf-8", newline="\n")
    writer = csv.DictWriter(f, fieldnames=["column", "order", "name", "date", "uf"])
    writer.writeheader()
    for file in data_dir.rglob("rais-*.*"):
        print(file)
        file_metadata = reader.parse_filename(file)
        file_metadata = reader.decompress(file_metadata)
        columns = get_columns(file_metadata["decompressed_filepath"])
        shutil.rmtree(file_metadata["tmp_dir"])
        print(file)
        print(columns)
        df_metadata = [
            {
                "column": column,
                "order": order,
                "name": file_metadata["name"],
                "date": file_metadata["date"],
                "uf": file_metadata["uf"],
            }
            for order, column in enumerate(columns)
        ]
        writer.writerows(df_metadata)
    f.close()


def get_caged_columns(data_dir: Path):
    f = open(DIR / "caged-columns.csv", "w", encoding="utf-8", newline="\n")
    writer = csv.DictWriter(f, fieldnames=["column", "order", "name", "date", "uf"])
    writer.writeheader()
    for file in data_dir.rglob("caged_*.*"):
        print(file)
        file_metadata = reader.parse_filename(file)
        file_metadata = reader.decompress(file_metadata)
        columns = get_columns(file_metadata["decompressed_filepath"])
        shutil.rmtree(file_metadata["tmp_dir"])
        print(file)
        print(columns)
        df_metadata = [
            {
                "column": column,
                "order": order,
                "name": file_metadata["name"],
                "date": file_metadata["date"],
            }
            for order, column in enumerate(columns)
        ]
        writer.writerows(df_metadata)
    f.close()


def get_caged_ajustes_columns(data_dir: Path):
    f = open(DIR / "caged-ajustes-columns.csv", "w", encoding="utf-8", newline="\n")
    writer = csv.DictWriter(f, fieldnames=["column", "order", "name", "date", "uf"])
    writer.writeheader()
    for file in data_dir.rglob("caged-ajustes_*.*"):
        print(file)
        file_metadata = reader.parse_filename(file)
        file_metadata = reader.decompress(file_metadata)
        columns = get_columns(file_metadata["decompressed_filepath"])
        shutil.rmtree(file_metadata["tmp_dir"])
        print(file)
        print(columns)
        df_metadata = [
            {
                "column": column,
                "order": order,
                "name": file_metadata["name"],
                "date": file_metadata["date"],
            }
            for order, column in enumerate(columns)
        ]
        writer.writerows(df_metadata)
    f.close()


def get_caged_2020_columns(data_dir: Path):
    f = open(DIR / "caged-2020-columns.csv", "w", encoding="utf-8", newline="\n")
    writer = csv.DictWriter(f, fieldnames=["column", "order", "name", "date", "uf"])
    writer.writeheader()
    for file in data_dir.rglob("caged-2020-*.*"):
        print(file)
        file_metadata = reader.parse_filename(file)
        file_metadata = reader.decompress(file_metadata)
        columns = get_columns(file_metadata["decompressed_filepath"], encoding="utf-8")
        shutil.rmtree(file_metadata["tmp_dir"])
        print(file)
        print(columns)
        df_metadata = [
            {
                "column": column,
                "order": order,
                "name": file_metadata["name"],
                "date": file_metadata["date"],
            }
            for order, column in enumerate(columns)
        ]
        writer.writerows(df_metadata)
    f.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-data-dir", type=Path)
    parser.add_argument("dataset")
    args = parser.parse_args()
    data_dir = args.data_dir
    dataset = args.dataset
    match dataset:
        case "rais-estabelecimentos":
            get_rais_estabelecimentos_columns(data_dir)
        case "rais-vinculos":
            get_rais_vinculos_columns(data_dir)
        case "caged":
            get_caged_columns(data_dir)
        case "caged-ajustes":
            get_caged_ajustes_columns(data_dir)
        case "caged-2020":
            get_caged_2020_columns(data_dir)


if __name__ == "__main__":
    main()
