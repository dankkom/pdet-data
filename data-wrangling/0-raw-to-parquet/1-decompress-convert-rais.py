import argparse
import shutil
from pathlib import Path

import polars as pl

from pdet_data import reader

DIR = Path(__file__).parent.absolute()


def convert_rais(
    decompressed_filepath: Path, dataset: str, dest_filepath: Path, year: int | str
):
    try:
        df = reader.read_rais(
            decompressed_filepath,
            year=year,
            dataset=dataset,
        )
        reader.write_parquet(df, dest_filepath)
    except pl.exceptions.ComputeError as e:
        print(f"Error converting {decompressed_filepath}: {e}")
    except pl.exceptions.ShapeError as e:
        print(f"Error converting {decompressed_filepath}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Decompress RAIS files")
    parser.add_argument("-data-dir", type=Path, help="Path to the data directory")
    parser.add_argument(
        "-dest-dir", type=Path, help="Path to the destination directory"
    )
    args = parser.parse_args()
    data_dir = args.data_dir
    dest_dir = args.dest_dir

    # rais-vinculos
    dataset = "vinculos"
    data_dir_rais_vinculos = data_dir / "rais-vinculos"
    dest_dir_rais_vinculos = dest_dir / "rais-vinculos"
    for file in data_dir_rais_vinculos.glob("**/rais-*.*"):
        file_metadata = reader.parse_filename(file)
        date = file_metadata["date"]
        name = file_metadata["name"]
        dest_filepath = dest_dir_rais_vinculos / str(date) / f"{name}.parquet"
        if dest_filepath.exists():
            continue
        decompressed = reader.decompress(file_metadata)
        decompressed_filepath = decompressed["decompressed_filepath"]
        convert_rais(decompressed_filepath, dataset, dest_filepath, date)
        shutil.rmtree(decompressed["tmp_dir"])

    # rais-estabelecimentos
    dataset = "estabelecimentos"
    data_dir_rais_estabelecimentos = data_dir / "rais-estabelecimentos"
    dest_dir_rais_estabelecimentos = dest_dir / "rais-estabelecimentos"
    for file in data_dir_rais_estabelecimentos.glob("**/rais-*.*"):
        file_metadata = reader.parse_filename(file)
        date = file_metadata["date"]
        name = file_metadata["name"]
        dest_filepath = dest_dir_rais_estabelecimentos / str(date) / f"{name}.parquet"
        if dest_filepath.exists():
            continue
        decompressed = reader.decompress(file_metadata)
        decompressed_filepath = decompressed["decompressed_filepath"]
        convert_rais(decompressed_filepath, dataset, dest_filepath, date)
        shutil.rmtree(decompressed["tmp_dir"])

    print("Done converting RAIS files.")


if __name__ == "__main__":
    main()
