import argparse
import shutil
from pathlib import Path

import polars as pl

from pdet_fetcher import reader


def convert_rais(decompressed_filepath, dataset, dest_filepath, year):
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


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decompress RAIS files")
    parser.add_argument("--data-dir", type=Path, help="Path to the data directory")
    parser.add_argument(
        "--dest-dir", type=Path, help="Path to the destination directory"
    )
    return parser.parse_args()


def main():
    args = get_args()
    data_dir = args.data_dir
    dest_dir = args.dest_dir

    for file in data_dir.glob("**/rais-*.*"):
        file_metadata = reader.parse_filename(file)
        year = file_metadata["year"]
        name = file_metadata["name"]
        dataset = "vinculos" if "vinculos" in name else "estabelecimentos"
        dest_filepath = dest_dir / str(year) / f"{name}.parquet"
        if dest_filepath.exists():
            continue
        decompressed = reader.decompress(file_metadata)
        decompressed_filepath = decompressed["decompressed_filepath"]
        convert_rais(decompressed_filepath, dataset, dest_filepath, year)
        shutil.rmtree(decompressed["tmp_dir"])
        print(f"Done {file}")


if __name__ == "__main__":
    main()
