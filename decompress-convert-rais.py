import argparse
import subprocess
import tempfile
import shutil
from pathlib import Path

import polars as pl

from pdet_fetcher import reader


def decompress(file_metadata) -> dict[str, Path]:
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


def convert_vinculos(decompressed_filepath, dest_filepath, year):
    try:
        df = reader.read_rais(
            decompressed_filepath,
            year=year,
            dataset="vinculos",
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

    for file in data_dir.glob("**/*.7z"):
        file_metadata = reader.parse_filename(file)
        year = file_metadata["year"]
        name = file_metadata["name"]
        dest_filepath = dest_dir / str(year) / f"{name}.parquet"
        if dest_filepath.exists():
            continue
        decompressed = decompress(file_metadata)
        decompressed_filepath = decompressed["decompressed_filepath"]
        convert_vinculos(decompressed_filepath, dest_filepath, year)
        shutil.rmtree(decompressed["tmp_dir"])
        print(f"Done {file}")


if __name__ == "__main__":
    main()
