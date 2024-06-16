import argparse
import threading
import queue
import subprocess
import tempfile
import shutil
from pathlib import Path

from pdet_fetcher import reader


class Decompressor(threading.Thread):
    def __init__(self, q_in: queue.Queue, q_out: queue.Queue):
        super().__init__()
        self.q_in = q_in
        self.q_out = q_out

    def run(self):
        while True:
            file_metadata = self.q_in.get()
            decompressed = decompress(file_metadata)
            self.q_out.put(file_metadata | decompressed)
            self.q_in.task_done()


class Converter(threading.Thread):
    def __init__(self, q_in: queue.Queue, dest_dir: Path):
        super().__init__()
        self.q_in = q_in
        self.dest_dir = dest_dir

    def run(self):
        while True:
            decompressed = self.q_in.get()
            decompressed_filepath = decompressed["decompressed_filepath"]
            year = decompressed["year"]
            name = decompressed["name"]
            convert(decompressed_filepath, self.dest_dir, year, name)
            shutil.rmtree(decompressed["tmp_dir"])
            self.q_in.task_done()


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


def convert(decompressed_filepath, dest_dir, year, name):
    dest_year_dir = dest_dir / str(year)
    dest_year_dir.mkdir(exist_ok=True, parents=True)

    try:
        df = reader.read_rais(
            decompressed_filepath,
            year=year,
            dataset="vinculos" if "vinculos" in name else "estabelecimentos",
        )
        dest_filepath = dest_year_dir / f"{name}.parquet"
        print(f"Writing data to {dest_filepath}")
        reader.write_parquet(df, dest_filepath)
    except Exception as e:
        print(f"Error converting {decompressed_filepath}: {e}")


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decompress RAIS files")
    parser.add_argument("--data-dir", type=Path, help="Path to the data directory")
    parser.add_argument(
        "--dest-dir", type=Path, help="Path to the destination directory"
    )
    return parser.parse_args()


def main0():
    args = get_args()
    data_dir = args.data_dir
    dest_dir = args.dest_dir

    for file in data_dir.glob("**/*.7z"):
        file_metadata = reader.parse_filename(file)
        decompressed = decompress(file_metadata)
        decompressed_filepath = decompressed["decompressed_filepath"]
        year = decompressed["year"]
        name = decompressed["name"]
        convert(decompressed_filepath, dest_dir, year, name)
        shutil.rmtree(decompressed["tmp_dir"])
        print(f"Done {file}")


def main():
    args = get_args()
    data_dir = args.data_dir
    dest_dir = args.dest_dir

    q_in_decompress = queue.Queue()
    q_in_convert = queue.Queue()

    decompressor = Decompressor(q_in_decompress, q_in_convert)
    decompressor.start()

    converter = Converter(q_in_convert, dest_dir)
    converter.start()

    for file in data_dir.glob("**/*.7z"):
        file_metadata = reader.parse_filename(file)
        q_in_decompress.put(file_metadata)

    q_in_decompress.join()
    q_in_convert.join()


if __name__ == "__main__":
    main0()
