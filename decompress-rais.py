import argparse
from pathlib import Path
import subprocess


def decompress(filepath, dest_dir):
    command = [
        "7z",
        "e",
        str(filepath),
        f"-o{dest_dir}",
    ]
    subprocess.run(command)


def decompress_rais_vinculos(data_dir: Path, dest_dir: Path):
    dest_dir.mkdir(parents=True, exist_ok=True)
    for year_dir in data_dir.iterdir():
        for filepath in year_dir.iterdir():
            decompress(filepath, dest_dir=dest_dir / f"{year_dir.name}")


def decompress_rais_estabelecimentos(data_dir: Path, dest_dir: Path):
    dest_dir.mkdir(parents=True, exist_ok=True)
    for year_dir in data_dir.iterdir():
        for filepath in year_dir.iterdir():
            decompress(filepath, dest_dir=dest_dir / f"{year_dir.name}")


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decompress RAIS files")
    parser.add_argument("--data-dir", type=Path, help="Path to the data directory")
    parser.add_argument(
        "--dest-dir", type=Path, help="Path to the destination directory"
    )
    parser.add_argument(
        "--rais_vinculos",
        action="store_true",
        help="Decompress Rais Vinculos files",
    )
    parser.add_argument(
        "--rais_estabelecimentos",
        action="store_true",
        help="Decompress RAIS Estabelecimentos files",
    )
    return parser.parse_args()


def main():
    args = get_args()
    data_dir = args.data_dir
    dest_dir = args.dest_dir
    if args.rais_vinculos:
        decompress_rais_vinculos(data_dir, dest_dir)
    if args.rais_estabelecimentos:
        decompress_rais_estabelecimentos(data_dir, dest_dir)


if __name__ == "__main__":
    main()
