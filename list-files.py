import argparse
from pathlib import Path
from pprint import pprint
from pdet_fetcher import fetch, storage


def list_files(ftp, dest_dir):
    try:
        for f in fetch.list_caged(ftp):
            dest_filepath = storage.get_caged_filepath(f, dest_dir)
            if dest_filepath.exists():
                continue
            print(f["full_path"], "-->", dest_filepath)

        for f in fetch.list_rais(ftp):
            dest_filepath = storage.get_rais_filepath(f, dest_dir)
            if dest_filepath.exists():
                continue
            print(f["full_path"], "-->", dest_filepath)

        for f in fetch.list_caged_2020(ftp):
            pprint(f)
            dest_filepath = storage.get_caged_2020_filepath(f, dest_dir)
            if dest_filepath.exists():
                continue
            print(f["full_path"], "-->", dest_filepath)
    finally:
        ftp.close()


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List files")
    parser.add_argument(
        "dest_dir",
        type=Path,
        help="Destination directory",
    )
    return parser.parse_args()


def main():
    args = get_args()
    dest_dir = args.dest_dir
    ftp = fetch.connect()
    list_files(ftp, dest_dir)


if __name__ == "__main__":
    main()
