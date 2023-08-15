import argparse
from pathlib import Path

from pdet_fetcher import fetcher


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-data-dir", required=True, type=Path)
    args = parser.parse_args()
    dest_dir = args.data_dir

    ftp = fetcher.connect()
    fetcher.fetch_rais(ftp=ftp, dest_dir=dest_dir)
    fetcher.fetch_caged(ftp=ftp, dest_dir=dest_dir)
    ftp.close()


if __name__ == "__main__":
    main()
