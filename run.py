import argparse
from pathlib import Path

from pdet_fetcher import fetch


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-data-dir", required=True, type=Path)
    args = parser.parse_args()
    dest_dir = args.data_dir

    ftp = fetch.connect()
    fetch.fetch_rais(ftp=ftp, dest_dir=dest_dir)
    fetch.fetch_caged(ftp=ftp, dest_dir=dest_dir)
    ftp.close()


if __name__ == "__main__":
    main()
