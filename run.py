import argparse
from pathlib import Path

from pdet_fetcher import fetcher


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", required=True, type=Path)
    args = parser.parse_args()

    dest_dir = args.output
    ftp = fetcher.connect()
    fetcher.fetch_rais(ftp=ftp, dest_dir=dest_dir / "rais")
    fetcher.fetch_caged(ftp=ftp, dest_dir=dest_dir / "caged")
    ftp.close()


if __name__ == "__main__":
    main()
