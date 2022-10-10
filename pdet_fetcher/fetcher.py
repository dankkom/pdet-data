import datetime as dt
import ftplib
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Generator

from tqdm import tqdm

from .meta import datasets
from .storage import get_caged_filepath, get_rais_filepath

FTP_HOST = "ftp.mtps.gov.br"
logger = logging.getLogger(__name__)


def connect() -> ftplib.FTP:
    ftp = ftplib.FTP(FTP_HOST, encoding="latin-1")
    ftp.login()
    return ftp


@lru_cache
def list_files(ftp: ftplib.FTP, directory: str) -> list[dict]:
    """List all files in the current directory."""
    ftp_lines = []
    ftp.cwd(directory)
    ftp.retrlines("LIST", ftp_lines.append)
    # parse files' date, size and name
    def parse_line(line):
        m = re.match(
            r"^(\d{2}-\d{2}-\d{2}) +(\d{2}:\d{2})(AM|PM) +(<DIR>|\d+) +(.*)$",
            line,
        )
        if m:
            date, time, am_pm, size, name = m.groups()
            # parse datetime
            datetime = dt.datetime.strptime(
                f"{date} {time}{am_pm}",
                "%m-%d-%y %I:%M%p",
            )
            # parse size
            if size == "<DIR>":
                size = None
            else:
                size = int(size)
            # parse name
            name = name.strip()
            try:
                extension = name.rsplit(".", maxsplit=1)[1]
            except IndexError:
                extension = None
            file = {
                "datetime": datetime,
                "size": size,
                "name": name,
                "extension": extension,
                "full_path": f"{directory}/{name}",
            }
            return file
        else:
            return None
    files = []
    for f in ftp_lines:
        file = parse_line(f)
        if file:
            files.append(file)
    return files


def fetch_file(ftp: ftplib.FTP, ftp_filepath: str, dest_filepath: Path, **kwargs):
    """Download a file from FTP."""
    dest_filepath.parent.mkdir(parents=True, exist_ok=True)

    if "file_size" in kwargs:
        file_size = kwargs["file_size"]
    else:
        file_size = ftp.size(ftp_filepath)

    logger.info(f"Fetching {ftp_filepath} --> {dest_filepath}")

    progress = tqdm(
        desc=dest_filepath.name,
        total=file_size,
        unit="B",
        unit_scale=True,
    )

    with open(dest_filepath, "wb") as f:
        def write(data):
            nonlocal f, progress
            f.write(data)
            progress.update(len(data))
        ftp.retrbinary(f"RETR {ftp_filepath}", write)
    progress.close()


def get_years(fi: list[dict]) -> list[int]:
    years = []
    for f in fi:
        if f["size"] is not None:
            continue
        m = re.match(r"^(\d{4})$", f["name"])
        if m:
            year = int(m.group(1))
            years.append(year)
    return years


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def list_caged_files(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    dataset = "caged"
    years = get_years(list_files(ftp, directory=datasets[dataset]["path"]))
    for year in years:
        files = list_files(ftp, directory=f"{datasets[dataset]['path']}/{year}")
        for file in files:
            m = re.match(
                datasets[dataset]["filename_pattern"],
                file["name"].lower(),
            )
            if m:
                month, year = m.groups()
                yield file | {
                    "year": int(year),
                    "month": int(month),
                    "dataset": dataset,
                }


def list_caged_ajustes_files(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    dataset = "caged-ajustes-2002a2009"
    files = list_files(ftp, directory=datasets[dataset]["path"])
    for file in files:
        m = re.match(
            datasets[dataset]["filename_pattern"],
            file["name"].lower(),
        )
        if m:
            year, = m.groups()
            yield file | {"year": int(year), "dataset": dataset}

    dataset = "caged-ajustes"
    years = get_years(list_files(ftp, directory=datasets[dataset]["path"]))
    for year in years:
        files = list_files(ftp, directory=f"{datasets[dataset]['path']}/{year}")
        for file in files:
            m = re.match(
                datasets[dataset]["filename_pattern"],
                file["name"].lower(),
            )
            if m:
                month, year = m.groups()
                yield file | {
                    "year": int(year),
                    "month": int(month),
                    "dataset": dataset,
                }


def list_caged(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    yield from list_caged_files(ftp)
    yield from list_caged_ajustes_files(ftp)



def fetch_caged(ftp: ftplib.FTP, dest_dir: Path) -> Generator[dict, None, None]:
    for file in list_caged(ftp):
        ftp_filepath = file["full_path"]
        dest_filepath = get_caged_filepath(file, dest_dir)
        if dest_filepath.exists():
            continue
        file_size = file["size"]
        fetch_file(ftp, ftp_filepath, dest_filepath, file_size=file_size)
        yield file | {"filepath": dest_filepath}


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def list_rais_files(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    dataset = "rais-1985-2017"
    years = get_years(list_files(ftp, directory=datasets[dataset]["path"]))
    for year in years:
        files = list_files(ftp, directory=f"{datasets[dataset]['path']}/{year}")
        for file in files:
            m = re.match(
                datasets[dataset]["filename_pattern"],
                file["name"].lower(),
            )
            if m:
                uf, year = m.groups()
                yield file | {
                    "year": int(year),
                    "uf": uf,
                    "dataset": dataset,
                }
    dataset = "rais"
    years = get_years(list_files(ftp, directory=datasets[dataset]["path"]))
    for year in years:
        files = list_files(ftp, directory=f"{datasets[dataset]['path']}/{year}")
        for file in files:
            m = re.match(
                datasets[dataset]["filename_pattern"],
                file["name"].lower(),
            )
            if m:
                region, = m.groups()
                region = region.replace("_", "")
                yield file | {
                    "year": year,
                    "region": region,
                    "dataset": dataset,
                }


def list_rais_estabelecimentos_files(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    dataset = "rais-1985-2017-estabelecimentos"
    years = get_years(list_files(ftp, directory=datasets[dataset]["path"]))
    for year in years:
        files = list_files(ftp, directory=f"{datasets[dataset]['path']}/{year}")
        for file in files:
            m = re.match(
                datasets[dataset]["filename_pattern"],
                file["name"].lower(),
            )
            if m:
                year, _ = m.groups()
                yield file | {"year": int(year), "dataset": dataset}
    dataset = "rais-estabelecimentos"
    years = get_years(list_files(ftp, directory=datasets[dataset]["path"]))
    for year in years:
        files = list_files(ftp, directory=f"{datasets[dataset]['path']}/{year}")
        for file in files:
            m = re.match(
                datasets[dataset]["filename_pattern"],
                file["name"].lower(),
            )
            if m:
                yield file | {"year": year, "dataset": dataset}


def list_rais_ignorados_files(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    dataset = "rais-1985-2017-ignorados"
    years = get_years(list_files(ftp, directory=datasets[dataset]["path"]))
    for year in years:
        files = list_files(ftp, directory=f"{datasets[dataset]['path']}/{year}")
        for file in files:
            m = re.match(
                datasets[dataset]["filename_pattern"],
                file["name"].lower(),
            )
            if m:
                yield file | {"year": year, "dataset": dataset}


def list_rais(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    yield from list_rais_files(ftp)
    yield from list_rais_estabelecimentos_files(ftp)
    yield from list_rais_ignorados_files(ftp)


def fetch_rais(ftp: ftplib.FTP, dest_dir: Path) -> Generator[dict, None, None]:
    for file in list_rais(ftp):
        ftp_filepath = file["full_path"]
        dest_filepath = get_rais_filepath(file, dest_dir)
        if dest_filepath.exists():
            continue
        file_size = file["size"]
        fetch_file(ftp, ftp_filepath, dest_filepath, file_size=file_size)
        yield file | {"filepath": dest_filepath}
