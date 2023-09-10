import datetime as dt
import ftplib
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Generator, Sequence

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


def _get_date_dirs(fi: list[dict], dir_pattern: str, dir_pattern_groups: Sequence[str]) -> list[dict]:
    """Filters list of directories in FTP server that groups files by date."""
    date_dirs = []
    for f in fi:
        if f["size"] is not None:
            continue
        m = re.match(dir_pattern, f["name"])
        if m:
            group_meta = {"dir": f["name"]}
            for i, group in enumerate(dir_pattern_groups):
                text = m.groups()[i]
                group_meta.update({group: text})
            date_dirs.append(group_meta)
    return date_dirs


def _get_group_meta(m: re.Match, variation: dict) -> dict:
    """Return a dictionary with info in a file name given by variation's
    fn_pattern.
    """
    group_meta = {}
    for group in variation["fn_pattern_groups"]:
        if not group:
            continue
        index = variation["fn_pattern_groups"].index(group)
        text = m.groups()[index].replace("_", "")
        group_meta.update({group: text})
    return group_meta


def _list_variation_files(ftp: ftplib.FTP, variation: dict) -> list[dict]:
    ftp_path = variation["path"]
    if variation["dir_pattern"]:
        date_dirs = _get_date_dirs(
            fi=list_files(ftp, directory=ftp_path),
            dir_pattern=variation["dir_pattern"],
            dir_pattern_groups=variation["dir_pattern_groups"],
        )
        for date_dir_meta in date_dirs:
            date_dir = date_dir_meta["dir"]
            files = list_files(ftp, directory=f"{ftp_path}/{date_dir}")
            yield from (f | date_dir_meta for f in files)
    else:
        files = list_files(ftp, directory=ftp_path)
        yield from (f | {"year": None} for f in files)


def _get_variation_files_metadata(ftp: ftplib.FTP, variation: dict) -> Generator[dict, None, None]:
    for file in _list_variation_files(ftp=ftp, variation=variation):
        m = re.match(
            variation["fn_pattern"],
            file["name"].lower(),
        )
        if m:
            group_meta = _get_group_meta(m, variation=variation)
            yield file | group_meta


def _list_dataset_files(ftp: ftplib.FTP, dataset: str) -> Generator[dict, None, None]:
    for variation in datasets[dataset]["variations"]:
        for f in _get_variation_files_metadata(ftp=ftp, variation=variation):
            yield f | {"dataset": dataset}


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def list_caged(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for dataset in ("caged", "caged-ajustes"):
        yield from _list_dataset_files(ftp=ftp, dataset=dataset)


def fetch_caged(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    metadata_list = []
    for file in list_caged(ftp):
        ftp_filepath = file["full_path"]
        dest_filepath = get_caged_filepath(file, dest_dir)
        if dest_filepath.exists():
            continue
        file_size = file["size"]
        fetch_file(ftp, ftp_filepath, dest_filepath, file_size=file_size)
        metadata = file | {"filepath": dest_filepath}
        metadata_list.append(metadata)
    return metadata_list


def list_caged_2020(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    dataset = "caged-2020"
    for variation in datasets[dataset]["variations"]:
        ftp_path = variation["path"]
        date_dirs1 = _get_date_dirs(
            fi=list_files(ftp, directory=ftp_path),
            dir_pattern=variation["dir_pattern"][0],
            dir_pattern_groups=variation["dir_pattern_groups"][0],
        )
        for date_dir1_meta in date_dirs1:
            date_dir1 = date_dir1_meta["dir"]
            date_dirs2 = _get_date_dirs(
                fi=list_files(ftp, directory=f"{ftp_path}/{date_dir1}"),
                dir_pattern=variation["dir_pattern"][1],
                dir_pattern_groups=variation["dir_pattern_groups"][1],
            )
            for date_dir2_meta in date_dirs2:
                date_dir2 = date_dir2_meta["dir"]
                files = list_files(ftp, directory=f"{ftp_path}/{date_dir1}/{date_dir2}")
                for file in files:
                    m = re.match(
                        variation["fn_pattern"],
                        file["name"].lower(),
                    )
                    if m:
                        group_meta = _get_group_meta(m, variation=variation)
                        yield file | group_meta | date_dir2_meta | {"dataset": dataset}


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def list_rais(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for dataset in ("rais-estabelecimentos", "rais-vinculos"):
        yield from _list_dataset_files(ftp=ftp, dataset=dataset)


def fetch_rais(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    metadata_list = []
    for file in list_rais(ftp):
        ftp_filepath = file["full_path"]
        dest_filepath = get_rais_filepath(file, dest_dir)
        if dest_filepath.exists():
            continue
        file_size = file["size"]
        fetch_file(ftp, ftp_filepath, dest_filepath, file_size=file_size)
        metadata = file | {"filepath": dest_filepath}
        metadata_list.append(metadata)
    return metadata_list
