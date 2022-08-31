import datetime as dt
import ftplib
import re
from functools import lru_cache

from .meta import datasets

FTP_HOST = "ftp.mtps.gov.br"


def connect():
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
            file = {
                "datetime": datetime,
                "size": size,
                "name": name,
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


def get_years(fi):
    years = []
    for f in fi:
        if f["size"] is not None:
            continue
        m = re.match(r"^(\d{4})$", f["name"])
        if m:
            year = int(m.group(1))
            years.append(year)
    return years


def list_caged_files(ftp):
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
                yield file | {"year": int(year), "month": int(month)}


def list_caged_ajustes_files(ftp):
    dataset = "caged-ajustes-2002a2009"
    files = list_files(ftp, directory=datasets[dataset]["path"])
    for file in files:
        m = re.match(
            datasets[dataset]["filename_pattern"],
            file["name"].lower(),
        )
        if m:
            year, = m.groups()
            yield file | {"year": int(year)}

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
                yield file | {"year": int(year), "month": int(month)}


def list_caged(ftp):
    yield from list_caged_files(ftp)
    yield from list_caged_ajustes_files(ftp)


def list_rais_files(ftp):
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
                yield file | {"year": int(year), "uf": uf}
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
                yield file | {"year": year, "region": region}


def list_rais_estabelecimentos_files(ftp):
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
                yield file | {"year": int(year)}
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
                yield file | {"year": year}


def list_rais_ignorados_files(ftp):
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
                yield file | {"year": year}


def list_rais(ftp):
    yield from list_rais_files(ftp)
    yield from list_rais_estabelecimentos_files(ftp)
    yield from list_rais_ignorados_files(ftp)
