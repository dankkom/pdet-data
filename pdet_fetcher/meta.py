# Brazil UFs
states = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]

BASE_PATH = "/pdet/microdados"

uf_pattern = "|".join(states).lower()
month_pattern = "|".join((f"{m:02}" for m in range(1, 13)))
year_pattern = r"\d{4}"

datasets = {
    "caged": {
        "path": BASE_PATH + "/CAGED",
        "filename_pattern": r"^cagedest_({month})({year})\.7z$".format(month=month_pattern, year=year_pattern),
    },
    "caged-ajustes": {
        "path": BASE_PATH + "/CAGED_AJUSTES",
        "filename_pattern": r"^cagedest_ajustes_({month})({year})\.7z$".format(month=month_pattern, year=year_pattern),
    },
    "caged-ajustes-2002a2009": {
        "path": BASE_PATH + "/CAGED_AJUSTES/2002a2009",
        "filename_pattern": r"^cagedest_ajustes_({year})\.7z$".format(year=year_pattern),
    },
    "novo-caged": {
        "path": BASE_PATH + "/NOVO_CAGED",
        "filename_pattern": r"^caged(exc|for|mov)({year})({month})\.7z$".format(year=year_pattern, month=month_pattern),
    },
    "rais-1985-2017": {
        "path": BASE_PATH + "/RAIS",
        "filename_pattern": r"^({uf_pattern})({year})\.7z$".format(uf_pattern=uf_pattern, year=year_pattern),
    },
    "rais-1985-2017-estabelecimentos": {
        "path": BASE_PATH + "/RAIS",
        "filename_pattern": r"^estb({year})\.(7z|zip)$".format(year=year_pattern),
    },
    "rais-1985-2017-ignorados": {
        "path": BASE_PATH + "/RAIS",
        "filename_pattern": r"^ignora(|n)do(|s)({year})\.7z$".format(year=year_pattern),
    },
    "rais": {
        "path": BASE_PATH + "/RAIS",
        "filename_pattern": r"^rais_vinc_pub_(centro_oeste|mg_es_rj|nordeste|norte|sp|sul)\.7z$",
    },
    "rais-estabelecimentos": {
        "path": BASE_PATH + "/RAIS",
        "filename_pattern": r"^rais_estab_pub\.7z$",
    },
}
