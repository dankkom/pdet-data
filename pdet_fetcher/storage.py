from pathlib import Path


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def harmonize_caged_dataset_name(original_dataset_name: str) -> str:
    match original_dataset_name:
        case "caged":
            dataset = "caged"
        case "caged-ajustes-2002a2009" | "caged-ajustes":
            dataset = "caged-ajustes"
        case _:
            dataset = original_dataset_name
    return dataset


def get_caged_filename(file_metadata: dict) -> str:
    # dataset part
    dataset = harmonize_caged_dataset_name(file_metadata["dataset"])

    # partition part
    year = file_metadata["year"]
    partition = f"{year:04}"
    if month := file_metadata.get("month"):
        partition = partition + f"{month:02}"

    # modified part
    modified = file_metadata["datetime"]

    # extension part
    extension = file_metadata["extension"]

    return f"{dataset}_{partition}_{modified:%Y%m%d%H%M}.{extension}"


def get_caged_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    dataset = harmonize_caged_dataset_name(file_metadata["dataset"])
    year = str(file_metadata["year"])
    return dest_dir / dataset / year / get_caged_filename(file_metadata)


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def harmonize_rais_dataset_name(original_dataset_name: str) -> str:
    match original_dataset_name:
        case "rais-1985-2017" | "rais":
            dataset = "rais"
        case "rais-1985-2017-estabelecimentos" | "rais-estabelecimentos":
            dataset = "rais-estabelecimentos"
        case "rais-1985-2017-ignorados":
            dataset = "rais-ignorados"
        case _:
            dataset = original_dataset_name
    return dataset


def get_rais_filename(file_metadata: dict) -> str:
    # dataset part
    dataset = harmonize_rais_dataset_name(file_metadata["dataset"])

    # partition part
    year = file_metadata["year"]
    partition = f"{year}"
    if region := file_metadata.get("uf", file_metadata.get("region")):
        partition = partition + f"-{region}"

    # modified part
    modified = file_metadata["datetime"]

    # extension part
    extension = file_metadata["extension"]

    return f"{dataset}_{partition}_{modified:%Y%m%d%H%M}.{extension}"


def get_rais_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    dataset = harmonize_rais_dataset_name(file_metadata["dataset"])
    year = str(file_metadata["year"])
    return dest_dir / dataset / year / get_rais_filename(file_metadata)
