from pathlib import Path


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def get_caged_filename(file_metadata: dict) -> str:
    # dataset part
    dataset = file_metadata["dataset"]

    # partition part
    year = file_metadata["year"]
    partition = f"{year:04}"
    if month := file_metadata.get("month"):
        partition = partition + f"{month:02}"

    # modified part
    modified = file_metadata["datetime"]

    # extension part
    extension = file_metadata["extension"]

    return f"{dataset}_{partition}_{modified:%Y%m%d}.{extension}"


def get_caged_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    dataset = file_metadata["dataset"]
    year = str(file_metadata["year"])
    return dest_dir / dataset / year / get_caged_filename(file_metadata)


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def get_rais_filename(file_metadata: dict) -> str:
    # dataset part
    dataset = file_metadata["dataset"]

    # partition part
    year = file_metadata["year"]
    partition = f"{year}"
    if region := file_metadata.get("uf", file_metadata.get("region")):
        partition = partition + f"-{region}"

    # modified part
    modified = file_metadata["datetime"]

    # extension part
    extension = file_metadata["extension"]

    return f"{dataset}_{partition}_{modified:%Y%m%d}.{extension}"


def get_rais_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    dataset = file_metadata["dataset"]
    year = str(file_metadata["year"])
    return dest_dir / dataset / year / get_rais_filename(file_metadata)
