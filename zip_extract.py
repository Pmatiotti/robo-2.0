import logging
import os
import zipfile
from typing import List

from utils import ensure_dir

logger = logging.getLogger(__name__)


def validate_zip(path: str) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    with open(path, "rb") as file_handle:
        signature = file_handle.read(4)
    return signature == b"PK\x03\x04"


def extract_zip(zip_path: str, dest_dir: str) -> List[str]:
    ensure_dir(dest_dir)
    extracted_files: List[str] = []
    with zipfile.ZipFile(zip_path, "r") as zip_handle:
        for member in zip_handle.namelist():
            zip_handle.extract(member, dest_dir)
            extracted_files.append(os.path.join(dest_dir, member))
    logger.info("Extraídos %s arquivos de %s", len(extracted_files), zip_path)
    return extracted_files
