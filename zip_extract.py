import logging
import os
import shutil
import zipfile
from typing import Dict, List

from utils import ensure_dir

logger = logging.getLogger(__name__)


def validate_zip(path: str) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    with open(path, "rb") as file_handle:
        signature = file_handle.read(4)
    return signature == b"PK\x03\x04"


def extract_zip(zip_path: str, dest_dir: str) -> Dict[str, List[str]]:
    ensure_dir(dest_dir)
    extracted_files: List[str] = []
    with zipfile.ZipFile(zip_path, "r") as zip_handle:
        for member in zip_handle.namelist():
            zip_handle.extract(member, dest_dir)
            extracted_files.append(os.path.join(dest_dir, member))
    logger.info("Extraídos %s arquivos de %s", len(extracted_files), zip_path)
    return {
        "extracted": extracted_files,
        "xlsx_paths": [
            path for path in extracted_files if os.path.basename(path).lower() == "dadosdocumento.xlsx"
        ],
        "pdf_paths": [path for path in extracted_files if path.lower().endswith(".pdf")],
    }


def copy_excels(excel_paths: List[str], excel_dir: str) -> List[str]:
    ensure_dir(excel_dir)
    copied: List[str] = []
    for path in excel_paths:
        if not os.path.exists(path):
            continue
        dest = os.path.join(excel_dir, os.path.basename(path))
        shutil.copy2(path, dest)
        copied.append(dest)
    return copied
