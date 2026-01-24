import hashlib
import logging
import os
import re
from datetime import datetime
from typing import Dict, Optional


CNPJ_DIGITS_RE = re.compile(r"\D+")


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def normalize_cnpj(value: str) -> str:
    return CNPJ_DIGITS_RE.sub("", value or "")


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_valid_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%d/%m/%Y")
        return True
    except ValueError:
        return False


def parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def parse_decimal(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def calculate_cagr(series: Dict[int, float], years: int = 5) -> Optional[float]:
    if not series or len(series) < 2:
        return None

    sorted_years = sorted(series.keys())
    start_year = sorted_years[0]
    end_year = sorted_years[-1]

    if end_year - start_year < years - 1:
        return None

    start_value = series[start_year]
    end_value = series[end_year]

    if start_value <= 0 or end_value <= 0:
        return None

    actual_years = end_year - start_year
    cagr = (end_value / start_value) ** (1 / actual_years) - 1
    return round(cagr, 4)
