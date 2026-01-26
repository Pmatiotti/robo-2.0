import logging
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

PERCENT_HINTS = ("m_", "margin", "roe", "roa", "payout", "ratio")
PERCENT_EXACT_KEYS = {"dividend_yield", "margem_liquida", "margem_ebitda", "margem_bruta"}
PERCENT_SUFFIXES = ("_margin", "_ratio")


def is_percent_indicator(key: str) -> bool:
    if key in PERCENT_EXACT_KEYS:
        return True
    if key.startswith(PERCENT_HINTS) or any(hint in key for hint in PERCENT_HINTS):
        return True
    return key.endswith(PERCENT_SUFFIXES)


def normalize_ratio(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    magnitude = abs(value)
    if magnitude <= 1:
        return value
    if magnitude <= 1000:
        return value / 100
    return None


def normalize_indicators(
    indicators: Dict[str, Optional[float]],
    is_financial: bool,
    ticker: str,
    year: int,
    percent_keys: Optional[Iterable[str]] = None,
) -> Tuple[Dict[str, Optional[float]], int, int, List[str], List[str]]:
    normalized = dict(indicators)
    conversions = 0
    anomalies = 0
    converted_keys: List[str] = []
    anomaly_keys: List[str] = []
    percent_keys_set = set(percent_keys) if percent_keys else set()

    for key, value in indicators.items():
        if value is None:
            continue
        should_normalize = key in percent_keys_set or is_percent_indicator(key)
        if should_normalize:
            updated = normalize_ratio(value)
            if updated is None:
                logger.warning("Anomalia %s %s %s: %s", ticker, year, key, value)
                anomalies += 1
                anomaly_keys.append(key)
                normalized[key] = None
                continue
            if updated != value:
                logger.info("Normalizado %s %s %s: %s => %s", ticker, year, key, value, updated)
                conversions += 1
                converted_keys.append(key)
            normalized[key] = updated

    if is_financial:
        for key in list(normalized.keys()):
            if "ebitda" in key:
                normalized[key] = None

    return normalized, conversions, anomalies, converted_keys, anomaly_keys
