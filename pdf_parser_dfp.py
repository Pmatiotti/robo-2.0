import logging
import re
from typing import Dict, Optional, Tuple

import pdfplumber

logger = logging.getLogger(__name__)

VALUE_RE = re.compile(r"(-?\(?[\d\.]+,\d+\)?)")
MILLI_RE = re.compile(r"(reais\s*mil|em\s*milhares)", re.IGNORECASE)
YEAR_RE = re.compile(r"\b(20\d{2})\b")
DIVIDEND_VALUE_RE = re.compile(r"(dividendos?|jcp|juros\s+sobre\s+capital).*?([\\d\\.]+,\\d+)", re.IGNORECASE)
DIVIDEND_DATE_RE = re.compile(r"(\\d{2}/\\d{2}/\\d{4})")

FIELD_PATTERNS = {
    "ativo_total": [r"Ativo\s+Total"],
    "passivo_total": [r"Passivo\s+Total"],
    "patrimonio_liquido": [r"Patrim[oô]nio\s+L[ií]quido"],
    "ativo_circulante": [r"Ativo\s+Circulante"],
    "passivo_circulante": [r"Passivo\s+Circulante"],
    "estoques": [r"Estoques"],
    "caixa": [r"Caixa\s+e\s+equivalentes"],
    "receita_liquida": [r"Receita\s+L[ií]quida"],
    "lucro_bruto": [r"Lucro\s+Bruto"],
    "ebit": [r"EBIT(?!DA)", r"Resultado\s+Operacional"],
    "depreciacao": [r"Deprecia[cç][aã]o"],
    "amortizacao": [r"Amortiza[cç][aã]o"],
    "lucro_liquido": [r"Lucro\s+L[ií]quido"],
    "lucro_por_acao": [r"Lucro\s+por\s+a[cç][aã]o"],
    "qtd_acoes_total": [r"Quantidade\s+de\s+a[cç][oõ]es"],
    "emprestimos_cp": [r"Empr[eé]stimos\s+e\s+financiamentos\s+CP"],
    "emprestimos_lp": [r"Empr[eé]stimos\s+e\s+financiamentos\s+LP"],
    "dividendos": [r"Dividendos"],
}


def _parse_value(text: str) -> Optional[float]:
    match = VALUE_RE.search(text)
    if not match:
        return None
    raw = match.group(1)
    negative = raw.startswith("(") and raw.endswith(")")
    cleaned = raw.strip("()")
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return -value if negative else value


def _find_value(field: str, text: str) -> Optional[float]:
    patterns = FIELD_PATTERNS.get(field, [])
    for pattern in patterns:
        regex = re.compile(rf"{pattern}.*?{VALUE_RE.pattern}", re.IGNORECASE)
        match = regex.search(text)
        if match:
            return _parse_value(match.group(0))
    return None


def _extract_year(text: str) -> Optional[int]:
    years = [int(match) for match in YEAR_RE.findall(text)]
    if not years:
        return None
    return max(years)


def _extract_last_dividend(text: str) -> Tuple[Optional[float], Optional[str]]:
    value_match = DIVIDEND_VALUE_RE.search(text)
    date_match = DIVIDEND_DATE_RE.search(text)
    value = _parse_value(value_match.group(0)) if value_match else None
    date = None
    if date_match:
        day, month, year = date_match.group(1).split("/")
        date = f"{year}-{month}-{day}"
    return value, date


def parse_dfp_pdf(path: str) -> Tuple[Dict[str, Optional[float]], Optional[int], str, Optional[float], Optional[str]]:
    logger.info("Parsing PDF %s", path)
    data: Dict[str, Optional[float]] = {key: None for key in FIELD_PATTERNS}
    multiplier = 1.0
    currency_unit = "BRL"

    with pdfplumber.open(path) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    if MILLI_RE.search(full_text):
        multiplier = 1000.0
        currency_unit = "BRL_THOUSANDS"

    for field in data:
        value = _find_value(field, full_text)
        if value is not None:
            data[field] = value * multiplier

    year = _extract_year(full_text)
    dividend_value, dividend_date = _extract_last_dividend(full_text)
    if dividend_value is not None:
        dividend_value = dividend_value * multiplier

    return data, year, currency_unit, dividend_value, dividend_date
