import logging
import re
from typing import Dict, List, Optional, Tuple

import pdfplumber

logger = logging.getLogger(__name__)

VALUE_RE = re.compile(r"(\(?-?\d{1,3}(?:\.\d{3})*,\d+\)?)")
MILLI_RE = re.compile(r"(reais\s*mil|em\s*milhares)", re.IGNORECASE)
DATE_RE = re.compile(r"31/12/(20\d{2})")
DIVIDEND_VALUE_RE = re.compile(r"(dividendos?|jcp|juros\s+sobre\s+capital).*?([\\d\\.]+,\\d+)", re.IGNORECASE)
DIVIDEND_DATE_RE = re.compile(r"(\\d{2}/\\d{2}/\\d{4})")

SECTION_KEYWORDS = {
    "balanco": ["balanço patrimonial", "balanco patrimonial"],
    "dre": ["demonstração do resultado", "demonstracao do resultado"],
    "dfc": ["demonstração dos fluxos de caixa", "demonstracao dos fluxos de caixa"],
    "capital": ["composição do capital", "composicao do capital", "dados da empresa"],
}
IGNORED_KEYWORDS = ["relatório da administração", "relatorio da administracao", "notas explicativas"]

FIELD_CODE_MAP = {
    "1": "ativo_total",
    "1.01": "ativo_circulante",
    "1.01.01": "caixa",
    "1.01.04": "estoques",
    "2": "passivo_total",
    "2.01": "passivo_circulante",
    "2.03": "patrimonio_liquido",
    "2.01.04": "emprestimos_cp",
    "2.02.01": "emprestimos_lp",
    "3.01": "receita_liquida",
    "3.03": "lucro_bruto",
    "3.05": "ebit",
    "3.11": "lucro_liquido",
}

FIELD_DESC_MAP = {
    "ativo_total": ["ativo total"],
    "ativo_circulante": ["ativo circulante"],
    "caixa": ["caixa e equivalentes de caixa", "caixa e equivalentes"],
    "estoques": ["estoques"],
    "passivo_total": ["passivo total"],
    "passivo_circulante": ["passivo circulante"],
    "patrimonio_liquido": ["patrimônio líquido", "patrimonio liquido"],
    "receita_liquida": ["receita de venda de bens", "receita de venda de bens e/ou serviços"],
    "lucro_bruto": ["resultado bruto", "lucro bruto"],
    "ebit": ["resultado antes do resultado financeiro e dos tributos"],
    "lucro_liquido": ["lucro/prejuízo do período", "lucro/prejuizo do periodo"],
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


def _extract_years_from_line(line: str) -> List[int]:
    return [int(match) for match in DATE_RE.findall(line)]


def _extract_values_from_line(line: str) -> List[float]:
    values: List[float] = []
    for match in VALUE_RE.findall(line):
        parsed = _parse_value(match)
        if parsed is not None:
            values.append(parsed)
    return values


def _extract_last_dividend(text: str) -> Tuple[Optional[float], Optional[str]]:
    value_match = DIVIDEND_VALUE_RE.search(text)
    date_match = DIVIDEND_DATE_RE.search(text)
    value = _parse_value(value_match.group(0)) if value_match else None
    date = None
    if date_match:
        day, month, year = date_match.group(1).split("/")
        date = f"{year}-{month}-{day}"
    return value, date


def _detect_section(text: str) -> Optional[str]:
    lowered = text.lower()
    if any(keyword in lowered for keyword in IGNORED_KEYWORDS):
        return None
    for section, keywords in SECTION_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return section
    return None


def _match_field_by_code(code: str) -> Optional[str]:
    if code in FIELD_CODE_MAP:
        return FIELD_CODE_MAP[code]
    for prefix, field in FIELD_CODE_MAP.items():
        if code.startswith(prefix + ".") and field == "lucro_liquido":
            return field
    return None


def _match_field_by_desc(description: str) -> Optional[str]:
    lowered = description.lower()
    for field, keywords in FIELD_DESC_MAP.items():
        if any(keyword in lowered for keyword in keywords):
            return field
    return None


def _extract_code_and_description(line: str) -> Tuple[Optional[str], str]:
    match = re.match(r"^\s*(\d(?:\.\d{2}){0,2})\s+(.*)$", line)
    if match:
        return match.group(1), match.group(2)
    return None, line


def _store_value(
    target: Dict[int, Dict[str, Optional[float]]],
    year: int,
    field: str,
    value: float,
) -> None:
    if year not in target:
        target[year] = {}
    if target[year].get(field) is None:
        target[year][field] = value


def parse_dfp_pdf(
    path: str,
) -> Tuple[Dict[int, Dict[str, Optional[float]]], str, Optional[float], Optional[str]]:
    logger.info("Parsing PDF %s", path)
    multiplier = 1.0
    currency_unit = "BRL"
    parsed_by_year: Dict[int, Dict[str, Optional[float]]] = {}

    with pdfplumber.open(path) as pdf:
        pages_text = [page.extract_text() or "" for page in pdf.pages]
        full_text = "\n".join(pages_text)

    if MILLI_RE.search(full_text):
        multiplier = 1000.0
        currency_unit = "BRL_THOUSANDS"

    for page_text in pages_text:
        section = _detect_section(page_text)
        if not section:
            continue
        years: List[int] = []
        for line in page_text.splitlines():
            if not years:
                years = _extract_years_from_line(line)
            if not years:
                continue
            code, description = _extract_code_and_description(line)
            field = _match_field_by_code(code) if code else None
            if field is None:
                field = _match_field_by_desc(description)
            if field is None:
                if section == "dfc" and "deprecia" in description.lower():
                    field = "depreciacao"
                else:
                    continue
            values = _extract_values_from_line(line)
            if not values:
                continue
            if len(values) >= len(years):
                values = values[-len(years) :]
            if len(values) != len(years):
                continue
            for year, value in zip(years, values):
                _store_value(parsed_by_year, year, field, value * multiplier)
                if field == "depreciacao":
                    _store_value(parsed_by_year, year, "amortizacao", 0.0)

        if section == "capital":
            for line in page_text.splitlines():
                if "total" in line.lower():
                    values = _extract_values_from_line(line)
                    if not values:
                        continue
                    year = years[0] if years else None
                    if year:
                        _store_value(parsed_by_year, year, "qtd_acoes_total", values[-1])
                        break

    dividend_value, dividend_date = _extract_last_dividend(full_text)
    if dividend_value is not None:
        dividend_value = dividend_value * multiplier

    return parsed_by_year, currency_unit, dividend_value, dividend_date


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Parse DFP PDF and print extracted fields by year")
    parser.add_argument("pdf_path", help="Caminho do PDF DFP para parsing")
    args = parser.parse_args()

    parsed, currency_unit, dividend_value, dividend_date = parse_dfp_pdf(args.pdf_path)
    output = {
        "currency_unit": currency_unit,
        "dividend_value": dividend_value,
        "dividend_date": dividend_date,
        "parsed_by_year": parsed,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
