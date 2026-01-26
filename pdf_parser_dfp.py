import json
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

import pdfplumber

logger = logging.getLogger(__name__)

VALUE_RE = re.compile(r"\(?-?\d{1,3}(?:\.\d{3})*(?:,\d+)?\)?")
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
CONSOLIDATED_KEYWORDS = ["consolidado", "consolidadas", "dfs consolidadas"]
INDIVIDUAL_KEYWORDS = ["individual", "individuais", "dfs individuais"]

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
    match = VALUE_RE.search(text.replace(" ", ""))
    if not match:
        return None
    raw = match.group(0)
    negative = raw.startswith("(") and raw.endswith(")")
    cleaned = raw.strip("()")
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        cleaned = cleaned.replace(".", "")
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return -value if negative else value


def _extract_years_from_line(line: str) -> List[int]:
    return [int(match) for match in DATE_RE.findall(line)]


def _extract_values_from_line(line: str) -> List[float]:
    values: List[float] = []
    for match in VALUE_RE.findall(line.replace(" ", "")):
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


def _detect_scope(text: str) -> Optional[str]:
    lowered = text.lower()
    if any(keyword in lowered for keyword in CONSOLIDATED_KEYWORDS):
        return "consolidado"
    if any(keyword in lowered for keyword in INDIVIDUAL_KEYWORDS):
        return "individual"
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
    debug_context: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[int, Dict[str, Optional[float]]], str, Optional[float], Optional[str]]:
    logger.info("Parsing PDF %s", path)
    multiplier = 1.0
    currency_unit = "BRL"
    parsed_by_year: Dict[int, Dict[str, Optional[float]]] = {}
    lines_used: List[str] = []

    with pdfplumber.open(path) as pdf:
        pages = list(pdf.pages)
        pages_text = [page.extract_text() or "" for page in pages]
        full_text = "\n".join(pages_text)

    if MILLI_RE.search(full_text):
        multiplier = 1000.0
        currency_unit = "BRL_THOUSANDS"

    page_scopes = [_detect_scope(text) for text in pages_text]
    has_consolidated = any(scope == "consolidado" for scope in page_scopes)
    has_individual = any(scope == "individual" for scope in page_scopes)
    if has_consolidated:
        selected_scopes = {"consolidado"}
    elif has_individual:
        selected_scopes = {"individual"}
    else:
        selected_scopes = {None}

    for page_text, scope, page in zip(pages_text, page_scopes, pages):
        if scope not in selected_scopes:
            continue
        section = _detect_section(page_text)
        if not section:
            continue
        years: List[int] = []
        found_depreciacao = False
        found_amortizacao = False
        for line in page_text.splitlines():
            if not years:
                years = _extract_years_from_line(line)
            if not years:
                continue
            code, description = _extract_code_and_description(line)
            field = _match_field_by_code(code) if code else None
            if field is None:
                field = _match_field_by_desc(description)
            if field is None and section == "dfc":
                lowered = description.lower()
                if "deprecia" in lowered or "amortiza" in lowered:
                    if "deprecia" in lowered and "amortiza" in lowered:
                        field = "depreciacao"
                    elif "deprecia" in lowered:
                        field = "depreciacao"
                    elif "amortiza" in lowered:
                        field = "amortizacao"
                else:
                    continue
            if field is None:
                continue
            values = _extract_values_from_line(line)
            if not values:
                logger.debug("Linha relevante sem valores: %s", line)
                continue
            if len(values) >= len(years):
                values = values[-len(years) :]
            if len(values) != len(years):
                logger.debug("Linha com valores não alinhados aos anos: %s", line)
                continue
            lines_used.append(line.strip())
            for year, value in zip(years, values):
                _store_value(parsed_by_year, year, field, value * multiplier)
                if field == "depreciacao":
                    found_depreciacao = True
                    if "amortiza" in description.lower():
                        _store_value(parsed_by_year, year, "amortizacao", 0.0)
                        logger.info("D&A agregado detectado no PDF (%s): amortizacao=0.0", year)
                if field == "amortizacao":
                    found_amortizacao = True

        if section == "capital":
            found_emitidas = False
            found_tesouraria = False
            in_integralizado = False
            in_tesouraria = False
            for line in page_text.splitlines():
                lowered = line.lower()
                if "capital integralizado" in lowered:
                    in_integralizado = True
                    in_tesouraria = False
                    continue
                if "tesouraria" in lowered:
                    in_tesouraria = True
                    in_integralizado = False
                    continue
                if "total" in lowered and (in_integralizado or in_tesouraria):
                    values = _extract_values_from_line(line)
                    if not values:
                        continue
                    year = years[0] if years else None
                    if year:
                        multiplier_shares = 1000.0 if "mil" in lowered else 1.0
                        if in_integralizado:
                            _store_value(parsed_by_year, year, "qtd_acoes_emitidas", values[-1] * multiplier_shares)
                            found_emitidas = True
                        if in_tesouraria:
                            _store_value(parsed_by_year, year, "qtd_acoes_tesouraria", values[-1] * multiplier_shares)
                            found_tesouraria = True
                        lines_used.append(line.strip())
            if not found_emitidas:
                logger.info("qtd_acoes_emitidas ausente no PDF")
            if not found_tesouraria:
                logger.info("qtd_acoes_tesouraria ausente no PDF")

        if section == "dfc":
            if not found_depreciacao:
                logger.info("depreciacao ausente no PDF")
            if not found_amortizacao:
                logger.info("amortizacao ausente no PDF")

    dividend_value, dividend_date = _extract_last_dividend(full_text)
    if dividend_value is not None:
        dividend_value = dividend_value * multiplier

    detected_years = sorted(parsed_by_year.keys())
    logger.info(
        "Escopo detectado: %s",
        "consolidado" if has_consolidated else "individual" if has_individual else "indefinido",
    )
    logger.info("Anos detectados no PDF: %s", detected_years)
    for year in detected_years:
        filled_fields = [field for field, value in parsed_by_year.get(year, {}).items() if value is not None]
        logger.info("Campos preenchidos %s: %s", year, ", ".join(sorted(filled_fields)))

    if debug_context and os.getenv("PARSE_DEBUG") == "1":
        output_root = debug_context.get("output_root")
        ticker = debug_context.get("ticker", "unknown")
        codigo_cvm = debug_context.get("codigo_cvm", "unknown")
        if output_root:
            debug_path = os.path.join(output_root, f"parse_debug_{ticker}_{codigo_cvm}.json")
            with open(debug_path, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "detected_scope": "consolidado" if has_consolidated else "individual" if has_individual else "indefinido",
                        "detected_years": detected_years,
                        "raw_by_year": parsed_by_year,
                        "lines_used": lines_used[:100],
                    },
                    handle,
                    ensure_ascii=False,
                    indent=2,
                )

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
