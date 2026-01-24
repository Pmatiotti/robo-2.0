import logging
import re
from typing import Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

CODE_MAP = {
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

DESC_MAP = {
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


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df


def _get_column(df: pd.DataFrame, *names: str) -> Optional[str]:
    for name in names:
        if name in df.columns:
            return name
    return None


def _parse_precision(value: Optional[str]) -> int:
    if not value:
        return 1
    return 1000 if re.search(r"mil", str(value), re.IGNORECASE) else 1


def _parse_number(value: Optional[float]) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if text in {"", "-", "nan", "NaN"}:
        return None
    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("()").replace(" ", "")
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(".", "")
    text = re.sub(r"[^0-9\.\-]", "", text)
    if text in {"", "-"}:
        return None
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    return -parsed if negative else parsed


def _match_field(code: Optional[str], description: Optional[str]) -> Optional[str]:
    if code:
        code = str(code).strip()
        if code in CODE_MAP:
            return CODE_MAP[code]
    if description:
        lowered = str(description).strip().lower()
        for field, keywords in DESC_MAP.items():
            if any(keyword in lowered for keyword in keywords):
                return field
    return None


def parse_xlsx(
    path_xlsx: str,
    reference_year: int,
    prefer_consolidated: bool = True,
) -> Tuple[Dict[int, Dict[str, Optional[float]]], str]:
    sheets = pd.read_excel(path_xlsx, sheet_name=None)
    currency_unit = "BRL"
    raw_by_year: Dict[int, Dict[str, Optional[float]]] = {}

    def select_sheet(prefix: str) -> Optional[str]:
        options = []
        for name in sheets:
            if name.lower().startswith(prefix.lower()):
                options.append(name)
        if not options:
            return None
        return options[0]

    if prefer_consolidated:
        sheet_prefixes = ["df cons", "df ind"]
    else:
        sheet_prefixes = ["df ind", "df cons"]

    sheet_names = []
    for prefix in sheet_prefixes:
        for name in sheets:
            if name.lower().startswith(prefix):
                sheet_names.append(name)
        if sheet_names:
            break

    used_sheets = []
    for sheet_name in sheet_names:
        df = _normalize_columns(sheets[sheet_name])
        code_col = _get_column(df, "código conta", "codigo conta")
        desc_col = _get_column(df, "descrição conta", "descricao conta")
        last_col = _get_column(df, "valor ultimo exercicio", "valor último exercicio")
        prev_col = _get_column(df, "valor penultimo exercicio", "valor penúltimo exercicio")
        prev2_col = _get_column(df, "valor antepenultimo exercicio", "valor antepenúltimo exercicio")
        precision_col = _get_column(df, "precisao", "precisão")
        if not (last_col and prev_col and prev2_col):
            continue
        used_sheets.append(sheet_name)

        sample_matches = []
        for _, row in df.iterrows():
            field = _match_field(row.get(code_col), row.get(desc_col))
            if not field:
                continue
            multiplier = _parse_precision(row.get(precision_col))
            if multiplier == 1000:
                currency_unit = "BRL_THOUSANDS"

            values = {
                reference_year: _parse_number(row.get(last_col)),
                reference_year - 1: _parse_number(row.get(prev_col)),
                reference_year - 2: _parse_number(row.get(prev2_col)),
            }
            for year, value in values.items():
                if value is None:
                    continue
                raw_by_year.setdefault(year, {})
                if raw_by_year[year].get(field) is None:
                    raw_by_year[year][field] = value * multiplier
                    if field in {"ativo_total", "passivo_total", "patrimonio_liquido", "receita_liquida", "lucro_liquido"}:
                        sample_matches.append((field, year, raw_by_year[year][field]))

        if sample_matches:
            logger.info("Sheet %s: amostras %s", sheet_name, sample_matches[:5])

    for year, data in raw_by_year.items():
        filled = {key: value for key, value in data.items() if value is not None}
        logger.info("XLSX campos preenchidos %s: %s", year, ", ".join(sorted(filled.keys())))
        for field in ["ativo_total", "passivo_total", "patrimonio_liquido", "receita_liquida", "lucro_liquido"]:
            logger.info("XLSX %s %s: %s", year, field, data.get(field))

    critical_values = [
        data.get(field)
        for data in raw_by_year.values()
        for field in ["ativo_total", "passivo_total", "patrimonio_liquido", "receita_liquida", "lucro_liquido"]
    ]
    if not critical_values or all(value in {None, 0} for value in critical_values):
        logger.warning("Nenhum valor crítico encontrado no XLSX.")
        logger.warning("Sheets avaliadas: %s", used_sheets)
        if used_sheets:
            df_sample = _normalize_columns(sheets[used_sheets[0]])
            logger.warning(
                "Colunas detectadas: code=%s desc=%s last=%s prev=%s prev2=%s precision=%s",
                _get_column(df_sample, "código conta", "codigo conta"),
                _get_column(df_sample, "descrição conta", "descricao conta"),
                _get_column(df_sample, "valor ultimo exercicio", "valor último exercicio"),
                _get_column(df_sample, "valor penultimo exercicio", "valor penúltimo exercicio"),
                _get_column(df_sample, "valor antepenultimo exercicio", "valor antepenúltimo exercicio"),
                _get_column(df_sample, "precisao", "precisão"),
            )
            logger.warning("Amostra linhas: %s", df_sample.head(3).to_dict(orient="records"))

    return raw_by_year, currency_unit
