import logging
import re
import unicodedata
from datetime import datetime
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
    "6.01.01.04": "depreciacao",
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


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.lower()
    normalized = re.sub(r"[\s_\-]+", "_", normalized)
    return normalized.strip("_")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_text(col) for col in df.columns]
    return df


def _normalize_sheet_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(name))
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.lower()
    return re.sub(r"[\s_\-]+", "", normalized)


def _get_column_by_tokens(df: pd.DataFrame, *tokens: str) -> Optional[str]:
    for col in df.columns:
        if all(token in col for token in tokens):
            return col
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


def _normalize_year(year: int) -> int:
    if year < 100:
        if year <= 79:
            return 2000 + year
        return 1900 + year
    return year


def _parse_date_to_year(value: object) -> Optional[int]:
    if isinstance(value, datetime):
        return value.year
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().year
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"(\d{2})/(\d{2})/(\d{2,4})", text)
    if match:
        year = int(match.group(3))
        return _normalize_year(year)
    match = re.search(r"(\d{4})", text)
    if match:
        return int(match.group(1))
    return None


def _infer_base_year_from_df(df: pd.DataFrame) -> Optional[int]:
    candidates = [
        "ultimo_exercicio",
        "data_ultimo_exercicio",
        "data_do_ultimo_exercicio",
        "dt_ultimo_exercicio",
        "data_fim_exercicio",
        "data_referencia",
    ]
    for column in df.columns:
        if any(candidate in column for candidate in candidates):
            series = df[column].dropna()
            if series.empty:
                continue
            for value in series.head(10):
                year = _parse_date_to_year(value)
                if year:
                    return year
    return None


def infer_workbook_base_year(
    sheets: Dict[str, pd.DataFrame],
) -> Optional[int]:
    for name, df in sheets.items():
        normalized = _normalize_sheet_name(name)
        if "composicaocapital" not in normalized:
            continue
        df_norm = _normalize_columns(df)
        year = _infer_base_year_from_df(df_norm)
        if year:
            return year
        for _, row in df_norm.dropna(how="all").head(3).iterrows():
            for value in row.values:
                year = _parse_date_to_year(value)
                if year:
                    return year
    return None


def _infer_base_year_from_sheet(df: pd.DataFrame) -> Optional[int]:
    inferred_year = _infer_base_year_from_df(df)
    if inferred_year:
        return inferred_year
    for _, row in df.dropna(how="all").head(3).iterrows():
        for value in row.values:
            year = _parse_date_to_year(value)
            if year:
                return year
    return None


def _populate_share_counts(
    raw_by_year: Dict[int, Dict[str, Optional[float]]],
    df: pd.DataFrame,
    base_year: int,
) -> None:
    issued_col = _get_column_by_tokens(df, "total", "capital", "integralizado")
    treasury_col = _get_column_by_tokens(df, "total", "tesouraria")
    precision_col = _get_column_by_tokens(df, "precisao") or _get_column_by_tokens(df, "unidade")
    if not issued_col:
        return
    multiplier = _parse_precision(df.get(precision_col).iloc[0] if precision_col else None)
    first_row = df.dropna(how="all").head(1)
    if first_row.empty:
        return
    row = first_row.iloc[0]
    shares_issued = _parse_number(row.get(issued_col))
    shares_treasury = _parse_number(row.get(treasury_col)) if treasury_col else None
    if shares_issued is None:
        return
    shares_issued *= multiplier
    if shares_treasury is not None:
        shares_treasury *= multiplier
    shares_outstanding = None
    if shares_treasury is not None and shares_issued >= shares_treasury:
        shares_outstanding = shares_issued - shares_treasury
    raw_by_year.setdefault(base_year, {})
    if shares_outstanding is not None and raw_by_year[base_year].get("qtd_acoes_total") is None:
        raw_by_year[base_year]["qtd_acoes_total"] = shares_outstanding
    if raw_by_year[base_year].get("qtd_acoes_emitidas") is None:
        raw_by_year[base_year]["qtd_acoes_emitidas"] = shares_issued
    if shares_treasury is not None and raw_by_year[base_year].get("qtd_acoes_tesouraria") is None:
        raw_by_year[base_year]["qtd_acoes_tesouraria"] = shares_treasury


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
    reference_year: Optional[int],
    prefer_consolidated: bool = True,
) -> Tuple[Dict[int, Dict[str, Optional[float]]], str]:
    sheets = pd.read_excel(path_xlsx, sheet_name=None)
    currency_unit = "BRL"
    raw_by_year: Dict[int, Dict[str, Optional[float]]] = {}

    normalized_sheets = {name: _normalize_sheet_name(name) for name in sheets}
    logger.info("Sheets encontradas: %s", list(sheets.keys()))
    candidates = []
    for name, normalized in normalized_sheets.items():
        scope = "cons" if "cons" in normalized else "ind" if "ind" in normalized else None
        kind = None
        for key in ["ativo", "passivo", "resultado", "fluxo"]:
            if key in normalized:
                kind = key
                break
        candidates.append((name, scope, kind))

    preferred_scope = "cons" if prefer_consolidated else "ind"
    preferred = [name for name, scope, _ in candidates if scope == preferred_scope]
    fallback = [name for name, scope, _ in candidates if scope != preferred_scope]
    sheet_names = preferred or fallback
    logger.info("Sheets usadas: %s", sheet_names)

    workbook_base_year = infer_workbook_base_year(sheets)
    if workbook_base_year is None:
        logger.warning("workbook_base_year não encontrado (sem fallback)")
        return {}, currency_unit
    logger.info("workbook_base_year=%s source=composicao_capital", workbook_base_year)

    if workbook_base_year:
        logger.info(
            "anos permitidos: %s",
            [workbook_base_year, workbook_base_year - 1, workbook_base_year - 2],
        )

    for name, normalized in normalized_sheets.items():
        if "composicaocapital" in normalized:
            df_capital = _normalize_columns(sheets[name])
            _populate_share_counts(raw_by_year, df_capital, workbook_base_year)

    used_sheets = []
    for sheet_name in sheet_names:
        df = _normalize_columns(sheets[sheet_name])
        base_year = workbook_base_year
        inferred_sheet_year = _infer_base_year_from_df(df)
        logger.info(
            "Sheet %s base_year=%s fallback_used=%s inferred_sheet_year=%s",
            sheet_name,
            base_year,
            False,
            inferred_sheet_year,
        )
        code_col = _get_column_by_tokens(df, "codigo")
        desc_col = _get_column_by_tokens(df, "descricao")
        last_col = _get_column_by_tokens(df, "ultimo")
        prev_col = _get_column_by_tokens(df, "penultimo")
        prev2_col = _get_column_by_tokens(df, "antepenultimo")
        precision_col = _get_column_by_tokens(df, "precisao") or _get_column_by_tokens(df, "unidade") or _get_column_by_tokens(df, "escala")
        logger.info(
            "Sheet %s colunas: code=%s desc=%s last=%s prev=%s prev2=%s precision=%s",
            sheet_name,
            code_col,
            desc_col,
            last_col,
            prev_col,
            prev2_col,
            precision_col,
        )
        if not (last_col and prev_col and prev2_col):
            continue
        used_sheets.append(sheet_name)

        sample_matches = []
        for _, row in df.iterrows():
            field = _match_field(row.get(code_col), row.get(desc_col))
            if not field:
                desc_text = str(row.get(desc_col) or "").lower()
                if "deprecia" in desc_text:
                    field = "depreciacao"
                elif "amortiza" in desc_text:
                    field = "amortizacao"
                else:
                    continue
            multiplier = _parse_precision(row.get(precision_col))
            if multiplier == 1000:
                currency_unit = "BRL_THOUSANDS"

            if base_year is None:
                continue
            allowed_years = {base_year, base_year - 1, base_year - 2}
            values = {
                base_year: _parse_number(row.get(last_col)),
                base_year - 1: _parse_number(row.get(prev_col)),
                base_year - 2: _parse_number(row.get(prev2_col)),
            }
            for year, value in values.items():
                if value is None or year not in allowed_years:
                    continue
                raw_by_year.setdefault(year, {})
                if raw_by_year[year].get(field) is None:
                    raw_by_year[year][field] = value * multiplier
                    if field in {"ativo_total", "passivo_total", "patrimonio_liquido", "receita_liquida", "lucro_liquido"}:
                        sample_matches.append((field, year, raw_by_year[year][field]))

        if sample_matches:
            logger.info("Sheet %s: amostras %s", sheet_name, sample_matches[:5])

    allowed_years = {workbook_base_year, workbook_base_year - 1, workbook_base_year - 2}
    raw_by_year = {year: data for year, data in raw_by_year.items() if year in allowed_years}
    for year, data in raw_by_year.items():
        filled = {key: value for key, value in data.items() if value is not None}
        logger.info("XLSX campos preenchidos %s: %s", year, ", ".join(sorted(filled.keys())))
        for field in [
            "ativo_total",
            "passivo_total",
            "patrimonio_liquido",
            "receita_liquida",
            "lucro_liquido",
            "qtd_acoes_total",
            "ebit",
            "depreciacao",
            "amortizacao",
        ]:
            logger.info("XLSX %s %s: %s", year, field, data.get(field))
    if raw_by_year:
        logger.info("Anos gerados: %s", sorted(raw_by_year.keys()))
    if not any(data.get("qtd_acoes_total") for data in raw_by_year.values()):
        logger.warning("qtd_acoes_total ausente (não foi possível derivar shares_outstanding)")

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
                _get_column_by_tokens(df_sample, "codigo"),
                _get_column_by_tokens(df_sample, "descricao"),
                _get_column_by_tokens(df_sample, "ultimo"),
                _get_column_by_tokens(df_sample, "penultimo"),
                _get_column_by_tokens(df_sample, "antepenultimo"),
                _get_column_by_tokens(df_sample, "precisao")
                or _get_column_by_tokens(df_sample, "unidade")
                or _get_column_by_tokens(df_sample, "escala"),
            )
            logger.warning("Amostra linhas: %s", df_sample.head(3).to_dict(orient="records"))

    return raw_by_year, currency_unit
