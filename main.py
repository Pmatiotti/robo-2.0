import argparse
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from cvm_flow import CvmFlow
from download_manager import download_documents
from indicators import calculate_indicators_by_year
from financial_universe import get_financial_profile
from moniitor_client import MoniitorClient
from normalization import normalize_indicators
from pdf_parser_dfp import parse_dfp_pdf
from xlsx_parser_dfp import parse_xlsx
from utils import (
    calculate_cagr,
    ensure_dir,
    parse_bool,
    parse_decimal,
    setup_logging,
    sha256_file,
)
from zip_extract import copy_excels, extract_zip

load_dotenv()

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CVM DFP Bot")
    parser.add_argument("--input", required=True, help="CSV com ticker e cod_cvm")
    parser.add_argument("--start-date", required=True, help="Data inicial dd/mm/yyyy")
    parser.add_argument("--end-date", required=True, help="Data final dd/mm/yyyy")
    parser.add_argument("--headless", default="true", help="true/false")
    parser.add_argument("--timeout-ms", type=int, default=60000)
    parser.add_argument("--max-retries", type=int, default=3)
    return parser.parse_args()


def load_input(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"ticker", "cod_cvm", "asset_class"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"CSV faltando colunas obrigatórias: {missing}")
    return df


def merge_raw_data(base: Dict[str, Optional[float]], new: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    merged = dict(base)
    for key, value in new.items():
        if merged.get(key) is None and value is not None:
            merged[key] = value
    return merged


def parse_reference_date(value: Optional[str]) -> datetime:
    if not value:
        return datetime.min
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    logger.warning("Formato de reference_date desconhecido: %s", value)
    return datetime.min


def consolidate_documents(documents: List[Dict[str, Any]]) -> Dict[int, Dict[str, Optional[float]]]:
    consolidated: Dict[int, Dict[str, Optional[float]]] = {}
    selected_reference: Dict[int, datetime] = {}

    for document in documents:
        reference_dt = document["reference_datetime"]
        raw_by_year = document["raw_by_year"]
        for year, raw in raw_by_year.items():
            current_dt = selected_reference.get(year, datetime.min)
            if reference_dt > current_dt:
                consolidated[year] = raw
                selected_reference[year] = reference_dt

    return consolidated


def build_absolute_payload(
    normalized_financials: Dict[int, Dict[str, Optional[float]]],
    ticker: str,
    year: int,
) -> Dict[str, Optional[float]]:
    data = normalized_financials.get(year, {})
    absolute_fields = {
        "receita_liquida": data.get("receita_liquida"),
        "lucro_liquido": data.get("lucro_liquido"),
        "lucro_bruto": data.get("lucro_bruto"),
        "caixa": data.get("caixa"),
        "emprestimos_cp": data.get("emprestimos_cp"),
        "emprestimos_lp": data.get("emprestimos_lp"),
        "ebit": data.get("ebit"),
        "ativo_total": data.get("ativo_total"),
        "patrimonio_liquido": data.get("patrimonio_liquido"),
    }
    filled_keys = [key for key, value in absolute_fields.items() if value is not None]
    logger.info(
        "Campos absolutos %s %s preenchidos: %s",
        ticker,
        year,
        filled_keys,
    )
    return absolute_fields


def collect_pdfs(extracted_paths: List[str], pdf_dir: str) -> List[str]:
    ensure_dir(pdf_dir)
    pdfs: List[str] = []
    for path in extracted_paths:
        if os.path.isdir(path):
            for root, _, files in os.walk(path):
                for file_name in files:
                    if file_name.lower().endswith(".pdf"):
                        src = os.path.join(root, file_name)
                        dest = os.path.join(pdf_dir, file_name)
                        shutil.copy2(src, dest)
                        pdfs.append(dest)
        elif path.lower().endswith(".pdf"):
            dest = os.path.join(pdf_dir, os.path.basename(path))
            shutil.copy2(path, dest)
            pdfs.append(dest)
    return pdfs


def write_result(output_root: str, result: Dict[str, Any]) -> Dict[str, Any]:
    result_path = os.path.join(output_root, "result.json")
    with open(result_path, "w", encoding="utf-8") as file_handle:
        json.dump(result, file_handle, ensure_ascii=False, indent=2)
    return result


def has_sufficient_series(series: Dict[int, float], years: int = 5) -> bool:
    if not series or len(series) < 2:
        return False
    sorted_years = sorted(series.keys())
    return sorted_years[-1] - sorted_years[0] >= years - 1


def process_row(
    row: pd.Series,
    start_date: str,
    end_date: str,
    headless: bool,
    timeout_ms: int,
    max_retries: int,
) -> Dict[str, Any]:
    ticker = str(row["ticker"]).upper().strip()
    asset_class = str(row["asset_class"]).strip()
    cod_cvm_raw = row.get("cod_cvm")
    cod_cvm = "" if pd.isna(cod_cvm_raw) else str(cod_cvm_raw).strip()

    output_root = os.path.join("/output", ticker)
    downloads_dir = os.path.join(output_root, "downloads")
    extracted_dir = os.path.join(output_root, "extracted")
    pdf_dir = os.path.join(output_root, "pdfs")
    excel_dir = os.path.join(output_root, "excels")
    ensure_dir(downloads_dir)
    ensure_dir(extracted_dir)
    ensure_dir(pdf_dir)
    ensure_dir(excel_dir)

    result: Dict[str, Any] = {
        "ticker": ticker,
        "codigo_cvm": cod_cvm or None,
        "source": "CVM_RAD_ENET_DFP",
        "period": {"start_date": start_date, "end_date": end_date},
        "currency_unit": None,
        "historical": {"receita_liquida": {}, "lucro_liquido": {}},
        "normalized_financials": {},
        "indicators": {},
        "dividends": {},
        "documents": [],
        "raw_extracted": {},
        "moniitor_payload": {},
        "moniitor_response": None,
        "missing_inputs": [],
        "errors": [],
        "status": "pending",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    current_price = parse_decimal(row.get("current_price"))
    market_cap = parse_decimal(row.get("market_cap"))
    enterprise_value = parse_decimal(row.get("enterprise_value"))
    dividendos_12m = parse_decimal(row.get("dividendos_12m"))

    raw_data_template: Dict[str, Optional[float]] = {
        "ativo_total": None,
        "passivo_total": None,
        "patrimonio_liquido": None,
        "ativo_circulante": None,
        "passivo_circulante": None,
        "estoques": None,
        "caixa": None,
        "receita_liquida": None,
        "lucro_bruto": None,
        "ebit": None,
        "depreciacao": None,
        "amortizacao": None,
        "lucro_liquido": None,
        "lucro_por_acao": None,
        "qtd_acoes_total": None,
        "emprestimos_cp": None,
        "emprestimos_lp": None,
        "dividendos": None,
    }

    currency_unit = "BRL"
    ultimo_dividendo = None
    data_ultimo_dividendo = None
    has_parsing_errors = False
    is_financial, financial_type = get_financial_profile(ticker)
    documents_data: List[Dict[str, Any]] = []

    if not cod_cvm:
        message = "Código CVM ausente no CSV"
        logger.warning("%s | ticker=%s", message, ticker)
        result["errors"].append(message)
        result["status"] = "failed"
        return write_result(output_root, result)

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=headless)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            flow = CvmFlow(page, timeout_ms=timeout_ms)
            logger.info("Processando %s | código CVM %s", ticker, cod_cvm)
            flow.open_enet(cod_cvm)
            flow.apply_filters(start_date, end_date)

            downloads = download_documents(
                page, ticker, cod_cvm, downloads_dir, retries=max_retries
            )
            if not downloads:
                result["errors"].append("Nenhum ZIP encontrado")

            for download in downloads:
                zip_path = download["zip_path"]
                reference_date = download.get("reference_date")
                reference_year = download.get("reference_year")
                protocol_year = download.get("protocol_year")
                num_protocolo = download.get("num_protocolo")
                extracted = extract_zip(zip_path, extracted_dir)
                excel_paths = copy_excels(extracted["xlsx_paths"], excel_dir)
                pdfs = collect_pdfs(extracted["extracted"], pdf_dir)
                sha = sha256_file(zip_path)
                result["documents"].append(
                    {
                        "filename": os.path.basename(zip_path),
                        "sha256": sha,
                        "size_bytes": os.path.getsize(zip_path),
                        "pdfs_extracted": len(pdfs),
                        "reference_date": reference_date,
                    }
                )
                doc_raw_data = dict(raw_data_template)
                doc_raw_by_year: Dict[int, Dict[str, Optional[float]]] = {}
                doc_used_xlsx = False
                if excel_paths:
                    logger.info(
                        "XLSX encontrado para %s: %s", os.path.basename(zip_path), excel_paths[0]
                    )
                    if reference_year:
                        xlsx_raw_by_year, xlsx_currency_unit = parse_xlsx(
                            excel_paths[0],
                            reference_year,
                            prefer_consolidated=True,
                        )
                    elif protocol_year:
                        logger.warning(
                            "Reference date ausente, usando ano do protocolo %s", num_protocolo
                        )
                        xlsx_raw_by_year, xlsx_currency_unit = parse_xlsx(
                            excel_paths[0],
                            protocol_year,
                            prefer_consolidated=True,
                        )
                    else:
                        logger.warning(
                            "Referencia ausente para XLSX em %s, tentando inferir do XLSX",
                            os.path.basename(zip_path),
                        )
                        xlsx_raw_by_year, xlsx_currency_unit = parse_xlsx(
                            excel_paths[0],
                            None,
                            prefer_consolidated=True,
                        )
                    if xlsx_currency_unit == "BRL_THOUSANDS":
                        currency_unit = xlsx_currency_unit
                    for year, parsed in xlsx_raw_by_year.items():
                        doc_raw_data = merge_raw_data(doc_raw_data, parsed)
                        doc_raw_by_year.setdefault(year, {})
                        doc_raw_by_year[year] = merge_raw_data(doc_raw_by_year[year], parsed)
                    if xlsx_raw_by_year:
                        doc_used_xlsx = True

                if not doc_used_xlsx:
                    for pdf_path in pdfs:
                        full_path = os.path.join(pdf_dir, os.path.basename(pdf_path))
                        try:
                            parsed_by_year, unit, dividend_value, dividend_date = parse_dfp_pdf(
                                full_path,
                                {
                                    "output_root": output_root,
                                    "ticker": ticker,
                                    "codigo_cvm": cod_cvm,
                                },
                            )
                            if unit == "BRL_THOUSANDS":
                                currency_unit = unit
                            for year, parsed in parsed_by_year.items():
                                doc_raw_data = merge_raw_data(doc_raw_data, parsed)
                                doc_raw_by_year.setdefault(year, {})
                                doc_raw_by_year[year] = merge_raw_data(doc_raw_by_year[year], parsed)
                            if ultimo_dividendo is None and dividend_value is not None:
                                ultimo_dividendo = dividend_value
                            if data_ultimo_dividendo is None and dividend_date is not None:
                                data_ultimo_dividendo = dividend_date
                        except Exception as exc:
                            has_parsing_errors = True
                            result["errors"].append(f"Erro parse PDF {os.path.basename(pdf_path)}: {exc}")

                reference_dt = parse_reference_date(reference_date)
                years_covered = sorted(doc_raw_by_year.keys())
                document_entry = {
                    "filename": os.path.basename(zip_path),
                    "sha256": sha,
                    "size_bytes": os.path.getsize(zip_path),
                    "pdfs_extracted": len(pdfs),
                    "reference_date": reference_date,
                    "base_year": reference_year or protocol_year,
                    "years_covered": years_covered,
                    "raw_by_year": doc_raw_by_year,
                }
                result["documents"][-1].update(document_entry)
                documents_data.append(
                    {
                        "reference_date": reference_date,
                        "reference_datetime": reference_dt,
                        "base_year": reference_year or protocol_year,
                        "years_covered": years_covered,
                        "raw_by_year": doc_raw_by_year,
                    }
                )

            browser.close()

        consolidated_raw_by_year = consolidate_documents(documents_data)
        raw_data = dict(raw_data_template)
        historical = {"receita_liquida": {}, "lucro_liquido": {}}
        for year in sorted(consolidated_raw_by_year.keys()):
            parsed = consolidated_raw_by_year[year]
            raw_data = merge_raw_data(raw_data, parsed)
            if parsed.get("receita_liquida") is not None:
                historical["receita_liquida"][str(year)] = parsed["receita_liquida"]
            if parsed.get("lucro_liquido") is not None:
                historical["lucro_liquido"][str(year)] = parsed["lucro_liquido"]

        result["raw_extracted"] = raw_data
        result["currency_unit"] = currency_unit
        result["historical"] = historical
        normalized_financials_by_year = consolidated_raw_by_year
        result["normalized_financials"] = {
            str(year): data for year, data in normalized_financials_by_year.items()
        }
        result["raw_by_year"] = consolidated_raw_by_year

        receita_series = {int(year): value for year, value in historical["receita_liquida"].items()}
        lucro_series = {int(year): value for year, value in historical["lucro_liquido"].items()}
        cagr_receitas_5 = calculate_cagr(receita_series, years=5)
        cagr_lucros_5 = calculate_cagr(lucro_series, years=5)
        if not has_sufficient_series(receita_series, years=5):
            result["missing_inputs"].append("serie_5y_incompleta")
        if not has_sufficient_series(lucro_series, years=5):
            result["missing_inputs"].append("serie_5y_incompleta")

        market_data = {
            "current_price": current_price,
            "market_cap": market_cap,
            "enterprise_value": enterprise_value,
            "dividendos_12m": dividendos_12m,
        }
        indicators_by_year, missing_by_indicator_by_year, missing_inputs_by_year, calc_trace_by_year = (
            calculate_indicators_by_year(consolidated_raw_by_year, market_data)
        )

        raw_indicators_by_year: Dict[int, Dict[str, Optional[float]]] = {}
        normalized_indicators_by_year: Dict[int, Dict[str, Optional[float]]] = {}
        conversions_total = 0
        anomalies_total = 0

        for year, indicators in indicators_by_year.items():
            indicators["cagr_receitas_5"] = cagr_receitas_5
            indicators["cagr_lucros_5"] = cagr_lucros_5
            if cagr_receitas_5 is None:
                missing_by_indicator_by_year.setdefault(year, {})["cagr_receitas_5"] = ["historical_receita_liquida"]
            if cagr_lucros_5 is None:
                missing_by_indicator_by_year.setdefault(year, {})["cagr_lucros_5"] = ["historical_lucro_liquido"]
            missing_inputs_by_year.setdefault(year, [])
            if cagr_receitas_5 is None and "historical_receita_liquida" not in missing_inputs_by_year[year]:
                missing_inputs_by_year[year].append("historical_receita_liquida")
            if cagr_lucros_5 is None and "historical_lucro_liquido" not in missing_inputs_by_year[year]:
                missing_inputs_by_year[year].append("historical_lucro_liquido")
            raw_indicators_by_year[year] = dict(indicators)
            normalized, conversions, anomalies = normalize_indicators(
                indicators,
                is_financial,
                ticker,
                year,
            )
            normalized_indicators_by_year[year] = normalized
            conversions_total += conversions
            anomalies_total += anomalies

        payloads = []
        liquidez_media_diaria = parse_decimal(row.get("liquidez_media_diaria"))
        for year, indicators in normalized_indicators_by_year.items():
            absolute_payload = build_absolute_payload(
                normalized_financials_by_year,
                ticker,
                year,
            )
            payload = {
                "ticker": ticker,
                "asset_class": asset_class,
                "data_source": "cvm_dfp_bot",
                "fiscal_year": year,
                "is_financial": is_financial,
                "financial_type": financial_type,
                "current_price": current_price,
                "market_cap": market_cap,
                "enterprise_value": enterprise_value,
                "dividend_yield": None,
                "ultimo_dividendo": ultimo_dividendo,
                "data_ultimo_dividendo": data_ultimo_dividendo,
                **absolute_payload,
                **{k: v for k, v in indicators.items()},
            }
            if liquidez_media_diaria is not None:
                payload["liquidez_media_diaria"] = liquidez_media_diaria
            payloads.append(payload)
        result["moniitor_payload"] = payloads
        result["indicators"] = normalized_indicators_by_year
        result["raw_indicators_by_year"] = raw_indicators_by_year
        result["normalized_indicators_by_year"] = normalized_indicators_by_year
        result["calc_trace_by_year"] = calc_trace_by_year
        result["dividends"] = {
            "ultimo_dividendo": ultimo_dividendo,
            "data_ultimo_dividendo": data_ultimo_dividendo,
        }
        result["missing_inputs_by_year"] = missing_inputs_by_year
        result["missing_inputs_by_indicator_by_year"] = missing_by_indicator_by_year
        aggregated_missing = set(result["missing_inputs"])
        for missing_inputs in missing_inputs_by_year.values():
            aggregated_missing.update(missing_inputs)
        result["missing_inputs"] = list(aggregated_missing)
        logger.info(
            "Normalizações: %s convertidos | %s anomalias | financeiros=%s",
            conversions_total,
            anomalies_total,
            int(is_financial),
        )

        try:
            client = MoniitorClient()
            if payloads:
                response = client.send_batch(payloads)
            else:
                response = {"error": "No fiscal year indicators to send"}
            result["moniitor_response"] = response
        except ValueError as exc:
            result["errors"].append(str(exc))
        if result["errors"] or has_parsing_errors:
            result["status"] = "partial"
        else:
            result["status"] = "success"

    except Exception as exc:
        logger.exception("Erro ao processar %s", ticker)
        result["errors"].append(str(exc))
        result["status"] = "failed"

    return write_result(output_root, result)


def main() -> None:
    args = parse_args()
    setup_logging()
    df = load_input(args.input)
    headless = parse_bool(args.headless, default=True)

    for _, row in df.iterrows():
        process_row(
            row,
            args.start_date,
            args.end_date,
            headless,
            args.timeout_ms,
            args.max_retries,
        )


if __name__ == "__main__":
    main()
