from typing import Any, Dict, List, Optional, Tuple


def _is_present(value: Optional[float]) -> bool:
    return value is not None


def _gt0(value: Optional[float]) -> bool:
    return value is not None and value > 0


def _safe_div(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _record_missing(missing: Dict[str, List[str]], indicator: str, fields: List[str]) -> None:
    missing[indicator] = fields


def _build_missing_inputs(missing_by_indicator: Dict[str, List[str]]) -> List[str]:
    missing_inputs: List[str] = []
    for fields in missing_by_indicator.values():
        for field in fields:
            if field not in missing_inputs:
                missing_inputs.append(field)
    return missing_inputs


def _calculate_indicators_for_year(
    raw: Dict[str, Optional[float]],
    market_data: Dict[str, Optional[float]],
    aliquota_ir: float = 0.34,
) -> Tuple[Dict[str, Optional[float]], Dict[str, List[str]], List[str], Dict[str, Dict[str, Any]]]:
    indicators: Dict[str, Optional[float]] = {}
    missing_by_indicator: Dict[str, List[str]] = {}
    calc_trace: Dict[str, Dict[str, Any]] = {}

    current_price = market_data.get("current_price")
    market_cap = market_data.get("market_cap")
    enterprise_value = market_data.get("enterprise_value")
    dividendos_12m = market_data.get("dividendos_12m")

    lucro_liquido = raw.get("lucro_liquido")
    patrimonio_liquido = raw.get("patrimonio_liquido")
    ativo_total = raw.get("ativo_total")
    ativo_circulante = raw.get("ativo_circulante")
    passivo_circulante = raw.get("passivo_circulante")
    receita_liquida = raw.get("receita_liquida")
    lucro_bruto = raw.get("lucro_bruto")
    ebit = raw.get("ebit")
    depreciacao = raw.get("depreciacao")
    amortizacao = raw.get("amortizacao")
    qtd_acoes_total = raw.get("qtd_acoes_total")
    passivo_total = raw.get("passivo_total")
    emprestimos_cp = raw.get("emprestimos_cp")
    emprestimos_lp = raw.get("emprestimos_lp")
    caixa = raw.get("caixa")
    estoques = raw.get("estoques")

    ebitda = None
    if _is_present(ebit) and _is_present(depreciacao) and _is_present(amortizacao):
        ebitda = ebit + depreciacao + amortizacao
    calc_trace["ebitda"] = {
        "formula": "ebit+depreciacao+amortizacao",
        "inputs": {"ebit": ebit, "depreciacao": depreciacao, "amortizacao": amortizacao},
    }

    divida_bruta = None
    if _is_present(emprestimos_cp) and _is_present(emprestimos_lp):
        divida_bruta = emprestimos_cp + emprestimos_lp
    calc_trace["divida_bruta"] = {
        "formula": "emprestimos_cp+emprestimos_lp",
        "inputs": {"emprestimos_cp": emprestimos_cp, "emprestimos_lp": emprestimos_lp},
    }

    divida_liquida = None
    if _is_present(divida_bruta) and _is_present(caixa):
        divida_liquida = divida_bruta - caixa
    calc_trace["divida_liquida"] = {
        "formula": "divida_bruta-caixa",
        "inputs": {"divida_bruta": divida_bruta, "caixa": caixa},
    }

    capital_investido = None
    if _is_present(patrimonio_liquido) and _is_present(divida_liquida):
        capital_investido = patrimonio_liquido + divida_liquida
    calc_trace["capital_investido"] = {
        "formula": "patrimonio_liquido+divida_liquida",
        "inputs": {"patrimonio_liquido": patrimonio_liquido, "divida_liquida": divida_liquida},
    }

    nopat = None
    if _is_present(ebit):
        nopat = ebit * (1 - aliquota_ir)
    calc_trace["nopat"] = {
        "formula": "ebit*(1-aliquota_ir)",
        "inputs": {"ebit": ebit, "aliquota_ir": aliquota_ir},
    }

    capital_giro = None
    if _is_present(ativo_circulante) and _is_present(passivo_circulante):
        capital_giro = ativo_circulante - passivo_circulante
    calc_trace["capital_giro"] = {
        "formula": "ativo_circulante-passivo_circulante",
        "inputs": {"ativo_circulante": ativo_circulante, "passivo_circulante": passivo_circulante},
    }

    ativo_circ_liq = None
    if _is_present(ativo_circulante) and _is_present(passivo_circulante):
        estoques_val = estoques if _is_present(estoques) else 0.0
        ativo_circ_liq = ativo_circulante - passivo_circulante - estoques_val
    calc_trace["ativo_circ_liq"] = {
        "formula": "ativo_circulante-passivo_circulante-estoques",
        "inputs": {"ativo_circulante": ativo_circulante, "passivo_circulante": passivo_circulante, "estoques": estoques},
    }

    vpa = _safe_div(patrimonio_liquido, qtd_acoes_total)
    calc_trace["vpa"] = {
        "formula": "patrimonio_liquido/qtd_acoes_total",
        "inputs": {"patrimonio_liquido": patrimonio_liquido, "qtd_acoes_total": qtd_acoes_total},
    }

    lpa = None
    if _gt0(lucro_liquido) and _gt0(qtd_acoes_total):
        lpa = lucro_liquido / qtd_acoes_total
    indicators["p_l"] = None
    if _gt0(current_price) and _gt0(lpa):
        indicators["p_l"] = current_price / lpa
    if indicators["p_l"] is None:
        _record_missing(missing_by_indicator, "p_l", ["current_price", "lucro_liquido", "qtd_acoes_total"])
    calc_trace["p_l"] = {
        "formula": "current_price/(lucro_liquido/qtd_acoes_total)",
        "inputs": {"current_price": current_price, "lucro_liquido": lucro_liquido, "qtd_acoes_total": qtd_acoes_total},
    }

    indicators["p_vp"] = None
    if _gt0(current_price) and _gt0(patrimonio_liquido) and _gt0(qtd_acoes_total) and _gt0(vpa):
        indicators["p_vp"] = current_price / vpa
    if indicators["p_vp"] is None:
        _record_missing(missing_by_indicator, "p_vp", ["current_price", "patrimonio_liquido", "qtd_acoes_total"])
    calc_trace["p_vp"] = {
        "formula": "current_price/(patrimonio_liquido/qtd_acoes_total)",
        "inputs": {"current_price": current_price, "patrimonio_liquido": patrimonio_liquido, "qtd_acoes_total": qtd_acoes_total},
    }

    indicators["ev_ebitda"] = None
    if _gt0(enterprise_value) and _gt0(ebitda):
        indicators["ev_ebitda"] = enterprise_value / ebitda
    if indicators["ev_ebitda"] is None:
        _record_missing(missing_by_indicator, "ev_ebitda", ["enterprise_value", "ebit", "depreciacao", "amortizacao"])
    calc_trace["ev_ebitda"] = {
        "formula": "enterprise_value/ebitda",
        "inputs": {"enterprise_value": enterprise_value, "ebitda": ebitda},
    }

    indicators["p_ebit"] = None
    if _gt0(market_cap) and _gt0(ebit):
        indicators["p_ebit"] = market_cap / ebit
    if indicators["p_ebit"] is None:
        _record_missing(missing_by_indicator, "p_ebit", ["market_cap", "ebit"])
    calc_trace["p_ebit"] = {"formula": "market_cap/ebit", "inputs": {"market_cap": market_cap, "ebit": ebit}}

    indicators["p_ebitda"] = None
    if _gt0(market_cap) and _gt0(ebitda):
        indicators["p_ebitda"] = market_cap / ebitda
    if indicators["p_ebitda"] is None:
        _record_missing(missing_by_indicator, "p_ebitda", ["market_cap", "ebit", "depreciacao", "amortizacao"])
    calc_trace["p_ebitda"] = {
        "formula": "market_cap/ebitda",
        "inputs": {"market_cap": market_cap, "ebitda": ebitda},
    }

    indicators["p_ativo"] = None
    if _gt0(market_cap) and _gt0(ativo_total):
        indicators["p_ativo"] = market_cap / ativo_total
    if indicators["p_ativo"] is None:
        _record_missing(missing_by_indicator, "p_ativo", ["market_cap", "ativo_total"])
    calc_trace["p_ativo"] = {
        "formula": "market_cap/ativo_total",
        "inputs": {"market_cap": market_cap, "ativo_total": ativo_total},
    }

    indicators["p_cap_giro"] = None
    if _gt0(market_cap) and _gt0(capital_giro):
        indicators["p_cap_giro"] = market_cap / capital_giro
    if indicators["p_cap_giro"] is None:
        _record_missing(missing_by_indicator, "p_cap_giro", ["market_cap", "ativo_circulante", "passivo_circulante"])
    calc_trace["p_cap_giro"] = {
        "formula": "market_cap/capital_giro",
        "inputs": {"market_cap": market_cap, "capital_giro": capital_giro},
    }

    indicators["p_ativo_circ_liq"] = None
    if _gt0(market_cap) and _gt0(ativo_circ_liq):
        indicators["p_ativo_circ_liq"] = market_cap / ativo_circ_liq
    if indicators["p_ativo_circ_liq"] is None:
        _record_missing(missing_by_indicator, "p_ativo_circ_liq", ["market_cap", "ativo_circulante", "passivo_circulante", "estoques"])
    calc_trace["p_ativo_circ_liq"] = {
        "formula": "market_cap/ativo_circ_liq",
        "inputs": {"market_cap": market_cap, "ativo_circ_liq": ativo_circ_liq},
    }

    indicators["payout_ratio"] = _safe_div(dividendos_12m, lucro_liquido)
    if indicators["payout_ratio"] is None:
        _record_missing(missing_by_indicator, "payout_ratio", ["dividendos_12m", "lucro_liquido"])
    calc_trace["payout_ratio"] = {
        "formula": "dividendos_12m/lucro_liquido",
        "inputs": {"dividendos_12m": dividendos_12m, "lucro_liquido": lucro_liquido},
    }

    indicators["roe"] = _safe_div(lucro_liquido, patrimonio_liquido)
    if indicators["roe"] is None:
        _record_missing(missing_by_indicator, "roe", ["lucro_liquido", "patrimonio_liquido"])
    calc_trace["roe"] = {
        "formula": "lucro_liquido/patrimonio_liquido",
        "inputs": {"lucro_liquido": lucro_liquido, "patrimonio_liquido": patrimonio_liquido},
    }

    indicators["roa"] = _safe_div(lucro_liquido, ativo_total)
    if indicators["roa"] is None:
        _record_missing(missing_by_indicator, "roa", ["lucro_liquido", "ativo_total"])
    calc_trace["roa"] = {
        "formula": "lucro_liquido/ativo_total",
        "inputs": {"lucro_liquido": lucro_liquido, "ativo_total": ativo_total},
    }

    indicators["roic"] = _safe_div(nopat, capital_investido)
    if indicators["roic"] is None:
        _record_missing(missing_by_indicator, "roic", ["ebit", "patrimonio_liquido", "emprestimos_cp", "emprestimos_lp", "caixa"])
    calc_trace["roic"] = {
        "formula": "nopat/capital_investido",
        "inputs": {"nopat": nopat, "capital_investido": capital_investido},
    }

    indicators["m_bruta"] = _safe_div(lucro_bruto, receita_liquida)
    if indicators["m_bruta"] is None:
        _record_missing(missing_by_indicator, "m_bruta", ["lucro_bruto", "receita_liquida"])
    calc_trace["m_bruta"] = {
        "formula": "lucro_bruto/receita_liquida",
        "inputs": {"lucro_bruto": lucro_bruto, "receita_liquida": receita_liquida},
    }

    indicators["m_ebitda"] = _safe_div(ebitda, receita_liquida)
    if indicators["m_ebitda"] is None:
        _record_missing(missing_by_indicator, "m_ebitda", ["ebit", "depreciacao", "amortizacao", "receita_liquida"])
    calc_trace["m_ebitda"] = {
        "formula": "ebitda/receita_liquida",
        "inputs": {"ebitda": ebitda, "receita_liquida": receita_liquida},
    }

    indicators["m_ebit"] = _safe_div(ebit, receita_liquida)
    if indicators["m_ebit"] is None:
        _record_missing(missing_by_indicator, "m_ebit", ["ebit", "receita_liquida"])
    calc_trace["m_ebit"] = {"formula": "ebit/receita_liquida", "inputs": {"ebit": ebit, "receita_liquida": receita_liquida}}

    indicators["m_liquida"] = _safe_div(lucro_liquido, receita_liquida)
    if indicators["m_liquida"] is None:
        _record_missing(missing_by_indicator, "m_liquida", ["lucro_liquido", "receita_liquida"])
    calc_trace["m_liquida"] = {
        "formula": "lucro_liquido/receita_liquida",
        "inputs": {"lucro_liquido": lucro_liquido, "receita_liquida": receita_liquida},
    }

    indicators["div_liquida_ebitda"] = _safe_div(divida_liquida, ebitda)
    if indicators["div_liquida_ebitda"] is None:
        _record_missing(missing_by_indicator, "div_liquida_ebitda", ["emprestimos_cp", "emprestimos_lp", "caixa", "ebit", "depreciacao", "amortizacao"])
    calc_trace["div_liquida_ebitda"] = {
        "formula": "divida_liquida/ebitda",
        "inputs": {"divida_liquida": divida_liquida, "ebitda": ebitda},
    }

    indicators["div_liquida_ebit"] = _safe_div(divida_liquida, ebit)
    if indicators["div_liquida_ebit"] is None:
        _record_missing(missing_by_indicator, "div_liquida_ebit", ["emprestimos_cp", "emprestimos_lp", "caixa", "ebit"])
    calc_trace["div_liquida_ebit"] = {"formula": "divida_liquida/ebit", "inputs": {"divida_liquida": divida_liquida, "ebit": ebit}}

    indicators["div_liquida_pl"] = _safe_div(divida_liquida, patrimonio_liquido)
    if indicators["div_liquida_pl"] is None:
        _record_missing(missing_by_indicator, "div_liquida_pl", ["emprestimos_cp", "emprestimos_lp", "caixa", "patrimonio_liquido"])
    calc_trace["div_liquida_pl"] = {
        "formula": "divida_liquida/patrimonio_liquido",
        "inputs": {"divida_liquida": divida_liquida, "patrimonio_liquido": patrimonio_liquido},
    }

    indicators["passivo_ativo"] = _safe_div(passivo_total, ativo_total)
    if indicators["passivo_ativo"] is None:
        _record_missing(missing_by_indicator, "passivo_ativo", ["passivo_total", "ativo_total"])
    calc_trace["passivo_ativo"] = {
        "formula": "passivo_total/ativo_total",
        "inputs": {"passivo_total": passivo_total, "ativo_total": ativo_total},
    }

    indicators["liq_corrente"] = _safe_div(ativo_circulante, passivo_circulante)
    if indicators["liq_corrente"] is None:
        _record_missing(missing_by_indicator, "liq_corrente", ["ativo_circulante", "passivo_circulante"])
    calc_trace["liq_corrente"] = {
        "formula": "ativo_circulante/passivo_circulante",
        "inputs": {"ativo_circulante": ativo_circulante, "passivo_circulante": passivo_circulante},
    }

    indicators["pl_ativo"] = _safe_div(patrimonio_liquido, ativo_total)
    if indicators["pl_ativo"] is None:
        _record_missing(missing_by_indicator, "pl_ativo", ["patrimonio_liquido", "ativo_total"])
    calc_trace["pl_ativo"] = {
        "formula": "patrimonio_liquido/ativo_total",
        "inputs": {"patrimonio_liquido": patrimonio_liquido, "ativo_total": ativo_total},
    }

    indicators["giro_ativos"] = _safe_div(receita_liquida, ativo_total)
    if indicators["giro_ativos"] is None:
        _record_missing(missing_by_indicator, "giro_ativos", ["receita_liquida", "ativo_total"])
    calc_trace["giro_ativos"] = {
        "formula": "receita_liquida/ativo_total",
        "inputs": {"receita_liquida": receita_liquida, "ativo_total": ativo_total},
    }

    indicators["vpa"] = vpa
    if indicators["vpa"] is None:
        _record_missing(missing_by_indicator, "vpa", ["patrimonio_liquido", "qtd_acoes_total"])

    indicators["patrimonio_liquido"] = patrimonio_liquido
    calc_trace["patrimonio_liquido"] = {
        "formula": "patrimonio_liquido",
        "inputs": {"patrimonio_liquido": patrimonio_liquido},
    }

    missing_inputs = _build_missing_inputs(missing_by_indicator)
    return indicators, missing_by_indicator, missing_inputs, calc_trace


def calculate_indicators_by_year(
    raw_by_year: Dict[int, Dict[str, Optional[float]]],
    market_data: Dict[str, Optional[float]],
    aliquota_ir: float = 0.34,
) -> Tuple[
    Dict[int, Dict[str, Optional[float]]],
    Dict[int, Dict[str, List[str]]],
    Dict[int, List[str]],
    Dict[int, Dict[str, Dict[str, Any]]],
]:
    indicators_by_year: Dict[int, Dict[str, Optional[float]]] = {}
    missing_by_indicator_by_year: Dict[int, Dict[str, List[str]]] = {}
    missing_inputs_by_year: Dict[int, List[str]] = {}
    calc_trace_by_year: Dict[int, Dict[str, Dict[str, Any]]] = {}

    for year, raw in raw_by_year.items():
        indicators, missing_by_indicator, missing_inputs, calc_trace = _calculate_indicators_for_year(
            raw,
            market_data,
            aliquota_ir=aliquota_ir,
        )
        indicators_by_year[year] = indicators
        missing_by_indicator_by_year[year] = missing_by_indicator
        missing_inputs_by_year[year] = missing_inputs
        calc_trace_by_year[year] = calc_trace

    return indicators_by_year, missing_by_indicator_by_year, missing_inputs_by_year, calc_trace_by_year
