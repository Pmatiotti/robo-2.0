from typing import Dict, List, Optional, Tuple


def _valid(value: Optional[float]) -> bool:
    return value is not None and value > 0


def _safe_div(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if not _valid(numerator) or not _valid(denominator):
        return None
    return numerator / denominator


def _record_missing(missing: Dict[str, List[str]], indicator: str, fields: List[str]) -> None:
    missing[indicator] = fields


def calculate_indicators(
    raw: Dict[str, Optional[float]],
    current_price: Optional[float],
    market_cap: Optional[float],
    enterprise_value: Optional[float],
    dividendos_12m: Optional[float],
    cagr_receitas_5: Optional[float],
    cagr_lucros_5: Optional[float],
) -> Tuple[Dict[str, Optional[float]], Dict[str, List[str]], List[str]]:
    indicators: Dict[str, Optional[float]] = {}
    missing_by_indicator: Dict[str, List[str]] = {}
    missing_inputs: List[str] = []

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

    ebitda = None
    if _valid(ebit) and _valid(depreciacao) and _valid(amortizacao):
        ebitda = ebit + depreciacao + amortizacao

    vpa = _safe_div(patrimonio_liquido, qtd_acoes_total)

    indicators["p_l"] = _safe_div(current_price, _safe_div(lucro_liquido, qtd_acoes_total))
    if indicators["p_l"] is None:
        _record_missing(missing_by_indicator, "p_l", ["current_price", "lucro_liquido", "qtd_acoes_total"])

    indicators["p_vp"] = _safe_div(current_price, vpa)
    if indicators["p_vp"] is None:
        _record_missing(missing_by_indicator, "p_vp", ["current_price", "patrimonio_liquido", "qtd_acoes_total"])

    indicators["ev_ebitda"] = _safe_div(enterprise_value, ebitda)
    if indicators["ev_ebitda"] is None:
        _record_missing(missing_by_indicator, "ev_ebitda", ["enterprise_value", "ebit", "depreciacao", "amortizacao"])

    indicators["p_ebit"] = _safe_div(current_price, _safe_div(ebit, qtd_acoes_total))
    if indicators["p_ebit"] is None:
        _record_missing(missing_by_indicator, "p_ebit", ["current_price", "ebit", "qtd_acoes_total"])

    indicators["p_ebitda"] = _safe_div(current_price, _safe_div(ebitda, qtd_acoes_total))
    if indicators["p_ebitda"] is None:
        _record_missing(missing_by_indicator, "p_ebitda", ["current_price", "ebit", "depreciacao", "amortizacao", "qtd_acoes_total"])

    indicators["p_ativo"] = _safe_div(market_cap, ativo_total)
    if indicators["p_ativo"] is None:
        _record_missing(missing_by_indicator, "p_ativo", ["market_cap", "ativo_total"])

    capital_giro = None
    if _valid(ativo_circulante) and _valid(passivo_circulante):
        capital_giro = ativo_circulante - passivo_circulante
    indicators["p_cap_giro"] = _safe_div(market_cap, capital_giro)
    if indicators["p_cap_giro"] is None:
        _record_missing(missing_by_indicator, "p_cap_giro", ["market_cap", "ativo_circulante", "passivo_circulante"])

    ativo_circ_liq = None
    if _valid(ativo_circulante) and _valid(passivo_circulante):
        ativo_circ_liq = ativo_circulante - passivo_circulante
    indicators["p_ativo_circ_liq"] = _safe_div(market_cap, ativo_circ_liq)
    if indicators["p_ativo_circ_liq"] is None:
        _record_missing(missing_by_indicator, "p_ativo_circ_liq", ["market_cap", "ativo_circulante", "passivo_circulante"])

    indicators["payout_ratio"] = _safe_div(dividendos_12m, lucro_liquido)
    if indicators["payout_ratio"] is None:
        _record_missing(missing_by_indicator, "payout_ratio", ["dividendos_12m", "lucro_liquido"])

    indicators["roe"] = _safe_div(lucro_liquido, patrimonio_liquido)
    if indicators["roe"] is None:
        _record_missing(missing_by_indicator, "roe", ["lucro_liquido", "patrimonio_liquido"])

    indicators["roa"] = _safe_div(lucro_liquido, ativo_total)
    if indicators["roa"] is None:
        _record_missing(missing_by_indicator, "roa", ["lucro_liquido", "ativo_total"])

    capital_investido = None
    if _valid(ativo_total) and _valid(passivo_circulante):
        capital_investido = ativo_total - passivo_circulante
    nopat = _safe_div(ebit, 1.0)
    indicators["roic"] = _safe_div(nopat, capital_investido)
    if indicators["roic"] is None:
        _record_missing(missing_by_indicator, "roic", ["ebit", "ativo_total", "passivo_circulante"])

    indicators["m_bruta"] = _safe_div(lucro_bruto, receita_liquida)
    if indicators["m_bruta"] is None:
        _record_missing(missing_by_indicator, "m_bruta", ["lucro_bruto", "receita_liquida"])

    indicators["m_ebitda"] = _safe_div(ebitda, receita_liquida)
    if indicators["m_ebitda"] is None:
        _record_missing(missing_by_indicator, "m_ebitda", ["ebit", "depreciacao", "amortizacao", "receita_liquida"])

    indicators["m_ebit"] = _safe_div(ebit, receita_liquida)
    if indicators["m_ebit"] is None:
        _record_missing(missing_by_indicator, "m_ebit", ["ebit", "receita_liquida"])

    indicators["m_liquida"] = _safe_div(lucro_liquido, receita_liquida)
    if indicators["m_liquida"] is None:
        _record_missing(missing_by_indicator, "m_liquida", ["lucro_liquido", "receita_liquida"])

    divida_bruta = None
    if _valid(emprestimos_cp) and _valid(emprestimos_lp):
        divida_bruta = emprestimos_cp + emprestimos_lp
    divida_liquida = None
    if _valid(divida_bruta) and _valid(caixa):
        divida_liquida = divida_bruta - caixa

    indicators["div_liquida_ebitda"] = _safe_div(divida_liquida, ebitda)
    if indicators["div_liquida_ebitda"] is None:
        _record_missing(missing_by_indicator, "div_liquida_ebitda", ["emprestimos_cp", "emprestimos_lp", "caixa", "ebit", "depreciacao", "amortizacao"])

    indicators["div_liquida_ebit"] = _safe_div(divida_liquida, ebit)
    if indicators["div_liquida_ebit"] is None:
        _record_missing(missing_by_indicator, "div_liquida_ebit", ["emprestimos_cp", "emprestimos_lp", "caixa", "ebit"])

    indicators["div_liquida_pl"] = _safe_div(divida_liquida, patrimonio_liquido)
    if indicators["div_liquida_pl"] is None:
        _record_missing(missing_by_indicator, "div_liquida_pl", ["emprestimos_cp", "emprestimos_lp", "caixa", "patrimonio_liquido"])

    indicators["passivo_ativo"] = _safe_div(passivo_total, ativo_total)
    if indicators["passivo_ativo"] is None:
        _record_missing(missing_by_indicator, "passivo_ativo", ["passivo_total", "ativo_total"])

    indicators["liq_corrente"] = _safe_div(ativo_circulante, passivo_circulante)
    if indicators["liq_corrente"] is None:
        _record_missing(missing_by_indicator, "liq_corrente", ["ativo_circulante", "passivo_circulante"])

    indicators["pl_ativo"] = _safe_div(patrimonio_liquido, ativo_total)
    if indicators["pl_ativo"] is None:
        _record_missing(missing_by_indicator, "pl_ativo", ["patrimonio_liquido", "ativo_total"])

    indicators["cagr_receitas_5"] = cagr_receitas_5
    if indicators["cagr_receitas_5"] is None:
        _record_missing(missing_by_indicator, "cagr_receitas_5", ["historical_receita_liquida"])
    indicators["cagr_lucros_5"] = cagr_lucros_5
    if indicators["cagr_lucros_5"] is None:
        _record_missing(missing_by_indicator, "cagr_lucros_5", ["historical_lucro_liquido"])

    indicators["giro_ativos"] = _safe_div(receita_liquida, ativo_total)
    if indicators["giro_ativos"] is None:
        _record_missing(missing_by_indicator, "giro_ativos", ["receita_liquida", "ativo_total"])

    indicators["vpa"] = vpa
    if indicators["vpa"] is None:
        _record_missing(missing_by_indicator, "vpa", ["patrimonio_liquido", "qtd_acoes_total"])

    indicators["patrimonio_liquido"] = patrimonio_liquido

    for fields in missing_by_indicator.values():
        for field in fields:
            if field not in missing_inputs:
                missing_inputs.append(field)

    return indicators, missing_by_indicator, missing_inputs
