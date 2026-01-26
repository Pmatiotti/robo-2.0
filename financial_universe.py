FINANCIAL_TICKERS = {
    "BBAS3",
    "BBDC3",
    "BBDC4",
    "ITUB3",
    "ITUB4",
    "SANB3",
    "SANB4",
    "BPAC3",
    "BPAC11",
    "BRSR6",
    "BAZA3",
    "BEES3",
    "BEES4",
    "BMEB3",
    "BNBR3",
    "PINE4",
    "ABCB4",
    "BBSE3",
    "IRBR3",
    "PSSA3",
}

FINANCIAL_TYPES = {
    "BBAS3": "bank",
    "BBDC3": "bank",
    "BBDC4": "bank",
    "ITUB3": "bank",
    "ITUB4": "bank",
    "SANB3": "bank",
    "SANB4": "bank",
    "BPAC3": "bank",
    "BPAC11": "bank",
    "BRSR6": "bank",
    "BAZA3": "bank",
    "BEES3": "bank",
    "BEES4": "bank",
    "BMEB3": "bank",
    "BNBR3": "bank",
    "PINE4": "bank",
    "ABCB4": "bank",
    "BBSE3": "insurer",
    "IRBR3": "insurer",
    "PSSA3": "insurer",
}


def is_financial_ticker(ticker: str) -> bool:
    return ticker.upper().strip() in FINANCIAL_TICKERS


def get_financial_profile(ticker: str) -> tuple[bool, str | None]:
    normalized = ticker.upper().strip()
    return normalized in FINANCIAL_TICKERS, FINANCIAL_TYPES.get(normalized)
