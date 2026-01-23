# CVM DFP Bot

Robô para coletar DFPs na CVM, extrair dados contábeis e enviar indicadores ao Moniitor.

## Requisitos

- Python 3.10+
- Playwright com navegadores instalados

```bash
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Configuração

Crie um arquivo `.env` com base no `.env.example`.

## Uso

```bash
python main.py \
  --input tickers.csv \
  --start-date 01/01/2024 \
  --end-date 01/01/2026 \
  --headless true \
  --timeout-ms 60000 \
  --max-retries 3
```

## Saída

Para cada ticker, o robô gera uma pasta em `/output/<TICKER>` com:

- `downloads/` ZIPs baixados
- `extracted/` conteúdo descompactado
- `pdfs/` PDFs copiados
- `result.json` com auditoria completa
