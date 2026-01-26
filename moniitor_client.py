import logging
import os
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class MoniitorClient:
    def __init__(self) -> None:
        self.url = os.getenv(
            "MONIITOR_INGEST_URL",
            "https://xlmvqhjwliamckyxlpfi.supabase.co/functions/v1/ingest-fundamental-data",
        )
        self.api_key = os.getenv("MONIITOR_API_KEY")

        if not self.api_key:
            raise ValueError("MONIITOR_API_KEY não configurada no .env")

    def _get_headers(self) -> Dict[str, str]:
        return {"Content-Type": "application/json", "x-api-key": self.api_key}

    def send_single(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._send({"data": payload})

    def send_batch(self, payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self._send({"data": payloads})

    def _send(self, body: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
        masked_key = (
            f"{self.api_key[:4]}...{self.api_key[-4:]}" if self.api_key else "N/A"
        )
        logger.info("Enviando para Moniitor (key: %s)", masked_key)

        for attempt in range(retries):
            try:
                response = requests.post(
                    self.url,
                    json=body,
                    headers=self._get_headers(),
                    timeout=30,
                )

                if response.status_code == 200:
                    result = response.json()
                    logger.info(
                        "Sucesso: %s registros processados",
                        result.get("processed", 0),
                    )
                    return result

                if response.status_code in {401, 403}:
                    logger.error("API Key inválida - verifique MONIITOR_API_KEY")
                    return {"error": "Unauthorized", "status": response.status_code}

                if response.status_code == 400:
                    error_data = response.json()
                    logger.error("Payload inválido: %s", error_data)
                    return error_data

                logger.warning(
                    "Tentativa %s/%s falhou: %s",
                    attempt + 1,
                    retries,
                    response.status_code,
                )

            except requests.exceptions.Timeout:
                logger.warning("Timeout na tentativa %s/%s", attempt + 1, retries)
            except requests.exceptions.RequestException as exc:
                logger.error("Erro de conexão: %s", exc)

        return {"error": "Max retries exceeded", "status": -1}
