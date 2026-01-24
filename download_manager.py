import logging
import os
import re
from typing import Dict, List, Optional

from playwright.sync_api import Page

from utils import ensure_dir
from zip_extract import validate_zip

logger = logging.getLogger(__name__)

DOWNLOAD_ICON_SELECTOR = "i.fi-download[title='Download']"
DATE_RE = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")


def _build_zip_name(ticker: str, codigo_cvm: str, index: int) -> str:
    return f"{ticker}__{codigo_cvm}__{index:03d}.zip"


def _wait_for_splash(page: Page) -> None:
    splash = page.locator("#divSplash")
    if splash.count() == 0:
        return
    try:
        splash.wait_for(state="hidden", timeout=10000)
    except Exception:
        logger.debug("Splash overlay ainda visível, tentando clicar mesmo assim")


def _download_single(page: Page, icon_locator, path: str, retries: int) -> None:
    for attempt in range(retries):
        try:
            _wait_for_splash(page)
            with page.expect_download() as download_info:
                icon_locator.click(no_wait_after=True)
            download = download_info.value
            download.save_as(path)

            if not validate_zip(path):
                raise ValueError(f"ZIP inválido: {path}")
            return
        except Exception as exc:
            logger.warning("Falha download %s (tentativa %s/%s): %s", path, attempt + 1, retries, exc)
            if attempt + 1 == retries:
                raise


def download_documents(
    page: Page,
    ticker: str,
    codigo_cvm: str,
    downloads_dir: str,
    retries: int = 3,
) -> List[Dict[str, Optional[str]]]:
    ensure_dir(downloads_dir)
    rows = page.locator("tr").filter(has=page.locator(DOWNLOAD_ICON_SELECTOR))
    row_count = rows.count()
    if row_count == 0:
        logger.warning("Nenhum documento encontrado para download")
        return []

    active_count = 0
    inactive_count = 0
    downloads: List[Dict[str, Optional[str]]] = []
    for idx in range(row_count):
        row = rows.nth(idx)
        cells = [text.strip() for text in row.locator("td").all_inner_texts()]
        is_active = any(cell.lower() == "ativo" for cell in cells)
        is_inactive = any(cell.lower() == "inativo" for cell in cells)
        if not is_active:
            inactive_count += 1
            if not is_inactive:
                logger.debug("Documento ignorado sem status Ativo/Inativo na linha %s", idx + 1)
            continue

        icon = row.locator(DOWNLOAD_ICON_SELECTOR)
        if icon.count() == 0:
            logger.warning("Linha %s marcada como Ativo sem ícone de download", idx + 1)
            continue

        active_count += 1
        filename = _build_zip_name(ticker, codigo_cvm, idx + 1)
        dest_path = os.path.join(downloads_dir, filename)
        reference_date = None
        for cell in cells:
            match = DATE_RE.search(cell)
            if match:
                reference_date = match.group(0)
                break
        logger.info("Baixando %s", filename)
        _download_single(page, icon.first, dest_path, retries)
        downloads.append({"zip_path": dest_path, "reference_date": reference_date})

    logger.info("Documentos ativos: %s | ignorados por status: %s", active_count, inactive_count)
    if active_count == 0:
        logger.warning("Nenhum documento com status Ativo encontrado")
    return downloads
