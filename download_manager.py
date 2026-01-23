import logging
import os
from typing import List

from playwright.sync_api import Page

from utils import ensure_dir
from zip_extract import validate_zip

logger = logging.getLogger(__name__)

DOWNLOAD_ICON_SELECTOR = "i.fi-download[title='Download']"


def _build_zip_name(ticker: str, codigo_cvm: str, index: int) -> str:
    return f"{ticker}__{codigo_cvm}__{index:03d}.zip"


def _download_single(page: Page, icon_locator, path: str, retries: int) -> None:
    for attempt in range(retries):
        try:
            with page.expect_download() as download_info:
                icon_locator.click()
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
) -> List[str]:
    ensure_dir(downloads_dir)
    rows = page.locator("tr").filter(has=page.locator(DOWNLOAD_ICON_SELECTOR))
    row_count = rows.count()
    if row_count == 0:
        logger.warning("Nenhum documento encontrado para download")
        return []

    active_count = 0
    inactive_count = 0
    paths: List[str] = []
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
        logger.info("Baixando %s", filename)
        _download_single(page, icon.first, dest_path, retries)
        paths.append(dest_path)

    logger.info("Documentos ativos: %s | ignorados por status: %s", active_count, inactive_count)
    if active_count == 0:
        logger.warning("Nenhum documento com status Ativo encontrado")
    return paths
