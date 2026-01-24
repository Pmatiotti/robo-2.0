import logging
import os
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests

from playwright.sync_api import Page

from utils import ensure_dir
from zip_extract import validate_zip

logger = logging.getLogger(__name__)

DOWNLOAD_ICON_SELECTOR = "i.fi-download[title='Download']"
DATE_RE = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
ONCLICK_RE = re.compile(r"OpenDownloadDocumentos\((.*?)\)")


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


def _parse_onclick_to_url(onclick_str: str) -> Optional[Tuple[str, str]]:
    match = ONCLICK_RE.search(onclick_str or "")
    if not match:
        return None
    args = re.findall(r"'(.*?)'", match.group(1))
    if len(args) < 4:
        return None
    num_sequencia, num_versao, num_protocolo, desc_tipo = args[:4]
    params = {
        "Tela": "ext",
        "numSequencia": num_sequencia,
        "numVersao": num_versao,
        "numProtocolo": num_protocolo,
        "descTipo": desc_tipo,
        "CodigoInstituicao": "1",
    }
    url = f"https://www.rad.cvm.gov.br/ENET/frmDownloadDocumento.aspx?{urlencode(params)}"
    return url, "GET"


def _download_http(page: Page, url: str, path: str) -> None:
    session = requests.Session()
    cookies = page.context.cookies()
    for cookie in cookies:
        session.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain"))
    response = session.get(url, timeout=60)
    response.raise_for_status()
    content = response.content
    if len(content) < 4 or content[:2] != b"PK":
        raise ValueError("Resposta não parece ZIP válido")
    with open(path, "wb") as handle:
        handle.write(content)
    logger.info("Download HTTP concluído (%s bytes)", len(content))


def _download_via_response(page: Page, clickable, path: str) -> None:
    captured: Dict[str, Optional[bytes]] = {"content": None}
    recent: List[Tuple[str, int, str]] = []

    def handler(response) -> None:
        try:
            url = response.url
            content_type = response.headers.get("content-type", "")
            if "zip" in content_type.lower() or "download" in url.lower():
                recent.append((url, response.status, content_type))
                if len(recent) > 10:
                    recent.pop(0)
                body = response.body()
                if body and body[:2] == b"PK":
                    captured["content"] = body
        except Exception:
            return

    page.on("response", handler)
    clickable.click(force=True, no_wait_after=True)
    start = time.time()
    while time.time() - start < 60:
        if captured["content"]:
            break
        time.sleep(0.5)
    page.off("response", handler)

    if not captured["content"]:
        logger.warning("Falha ao capturar ZIP via response")
        for url, status, ctype in recent[-5:]:
            logger.warning("Resposta recente: %s | %s | %s", url, status, ctype)
        raise ValueError("Não foi possível capturar ZIP via response")
    with open(path, "wb") as handle:
        handle.write(captured["content"])
    logger.info("Download via response concluído (%s bytes)", len(captured["content"]))


def _download_single(page: Page, icon_locator, path: str, retries: int) -> None:
    for attempt in range(retries):
        try:
            _wait_for_splash(page)
            clickable = icon_locator.locator("xpath=ancestor::a[1]")
            if clickable.count() == 0:
                clickable = icon_locator.locator("xpath=ancestor::button[1]")
            if clickable.count() == 0:
                clickable = icon_locator.locator("xpath=ancestor::*[@onclick][1]")
            target = clickable.first if clickable.count() else icon_locator

            onclick = target.get_attribute("onclick")
            if onclick:
                logger.info("Onclick capturado: %s", onclick)
                parsed = _parse_onclick_to_url(onclick)
                if parsed:
                    url, method = parsed
                    logger.info("Baixando via HTTP (%s)", url)
                    if method == "GET":
                        _download_http(page, url, path)
                    else:
                        _download_http(page, url, path)
                    return

            _download_via_response(page, target, path)

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
