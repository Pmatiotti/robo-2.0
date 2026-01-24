import logging

from playwright.sync_api import Page

logger = logging.getLogger(__name__)

RAD_ENET_URL = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx"


class CvmFlow:
    def __init__(self, page: Page, timeout_ms: int = 60000) -> None:
        self.page = page
        self.timeout_ms = timeout_ms

    def _wait_for_overlay(self) -> None:
        overlay = self.page.locator(".ui-widget-overlay.ui-front")
        if overlay.count() == 0:
            return
        try:
            overlay.wait_for(state="hidden", timeout=20000)
        except Exception:
            logger.debug("Overlay ainda visível, tentando remover via script")
            try:
                self.page.evaluate(
                    "document.querySelectorAll('.ui-widget-overlay.ui-front').forEach(el => el.remove());"
                )
            except Exception:
                logger.debug("Não foi possível remover overlay via script")

    def open_enet(self, codigo_cvm: str) -> None:
        url = f"{RAD_ENET_URL}?tipoconsulta=CVM&codigoCVM={codigo_cvm}"
        logger.info("Acessando ENET: %s", url)
        self.page.goto(url, timeout=self.timeout_ms)

    def apply_filters(self, start_date: str, end_date: str) -> None:
        self.page.check("#rdPeriodo")
        self.page.fill("#txtDataIni", start_date)
        self.page.dispatch_event("#txtDataIni", "blur")
        self.page.fill("#txtDataFim", end_date)
        self.page.dispatch_event("#txtDataFim", "blur")
        self._select_dfp_category()
        self.page.click("#btnConsulta")
        self.page.wait_for_selector("i.fi-download", timeout=self.timeout_ms)

    def _select_dfp_category(self) -> None:
        select = self.page.locator("#cboCategorias")
        if select.count():
            try:
                select.select_option(label="DFP - Demonstrações Financeiras Padronizadas")
                return
            except Exception:
                logger.info("Fallback para select2/chosen")

        chosen = self.page.locator("#cboCategorias_chosen")
        if chosen.count() == 0:
            raise ValueError("Não foi possível localizar seletor de categorias")
        self._wait_for_overlay()
        chosen.click(no_wait_after=True)
        option = self.page.locator(
            "#cboCategorias_chosen .chosen-results li",
            has_text="DFP - Demonstrações Financeiras Padronizadas",
        )
        if option.count() == 0:
            raise ValueError("Categoria DFP não encontrada")
        option.first.click()
