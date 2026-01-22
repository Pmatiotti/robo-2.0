import logging
from typing import Optional, Tuple

from playwright.sync_api import Page

from utils import normalize_cnpj

logger = logging.getLogger(__name__)

CVM_START_URL = (
    "https://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CPublica/CiaAb/"
    "FormBuscaCiaAb.aspx?TipoConsult=c"
)
RAD_ENET_URL = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx"


class CvmFlow:
    def __init__(self, page: Page, timeout_ms: int = 60000) -> None:
        self.page = page
        self.timeout_ms = timeout_ms

    def find_company_by_cnpj(self, cnpj: str) -> Tuple[str, Optional[str]]:
        normalized = normalize_cnpj(cnpj)
        logger.info("Buscando CNPJ %s", normalized)
        self.page.goto(CVM_START_URL, timeout=self.timeout_ms)
        self.page.fill("#txtCNPJNome", normalized)
        with self.page.expect_navigation(timeout=self.timeout_ms):
            self.page.click("#btnContinuar")

        table = self.page.locator("#dlCiasCdCVM")
        rows = table.locator("tr")
        for idx in range(rows.count()):
            row = rows.nth(idx)
            cnpj_span = row.locator("span[id*='_lblCNPJCd']")
            if cnpj_span.count() == 0:
                continue
            row_cnpj = normalize_cnpj(cnpj_span.first.inner_text())
            if row_cnpj != normalized:
                continue
            code_span = row.locator("span[id*='_lblCodigoCVM']")
            codigo_cvm = code_span.first.inner_text().strip() if code_span.count() else None
            anchor = cnpj_span.first.locator("xpath=ancestor::a[1]")
            if anchor.count():
                anchor.first.click()
            return normalized, codigo_cvm

        raise ValueError(f"CNPJ não encontrado: {normalized}")

    def open_enet(self, codigo_cvm: str) -> None:
        url = f"{RAD_ENET_URL}?tipoconsulta=CVM&codigoCVM={codigo_cvm}"
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
        chosen.click()
        option = self.page.locator(
            "#cboCategorias_chosen .chosen-results li",
            has_text="DFP - Demonstrações Financeiras Padronizadas",
        )
        if option.count() == 0:
            raise ValueError("Categoria DFP não encontrada")
        option.first.click()
