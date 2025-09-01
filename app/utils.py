import re
from lxml import etree as ET


def normalize_base_url(url: str) -> str:
    url = (url or "").strip()
    return url.rstrip("/")


def pretty_xml(xml_bytes: bytes) -> str:
    parser = ET.XMLParser(remove_blank_text=True, recover=True)
    root = ET.fromstring(xml_bytes, parser=parser)
    return ET.tostring(root, pretty_print=True, encoding="unicode")


def tac_city(tac: str) -> str:
    """Estrai il codice città (3 lettere) da un TAC (es: 2024MIL#VCE)."""
    m = re.match(r"^\d{4}([A-Z]{3})", tac or "")
    return m.group(1) if m else ""


def tac_dep(tac: str) -> str:
    """Estrai il codice aeroporto di partenza (dopo #)."""
    m = re.search(r"#([A-Z]{3})", tac or "")
    return m.group(1) if m else ""
