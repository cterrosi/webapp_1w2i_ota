import re
from urllib.parse import urlencode
from ..utils import normalize_base_url

def build_endpoint(base_url: str) -> str:
    base = normalize_base_url(base_url)
    return base if base.lower().endswith("/touractivityproduct") else base + "/TourActivityProduct"

def build_search_endpoint(base_url: str) -> str:
    base = normalize_base_url(base_url)
    return base if base.lower().endswith("/touractivitysearch") else base + "/TourActivitySearch"

def build_descriptive_endpoint(base_url: str) -> str:
    base = normalize_base_url(base_url)
    return base if base.lower().endswith("/touractivitydescriptiveinfo") else base + "/TourActivityDescriptiveInfo"

def build_avail_endpoint(base_url: str) -> str:
    base = normalize_base_url(base_url)
    return base if base.lower().endswith("/touractivityavail") else base + "/TourActivityAvail"

def build_res_endpoint(base_url: str) -> str:
    base = normalize_base_url(base_url)
    return base if base.lower().endswith("/touractivityres") else base + "/TourActivityRes"

def build_admin_calendar_url(s) -> str:
    """
    .../AdminUtilityService/ExpoProductsCalendar?username=...&password=...&chainCode=...
    partendo da base_url (che tipicamente finisce con /OtaService).
    """
    base = normalize_base_url(s.base_url or "")
    root = re.sub(r"/OtaService(?:/.*)?$", "", base, flags=re.IGNORECASE)
    root = re.sub(r"/TourActivity(?:Product|Search|DescriptiveInfo|Avail|Res)$", "", root, flags=re.IGNORECASE)
    qs = urlencode({"username": s.requestor_id, "password": s.message_password, "chainCode": s.chain_code})
    return f"{root}/AdminUtilityService/ExpoProductsCalendar?{qs}"
