from dataclasses import dataclass
from typing import Tuple
from sqlalchemy.exc import OperationalError
from ..extensions import db
from ..models import SettingOTA
from ..utils import normalize_base_url

@dataclass
class RuntimeConfig:
    base_url: str
    requestor_id: str
    requestor_password: str
    chain_code: str
    departure: str
    target: str
    primary_lang_id: str
    market_country_code: str
    bearer_token: str = ""
    default_category: str = "211"
    default_los: Tuple[int, int] = (7, 14)
    timeout_seconds: int = 40
    basic_user: str = ""
    basic_pass: str = ""

def get_setting_safe() -> SettingOTA:
    try:
        s = SettingOTA.query.first()
    except OperationalError as e:
        msg = str(getattr(e, "orig", e))
        if "no such column" in msg and "setting_ota" in msg:
            from ..models import ensure_setting_columns
            ensure_setting_columns()
            s = SettingOTA.query.first()
        else:
            raise
    if not s:
        s = SettingOTA()
        db.session.add(s)
        db.session.commit()
    return s

def get_runtime_config() -> RuntimeConfig:
    s = get_setting_safe()
    return RuntimeConfig(
        base_url           = normalize_base_url(s.base_url),
        requestor_id       = s.requestor_id,
        requestor_password = s.message_password,
        chain_code         = s.chain_code,
        departure          = s.departure_default,
        target             = s.target or "Production",
        primary_lang_id    = s.primary_lang or "it",
        market_country_code= s.market_country_code or "it",
        bearer_token       = s.bearer_token or "",
        default_category   = s.category_code or "211",
        default_los        = (s.los_min or 7, s.los_max or 14),
        timeout_seconds    = s.timeout_seconds or 40,
        basic_user         = s.basic_user or "",
        basic_pass         = s.basic_pass or "",
    )

def get_api_headers(cfg: RuntimeConfig) -> dict:
    headers = {
        "Content-Type": "application/xml; charset=utf-8",
        "Accept": "application/xml",
    }
    if getattr(cfg, "bearer_token", ""):
        headers["Authorization"] = f"Bearer {cfg.bearer_token}"
    return headers
