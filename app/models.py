from datetime import datetime
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy.orm import relationship
from .extensions import db

# =========================
# MODELS
# =========================

class User(UserMixin, db.Model):
    __tablename__ = "user"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)

    def set_password(self, raw: str):
        self.password_hash = generate_password_hash(raw, method="pbkdf2:sha256")

    def check_password(self, raw: str) -> bool:
        return check_password_hash(self.password_hash, raw)


class SettingOTA(db.Model):
    __tablename__ = "setting_ota"

    id = db.Column(db.Integer, primary_key=True)

    # endpoint/identità OTA
    base_url = db.Column(db.String(300), nullable=False, default="http://api.1way2italy.it/Service/Production/v10/OtaService")
    target = db.Column(db.String(20), nullable=False, default="Production")
    primary_lang = db.Column(db.String(10), nullable=False, default="en")
    requestor_id = db.Column(db.String(50), nullable=False, default="SANDT-IT")
    message_password = db.Column(db.String(100), nullable=False, default="Prt345Xdt2R")
    chain_code = db.Column(db.String(50), nullable=False, default="SANDTOUR")
    market_country_code = db.Column(db.String(5), nullable=False, default="it")

    # filtri di prodotto
    product_type = db.Column(db.String(20), nullable=False, default="Tour")
    category_code = db.Column(db.String(20), nullable=False, default="211")
    city_code = db.Column(db.String(10), nullable=True, default="")
    tour_activity_code = db.Column(db.String(100), nullable=True, default="")

    # runtime
    bearer_token = db.Column(db.String(500), nullable=True, default="")
    basic_user = db.Column(db.String(200), nullable=True, default="")
    basic_pass = db.Column(db.String(200), nullable=True, default="")
    timeout_seconds = db.Column(db.Integer, nullable=False, default=40)
    departure_default = db.Column(db.String(10), nullable=False, default="VCE")
    los_min = db.Column(db.Integer, nullable=False, default=7)
    los_max = db.Column(db.Integer, nullable=False, default=14)


class OTAProduct(db.Model):
    __tablename__ = "ota_product"

    id = db.Column(db.Integer, primary_key=True)
    tour_activity_code = db.Column(db.String(120), index=True)
    tour_activity_name = db.Column(db.String(300))
    city_code = db.Column(db.String(20))
    area_id = db.Column(db.String(50))
    country_iso = db.Column(db.String(5))
    country_name = db.Column(db.String(80))
    product_type = db.Column(db.String(20))
    product_type_code = db.Column(db.String(50))
    product_type_name = db.Column(db.String(80))
    category_code = db.Column(db.String(20))
    category_detail = db.Column(db.String(120))

    # relazioni
    detail = relationship("OTAProductDetail", uselist=False, backref="product")
    media  = relationship("OTAProductMedia", backref="product", cascade="all, delete-orphan")


class OTAProductDetail(db.Model):
    __tablename__ = "ota_product_detail"

    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey("ota_product.id"), unique=True, index=True, nullable=False)
    name = db.Column(db.String(300), default="")
    duration = db.Column(db.String(120), default="")
    city = db.Column(db.String(50), default="")
    country = db.Column(db.String(80), default="")
    categories_json = db.Column(db.Text, default="[]")
    types_json = db.Column(db.Text, default="[]")
    descriptions_json = db.Column(db.Text, default="[]")
    pickup_notes_json = db.Column(db.Text, default="[]")
    policies_json = db.Column(db.Text, default="[]")
    contacts_json = db.Column(db.Text, default="[]")
    included_html = db.Column(db.Text)
    excluded_html = db.Column(db.Text)
    notes_html    = db.Column(db.Text)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class OTAProductMedia(db.Model):
    __tablename__ = "ota_product_media"

    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey("ota_product.id"), index=True, nullable=False)
    url = db.Column(db.Text, nullable=False)
    kind = db.Column(db.String(50), default="image")   # image / video / other
    caption = db.Column(db.String(300), default="")
    sort_order = db.Column(db.Integer, default=0)


# =========================
# MIGRAZIONE IDEMPOTENTE
# =========================

def ensure_setting_columns():
    """
    Aggiunge le colonne nuove su setting_ota se mancanti (idempotente).
    Utile in SQLite per evitare migrazioni manuali.
    """
    engine = db.engine
    with engine.begin() as conn:
        tables = {r[0] for r in conn.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if "setting_ota" not in tables:
            return

        cols = {r[1] for r in conn.exec_driver_sql("PRAGMA table_info(setting_ota)").fetchall()}

        def add(coldef: str):
            conn.exec_driver_sql(f"ALTER TABLE setting_ota ADD COLUMN {coldef}")

        if "market_country_code" not in cols: add("market_country_code TEXT DEFAULT 'it' NOT NULL")
        if "bearer_token"        not in cols: add("bearer_token TEXT DEFAULT ''")
        if "basic_user"          not in cols: add("basic_user TEXT DEFAULT ''")
        if "basic_pass"          not in cols: add("basic_pass TEXT DEFAULT ''")
        if "timeout_seconds"     not in cols: add("timeout_seconds INTEGER DEFAULT 40 NOT NULL")
        if "departure_default"   not in cols: add("departure_default TEXT DEFAULT 'VCE' NOT NULL")
        if "los_min"             not in cols: add("los_min INTEGER DEFAULT 7 NOT NULL")
        if "los_max"             not in cols: add("los_max INTEGER DEFAULT 14 NOT NULL")
