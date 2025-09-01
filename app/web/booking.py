# app/blueprints/booking.py
from datetime import date, timedelta
from flask import Blueprint, render_template
from flask_login import login_required
from sqlalchemy import text as _sql
from ..extensions import db

bp = Blueprint("booking", __name__, url_prefix="")

def next_saturday(d: date) -> date:
    delta = (5 - d.weekday()) % 7  # 5 = sabato
    return d + timedelta(days=delta or 7)

def get_airports_list():
    return [
        ("", "Tutti gli aeroporti"),
        ("BRI","Bari"),("BGY","Bergamo"),("BLQ","Bologna"),("CTA","Catania"),
        ("FLR","Firenze"),("MXP","Milano Malpensa"),("NAP","Napoli"),
        ("PMO","Palermo"),("PSA","Pisa"),("CIA","Roma Ciampino"),
        ("FCO","Roma Fiumicino"),("TRN","Torino"),("TSF","Treviso"),
        ("VCE","Venezia"),("VRN","Verona"),
    ]

@bp.get("/booking")
@login_required
def booking_form():
    # 1) Leggi le destinazioni dalla tabella 'destinations'
    rows = db.session.execute(_sql("""
        SELECT code, label
        FROM destinations
        WHERE code IS NOT NULL AND TRIM(code) <> ''
        ORDER BY label, code
    """)).fetchall()
    destinations = [(r.code, r.label) for r in rows]

    # 2) (fallback) se la tabella fosse vuota, derivale da ota_product
    if not destinations:
        rows = db.session.execute(_sql("""
            SELECT DISTINCT
                   UPPER(TRIM(city_code)) AS code,
                   COALESCE(NULLIF(UPPER(TRIM(area_id)), ''), UPPER(TRIM(city_code))) AS label
            FROM ota_product
            WHERE city_code IS NOT NULL AND TRIM(city_code) <> ''
            ORDER BY label, code
        """)).fetchall()
        destinations = [(r.code, r.label) for r in rows]

    return render_template(
        "booking/booking.html",   # <-- verifica che il path del tuo template sia questo
        airports=get_airports_list(),
        destinations=destinations,
        default_depart=next_saturday(date.today()).isoformat(),
    )
