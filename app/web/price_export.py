# app/web/price_export.py
from __future__ import annotations
from datetime import date, timedelta, datetime
from typing import List
from flask import Blueprint, render_template, request, Response, current_app, jsonify, make_response
from flask_login import login_required
from sqlalchemy import text as _sql, or_
from decimal import Decimal, ROUND_HALF_UP

import csv, io
import json
from calendar import monthrange

try:
    import xlsxwriter
    HAS_XLSX = True
except Exception:
    HAS_XLSX = False

from ..extensions import db
from ..models import OTAProduct, OTAProductDetail
from ..services.runtime import get_setting_safe
from ..services.ota_io import post_ota_xml
from ..services import ota_xml as otax

bp = Blueprint("price_export", __name__, url_prefix="/price_export")

# ---- Preset occupazioni ----
OCCUPANCY_PRESETS = [
    {"label": "1",                    "adults": 1, "children_ages": []},
    {"label": "2",                    "adults": 2, "children_ages": []},
    {"label": "3",                    "adults": 3, "children_ages": []},
    {"label": "2 adulti e 1 bambino", "adults": 2, "children_ages": [8]},
]

def _cfg():
    return get_setting_safe()

# ------- URL helpers (fix anti-duplicazione) -------
def _build_ota_url(s, method: str) -> str:
    base = (s.base_url or "").rstrip("/")
    if "OtaService" in base:
        return f"{base}/{method}"
    return f"{base}/Service/{s.target or 'Production'}/v10/OtaService/{method}"

def _avail_url_for(s) -> str:
    return _build_ota_url(s, "TourActivityAvail")

def _quote_url_for(s) -> str:
    return _build_ota_url(s, "TourActivityQuote")

def _airport_label(apt_code: str) -> str:
    mapping = {
        "FCO": "Roma Fiumicino",
        "CIA": "Roma Ciampino",
        "MXP": "Milano Malpensa",
        "LIN": "Milano Linate",
        "BGY": "Bergamo",
        "BLQ": "Bologna",
        "VRN": "Verona",
        "VCE": "Venezia",
        "TSF": "Treviso",
        "PSA": "Pisa",
        "FLR": "Firenze",
        "TRN": "Torino",
        "GOA": "Genova",
        "NAP": "Napoli",
        "BRI": "Bari",
        "CTA": "Catania",
        "PMO": "Palermo",
        "OLB": "Olbia",
        "AHO": "Alghero",
        "CAG": "Cagliari",
        "TRS": "Trieste",
        "PSR": "Pescara",
    }
    base = "".join([c for c in (apt_code or "") if not c.isdigit()])  # es. "MXP2" -> "MXP"
    return mapping.get(base, base or "")

def _format_it_range(depart_date: str, duration_days: int) -> str:
    y, m, d = map(int, depart_date.split("-"))
    start = date(y, m, d)
    end = start + timedelta(days=duration_days)
    return f"Dal {start.strftime('%d/%m/%Y')} al {end.strftime('%d/%m/%Y')}"

def _iter_departures_for_product(product_code_base: str, date_from: str | None = None, date_to: str | None = None):
    params = {"base": f"{product_code_base}#%"}
    where = "product_code LIKE :base"
    if date_from:
        where += " AND depart_date >= :dfrom"
        params["dfrom"] = date_from
    if date_to:
        where += " AND depart_date <= :dto"
        params["dto"] = date_to
    sql = f"""
        SELECT
          depart_date,
          duration_days,
          COALESCE(depart_airport, SUBSTR(product_code, INSTR(product_code, '#')+1)) AS apt
        FROM departures_cache
        WHERE {where}
        ORDER BY depart_date ASC, apt ASC, duration_days ASC
    """
    for row in db.session.execute(_sql(sql), params).mappings():
        depart = row.get("depart_date")
        depart_str = depart.strftime("%Y-%m-%d") if isinstance(depart, (date, datetime)) else str(depart)[:10]
        duration = int(row.get("duration_days") or 0)
        apt = (row.get("apt") or "").strip()
        yield depart_str, duration, apt

def _min_price_from_rooms(rooms: List[dict]) -> tuple[str, float | None, dict | None]:
    best = None
    cur = ""
    best_room = None
    for r in rooms or []:
        amt = (r.get("price") or "").strip()
        ccy = r.get("currency") or ""
        try:
            val = float(str(amt).replace(",", "."))
        except Exception:
            continue
        if best is None or val < best:
            best, cur, best_room = val, ccy, r
    return (cur or "EUR"), best, best_room

def _make_guests(adults: int, children_ages: list[int]) -> list[dict]:
    out = []
    rph = 1
    today = date.today()
    for _ in range(max(0, adults)):
        bd = date(today.year - 35, today.month, min(today.day, 28)).isoformat()
        out.append({"rph": rph, "birthdate": bd, "given": "Adult", "surname": f"A{rph}", "email": "x@example.com"})
        rph += 1
    for age in children_ages or []:
        bd = date(today.year - int(age), today.month, min(today.day, 28)).isoformat()
        out.append({"rph": rph, "birthdate": bd, "given": "Child", "surname": f"C{rph}", "email": "x@example.com"})
        rph += 1
    return out

def _find_product_by_code_base(code_base: str):
    if not code_base:
        return None
    return (
        db.session.query(OTAProduct, OTAProductDetail)
        .join(OTAProductDetail, OTAProductDetail.product_id == OTAProduct.id, isouter=True)
        .filter(OTAProduct.tour_activity_code.like(f"{code_base}#%"))
        .order_by(OTAProductDetail.name.asc(), OTAProduct.tour_activity_name.asc())
        .first()
    )

def _find_product_by_name(q: str):
    if not q:
        return None
    return (
        db.session.query(OTAProduct, OTAProductDetail)
        .join(OTAProductDetail, OTAProductDetail.product_id == OTAProduct.id, isouter=True)
        .filter(
            or_(
                OTAProductDetail.name.ilike(f"%{q}%"),
                OTAProduct.tour_activity_name.ilike(f"%{q}%"),
                OTAProduct.tour_activity_code.ilike(f"%{q}%"),
            )
        )
        .order_by(OTAProductDetail.name.asc(), OTAProduct.tour_activity_name.asc())
        .first()
    )

def _wp_id_for_base_code(base_code: str) -> int | None:
    if not base_code:
        return None
    row = db.session.execute(
        _sql("""
            SELECT ID
            FROM wp_product_master
            WHERE SKU = :sku
              AND SKU IS NOT NULL
              AND length(trim(SKU)) > 0
            LIMIT 1
        """),
        {"sku": base_code}
    ).first()
    return int(row[0]) if row and row[0] is not None else None

def _round_eur(value) -> Decimal:
    try:
        return Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0")

# -----------------------------
# Routes
# -----------------------------
@bp.route("/", methods=["GET"])
@login_required
def form():
    today = date.today()
    eom = date(today.year, today.month, monthrange(today.year, today.month)[1])
    return render_template("reports/price_export.html", today=today.isoformat(), eom=eom.isoformat())

@bp.route("/suggest", methods=["GET"])
@login_required
def suggest():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])
    rows = (
        db.session.query(OTAProduct, OTAProductDetail)
        .join(OTAProductDetail, OTAProductDetail.product_id == OTAProduct.id, isouter=True)
        .filter(
            or_(
                OTAProductDetail.name.ilike(f"%{q}%"),
                OTAProduct.tour_activity_name.ilike(f"%{q}%"),
                OTAProduct.tour_activity_code.ilike(f"%{q}%"),
            )
        )
        .order_by(OTAProductDetail.name.asc(), OTAProduct.tour_activity_name.asc())
        .limit(100)
        .all()
    )
    seen = set()
    out = []
    for prod, det in rows:
        code = prod.tour_activity_code or ""
        base = code.split("#")[0] if code else ""
        if not base or base in seen:
            continue
        seen.add(base)
        out.append(
            {
                "id": prod.id,
                "code": code,
                "code_base": base,
                "name": (det.name.strip() if det and det.name else "") or (prod.tour_activity_name or base),
                "city": prod.city_code or "",
            }
        )
    return jsonify(out[:20])

@bp.route("/run", methods=["POST"])
@login_required
def run_export():
    """
    mode:
      - preview (default): mostra tabella a video
      - download: scarica CSV/XLSX
    """
    # --- INPUT ---
    hotel_name = (request.form.get("hotel_name") or "").strip()
    hotel_code = (request.form.get("hotel_code") or "").strip()

    import re
    if hotel_code and not re.match(r"^[0-9]{4}[A-Z0-9]{4,}(?:#.*)?$", hotel_code, re.I):
        resp = make_response("Codice non valido. Seleziona dall’elenco (non il nome).", 400)
        resp.headers["Content-Type"] = "text/plain; charset=utf-8"
        return resp

    date_from  = (request.form.get("date_from") or "").strip() or None
    date_to    = (request.form.get("date_to") or "").strip() or None

    if not date_from:
        date_from = date.today().isoformat()
    if not date_to:
        dfrom = date.fromisoformat(date_from)
        eom = date(dfrom.year, dfrom.month, monthrange(dfrom.year, dfrom.month)[1])
        date_to = eom.isoformat()

    fmt        = (request.form.get("fmt") or "csv").lower()
    mode       = (request.form.get("mode") or "preview").lower()
    dl_token   = (request.form.get("dl_token") or "").strip()

    # Markup per persona (EUR) da sommare al TOTALE camera * numero_pax
    markup_str = (request.form.get("markup") or "0").strip().replace(",", ".")
    try:
        MARKUP_FLAT = Decimal(markup_str)
        if MARKUP_FLAT < 0:
            MARKUP_FLAT = Decimal("0")
    except Exception:
        MARKUP_FLAT = Decimal("0")

    def _attach_dl_cookie(resp: Response) -> Response:
        if mode == "download" and dl_token:
            resp.set_cookie(
                "dl_token", dl_token, max_age=60, path="/",
                samesite="Lax", secure=request.is_secure, httponly=True
            )
        return resp

    if fmt not in ("csv", "xlsx"):
        fmt = "csv"
    if mode not in ("preview", "download"):
        mode = "preview"

    if mode == "download" and fmt == "xlsx" and not HAS_XLSX:
        resp = make_response("XLSX non disponibile (manca xlsxwriter). Usa CSV o installa xlsxwriter.", 400)
        resp.headers["Content-Type"] = "text/plain; charset=utf-8"
        return _attach_dl_cookie(resp)

    rows_json = request.form.get("rows_json")
    rows = None
    if rows_json:
        try:
            rows = json.loads(rows_json)
            current_app.logger.info("PRICE EXPORT: using rows from preview payload (%d rows).", len(rows))
        except Exception as ex:
            current_app.logger.warning("PRICE EXPORT: invalid rows_json (%s), will recompute.", ex)
            rows = None

    if not hotel_code:
        resp = make_response("Seleziona un hotel dall’elenco: il codice interno (hotel_code) è obbligatorio.", 400)
        resp.headers["Content-Type"] = "text/plain; charset=utf-8"
        return _attach_dl_cookie(resp)

    product = detail = None
    base = hotel_code.split("#")[0]
    hit = _find_product_by_code_base(base)
    if hit:
        product, detail = hit
    else:
        resp = make_response(f"Nessun prodotto trovato per codice '{hotel_code}'.", 404)
        resp.headers["Content-Type"] = "text/plain; charset=utf-8"
        return _attach_dl_cookie(resp)

    struttura_id = product.id
    prod_code_value = product.tour_activity_code or ""
    product_code_base = prod_code_value.split("#")[0] if prod_code_value else ""

    wp_id = _wp_id_for_base_code(product_code_base)

    if not product_code_base:
        resp = make_response("Prodotto trovato ma manca tour_activity_code.", 400)
        resp.headers["Content-Type"] = "text/plain; charset=utf-8"
        return _attach_dl_cookie(resp)

    if rows is None:
        s = _cfg()
        url_av = _avail_url_for(s)
        url_q  = _quote_url_for(s)
        rows = []

        deps = list(_iter_departures_for_product(product_code_base, date_from, date_to))
        current_app.logger.info(
            "PRICE EXPORT: departures trovate per %s in [%s..%s] => %d",
            product_code_base, date_from, date_to, len(deps)
        )

        for depart_date, duration_days, apt in deps:
            period_label = _format_it_range(depart_date, duration_days)
            apt_label = _airport_label(apt)
            end_date = (date.fromisoformat(depart_date) + timedelta(days=duration_days)).isoformat()

            for occ in OCCUPANCY_PRESETS:
                label = occ["label"]
                price_val = None

                try:
                    # 1) AVAILABILITY
                    xml_av = otax.build_availability_xml_with_guests(
                        requestor_id=s.requestor_id,
                        message_password=s.message_password,
                        chain_code=s.chain_code,
                        product_type=s.product_type,
                        category_code=s.category_code,
                        city_code=getattr(s, "city_code", "") or "",
                        departure_loc=apt,
                        start_date=depart_date,
                        duration_days=duration_days,
                        tour_activity_code=product_code_base,
                        target=s.target or "Production",
                        primary_lang_id=s.primary_lang or "it",
                        market_country_code=s.market_country_code or "it",
                        adults=occ["adults"],
                        children_ages=occ["children_ages"],
                    )
                    resp_av = post_ota_xml(url_av, xml_av, settings=s, timeout=getattr(s, "timeout_seconds", 40) or 40)
                    parsed = otax.parse_availability_xml(resp_av)

                    _, best, best_room = _min_price_from_rooms(parsed.get("rooms") or [])
                    price_val = best

                    # 2) FALLBACK QUOTE (se AVAIL non ha prezzo)
                    if price_val is None:
                        candidate = best_room or {}
                        if not candidate.get("booking_code"):
                            for r in (parsed.get("rooms") or []):
                                if r.get("booking_code"):
                                    candidate = r
                                    break
                        if candidate.get("booking_code"):
                            guests = _make_guests(occ["adults"], occ["children_ages"])
                            xml_q = otax.build_quote_xml_simple(
                                requestor_id=s.requestor_id,
                                message_password=s.message_password,
                                chain_code=s.chain_code,
                                target=s.target or "Production",
                                primary_lang_id=s.primary_lang or "it",
                                market_country_code=s.market_country_code or "it",
                                booking_code=candidate["booking_code"],
                                start_date=depart_date,
                                end_date=end_date,
                                guests=guests,
                            )
                            resp_q = post_ota_xml(url_q, xml_q, settings=s, timeout=getattr(s, "timeout_seconds", 40) or 40)
                            q = otax.parse_quote_total(resp_q)
                            price_val = q.get("total")

                    current_app.logger.info(
                        "AVAIL %s | %s->%s | LOS=%s | occ=%s | rooms=%d | sample=%s",
                        product_code_base, depart_date, apt, duration_days, label,
                        len(parsed.get("rooms") or []),
                        [(r.get("price"), r.get("currency"), r.get("booking_code")) for r in (parsed.get("rooms") or [])[:3]],
                    )

                except Exception as ex:
                    current_app.logger.warning(
                        "price_export error %s %s %s [%s]: %s",
                        product_code_base, depart_date, duration_days, label, ex
                    )
                    price_val = None

                # --- Prezzo finale ESPOSTO: TOTALE camera + (MARKUP per persona * numero_pax) ---
                final_price = ""
                try:
                    if price_val is not None:
                        tot = Decimal(str(price_val))
                        pax = int(occ["adults"]) + len(occ["children_ages"] or [])
                        add_on = MARKUP_FLAT * Decimal(pax)
                        final_price = str(_round_eur(tot + add_on))
                except Exception:
                    final_price = ""

                rows.append({
                    "Id Struttura": (str(wp_id) if wp_id is not None else ""),
                    "Date partenza e arrivo": period_label,
                    "Aeroporto": apt_label,
                    "Numero Adulti e Bambini sotto i 12 anni": label,
                    "Prezzo di listino": final_price,
                })

    if mode == "preview":
        return render_template(
            "reports/price_export_preview.html",
            rows=rows,
            hotel=((detail.name if detail and detail.name else "") or (product.tour_activity_name or product.tour_activity_code or "")),
            fmt=fmt,
            hotel_code=hotel_code,
            date_from=date_from,
            date_to=date_to,
        )

    date_tag = ""
    if date_from or date_to:
        date_tag = f"_{date_from or ''}-{date_to or ''}".replace('/', '-')
    fname_base = f"listino_{product_code_base}{date_tag}"

    headers = [
        "Id Struttura",
        "Date partenza e arrivo",
        "Aeroporto",
        "Numero Adulti e Bambini sotto i 12 anni",
        "Prezzo di listino",
    ]

    if fmt == "csv":
        si = io.StringIO()
        writer = csv.DictWriter(si, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
        data = si.getvalue().encode("utf-8")

        resp = Response(
            data,
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{fname_base}.csv"',
                "Cache-Control": "no-store",
            },
        )
        return _attach_dl_cookie(resp)

    bio = io.BytesIO()
    wb = xlsxwriter.Workbook(bio, {"in_memory": True})
    ws = wb.add_worksheet("Listino")
    for c, h in enumerate(headers):
        ws.write(0, c, h)
    for i, r in enumerate(rows, start=1):
        ws.write(i, 0, r["Id Struttura"])
        ws.write(i, 1, r["Date partenza e arrivo"])
        ws.write(i, 2, r["Aeroporto"])
        ws.write(i, 3, r["Numero Adulti e Bambini sotto i 12 anni"])
        try:
            v = float(r["Prezzo di listino"]) if r["Prezzo di listino"] else None
        except Exception:
            v = None
        if v is None:
            ws.write(i, 4, r["Prezzo di listino"])
        else:
            ws.write_number(i, 4, v)
    wb.close()
    bio.seek(0)

    resp = Response(
        bio.read(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{fname_base}.xlsx"',
            "Cache-Control": "no-store",
        },
    )
    return _attach_dl_cookie(resp)


@bp.route("/download/ping", methods=["GET"], endpoint="download_ping_v2")
@login_required
def download_ping_v2():
    """
    204 quando il cookie 'dl_token' coincide col token richiesto; 202 altrimenti.
    Pulisce il cookie quando done.
    """
    token  = (request.args.get("token") or "").strip()
    cookie = request.cookies.get("dl_token") or ""
    if token and cookie == token:
        r = make_response("", 204)
        r.delete_cookie("dl_token", path="/", samesite="Lax", secure=request.is_secure)
        return r
    return ("", 202)
