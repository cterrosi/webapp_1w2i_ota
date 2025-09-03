# app/web/products.py
import os, json, html, re, requests
from markupsafe import Markup
from flask import Blueprint, render_template, request, redirect, url_for
from flask_login import login_required
from lxml import etree as ET
from requests.auth import HTTPBasicAuth

from ..extensions import db
from ..models import OTAProduct, OTAProductDetail, OTAProductMedia
from ..services.runtime import get_setting_safe
from ..services.ota_endpoints import build_endpoint, build_descriptive_endpoint
from ..services.ota_detail import merge_detail_with_row, is_meaningful_detail
from ..services import parse_products
from ..services.ota_io import (
    build_ota_product_request,
    build_ota_descriptive_by_code_request,
    build_ota_product_request_by_code,
    parse_ota_descriptive_detail,
    product_dict_to_detail,
    build_availability_xml_from_product,
    parse_availability_xml,
)
from ..settings import DEBUG_DIR

bp = Blueprint("products", __name__)

def _save_detail_only(product_id: int, detail: dict):
    rec = db.session.query(OTAProductDetail).filter_by(product_id=product_id).one_or_none()
    payload = dict(
        product_id=product_id,
        name=(detail.get("name") or "").strip(),
        duration=(detail.get("duration") or "").strip(),
        city=(detail.get("city") or "").strip(),
        country=(detail.get("country") or "").strip(),
        categories_json=json.dumps(detail.get("categories") or []),
        types_json=json.dumps(detail.get("types") or []),
        descriptions_json=json.dumps(detail.get("descriptions") or []),
        pickup_notes_json=json.dumps(detail.get("pickup_notes") or []),
    )
    if rec:
        for k, v in payload.items():
            setattr(rec, k, v)
    else:
        db.session.add(OTAProductDetail(**payload))
    db.session.commit()


def _normalize_base_url(url: str) -> str:
    return (url or "").rstrip("/")

def _build_avail_endpoint(base_url: str) -> str:
    base = _normalize_base_url(base_url)
    return base if base.lower().endswith("/touractivityavail") else base + "/TourActivityAvail"

def _pretty_xml(xml_bytes: bytes) -> str:
    try:
        parser = ET.XMLParser(remove_blank_text=True, recover=True)
        root = ET.fromstring(xml_bytes, parser=parser)
        return ET.tostring(root, pretty_print=True, encoding="unicode")
    except Exception:
        try:
            return (xml_bytes.decode("utf-8", errors="ignore")
                    if isinstance(xml_bytes, (bytes, bytearray)) else str(xml_bytes))
        except Exception:
            return str(xml_bytes)

def _save_detail_and_media(product_id: int, detail: dict):
    # detail -> OTAProductDetail
    db.session.query(OTAProductDetail).filter_by(product_id=product_id).delete()
    db.session.add(OTAProductDetail(
        product_id=product_id,
        name=(detail.get("name") or "").strip(),
        duration=(detail.get("duration") or "").strip(),
        city=(detail.get("city") or "").strip(),
        country=(detail.get("country") or "").strip(),
        categories_json=json.dumps(detail.get("categories") or []),
        types_json=json.dumps(detail.get("types") or []),
        descriptions_json=json.dumps(detail.get("descriptions") or []),
        pickup_notes_json=json.dumps(detail.get("pickup_notes") or []),
    ))
    # images -> OTAProductMedia
    db.session.query(OTAProductMedia).filter_by(product_id=product_id).delete()
    for i, u in enumerate(detail.get("image_urls") or []):
        if not u:
            continue
        db.session.add(OTAProductMedia(
            product_id=product_id,
            kind="image",
            url=u,
            sort_order=i
        ))
    db.session.commit()


@bp.post("/<int:product_id>/availability", endpoint="ota_product_availability")
@login_required
def ota_product_availability(product_id: int):
    from datetime import date

    s = get_setting_safe()
    row = db.session.get(OTAProduct, product_id)
    if not row:
        return "Prodotto non trovato", 404

    product_name = (row.tour_activity_name or "").strip()
    tac = (row.tour_activity_code or "").strip()

    start_date = (request.form.get("start_date") or "").strip()
    end_date   = (request.form.get("end_date")   or "").strip()   # <-- LO LEGGIAMO
    try:
        units = int(request.form.get("units", "2"))
    except Exception:
        units = 2

    # ---- calcolo notti dalla coppia start/end ----
    nights = 0
    if start_date and end_date:
        try:
            nights = max((date.fromisoformat(end_date) - date.fromisoformat(start_date)).days, 0)
        except Exception:
            nights = 0

    # fallback: se non riusciamo a calcolare, usa il minimo configurato
    los_min = getattr(s, "los_min", 7) or 7
    if nights <= 0:
        nights = los_min

    # ---- parametri OTA ----
    target              = s.target or "Production"
    primary_lang_id     = s.primary_lang or "it"
    market_country_code = s.market_country_code or "it"
    chain_code          = s.chain_code or "SANDTOUR"
    product_type        = s.product_type or "Tour"
    category_code       = (row.category_code or s.category_code or "211").strip()
    city_code           = (row.city_code or "").strip()

    if not city_code and tac:
        m = re.match(r'^\d{4}([A-Z]{3})', tac)
        if m:
            city_code = m.group(1)

    # departure dall’hashtag del TAC, poi default
    dep_loc = ""
    if tac:
        m = re.search(r'#([A-Z]{3})', tac)
        if m:
            dep_loc = m.group(1)
    if not dep_loc:
        dep_loc = getattr(s, "departure_default", "") or "VCE"

    # ✅ una sola durata
    lengths_of_stay = (nights,)

    req_xml = build_availability_xml_from_product(
        requestor_id=s.requestor_id,
        message_password=s.message_password,
        chain_code=chain_code,
        product_type=product_type,
        category_code=category_code,
        city_code=city_code,
        departure_loc=dep_loc,
        start_date=start_date,
        units=units,
        lengths_of_stay=lengths_of_stay,
        tour_activity_code=tac or None,
        target=target,
        primary_lang_id=primary_lang_id,
        market_country_code=market_country_code,
    )

    url = _build_avail_endpoint(s.base_url)
    headers = {"Content-Type": "application/xml; charset=utf-8", "Accept": "application/xml"}
    auth = None
    if s.bearer_token:
        headers["Authorization"] = f"Bearer {s.bearer_token}"
    elif s.basic_user and s.basic_pass:
        auth = HTTPBasicAuth(s.basic_user, s.basic_pass)

    try:
        r = requests.post(
            url,
            data=req_xml.encode("utf-8"),
            headers=headers,
            timeout=(s.timeout_seconds or 40),
            auth=auth,
        )
        raw_res = r.content
        avail = parse_availability_xml(raw_res)
    except requests.RequestException as e:
        raw_res = b""
        avail = {"ok": False, "error_code": "CONNECTION", "error_text": str(e), "rooms": []}

    return render_template(
        "availability/availability_detail.html",
        product=row,
        product_id=product_id,
        product_name=product_name or tac or f"Product {product_id}",
        start_date=start_date,
        units=units,
        request_xml=_pretty_xml(req_xml.encode("utf-8")),
        response_xml=_pretty_xml(raw_res) if raw_res else "(no response)",
        avail=avail,
    )



@bp.route("/ota_update_products", methods=["GET", "POST"], endpoint="ota_update_products")
@login_required
def ota_update_products():
    import os
    from requests.auth import HTTPBasicAuth
    from flask import request, redirect, url_for, current_app

    current_app.logger.info(">>> Import request args: %s", request.args.to_dict())
    print(">>> Import request args:", request.args.to_dict())

    s = get_setting_safe()
    if not s:
        return "⚠️ Configurazione OTA mancante", 400

    # params
    debug_flag = (str(request.args.get("debug", "")).lower() in ("1", "true", "yes", "on"))
    try:
        limit = int(request.args.get("limit", "0") or 0)
    except Exception:
        limit = 0

    # EAGER defaults: attiva sempre immagini; dettagli opzionali (mettilo True se vuoi)
    fill_images  = True
    fill_details = False
    # tetto predefinito alle DESCRIPTIVE per non sovraccaricare
    try:
        maxcores = int(request.args.get("maxcores", "120") or 120)
    except Exception:
        maxcores = 120

    # build request
    rq_xml = build_ota_product_request(s)
    url = build_endpoint(s.base_url)
    headers = {"Content-Type": "application/xml; charset=utf-8", "Accept": "application/xml"}
    auth = None
    if (s.bearer_token or "").strip():
        headers["Authorization"] = f"Bearer {s.bearer_token.strip()}"
    elif (s.basic_user and s.basic_pass):
        auth = HTTPBasicAuth(s.basic_user, s.basic_pass)

    print(f"[PRODUCTS] POST URL: {url}", flush=True)

    if debug_flag:
        dbgdir = "/app/data/debug"
        os.makedirs(dbgdir, exist_ok=True)
        with open(os.path.join(dbgdir, "PRODUCTS_RQ.xml"), "wb") as f:
            f.write(rq_xml or b"")

    print(">>> REQUEST URL:", url, flush=True)
    print(">>> REQUEST HEADERS:", headers, flush=True)
    try:
        print(">>> REQUEST PAYLOAD:\n", rq_xml.decode("utf-8"), flush=True)
    except Exception:
        print(">>> REQUEST PAYLOAD (non-text):", rq_xml[:200], flush=True)

    # HTTP (singola)
    resp = requests.post(url, data=rq_xml, headers=headers, timeout=(s.timeout_seconds or 60), auth=auth)
    print(f"[PRODUCTS] HTTP STATUS: {resp.status_code}", flush=True)

    if debug_flag:
        with open(os.path.join(dbgdir, "PRODUCTS_RS.xml"), "wb") as f:
            f.write(resp.content or b"")

    if resp.status_code != 200:
        snippet = (resp.text or "")[:1000]
        print(f"[PRODUCTS][ERR] HTTP {resp.status_code} body: {snippet}", flush=True)
        return f"Errore HTTP {resp.status_code}<br><pre>{snippet}</pre>", resp.status_code

    # parse prodotti
    all_products = parse_products.ota_products(resp.content)
    total_in_rs = len(all_products)
    products = all_products[:limit] if (limit and total_in_rs) else all_products
    print(f"[PRODUCTS] parsed={total_in_rs} selected={len(products)} (limit={limit})", flush=True)

    # wipe coerente
    db.session.query(OTAProductMedia).delete()
    db.session.query(OTAProductDetail).delete()
    db.session.query(OTAProduct).delete()

    # insert
    for p in products:
        db.session.add(OTAProduct(
            tour_activity_code = p.get("TourActivityCode", ""),
            tour_activity_name = p.get("TourActivityName", ""),
            city_code          = p.get("TourActivityCityCode", ""),
            area_id            = p.get("AreaID", ""),
            country_iso        = p.get("CountryISOCode", ""),
            country_name       = p.get("CountryName", ""),
            product_type       = p.get("ProductType", ""),
            product_type_code  = p.get("ProductTypeCode", ""),
            product_type_name  = p.get("ProductTypeName", ""),
            category_code      = p.get("CategoryCode", ""),
            category_detail    = p.get("CategoryCodeDetail", ""),
        ))
    db.session.commit()
    print(f"[PRODUCTS] imported={len(products)} (total_in_RS={total_in_rs})", flush=True)

    # ------------------ EAGER FILL: immagini (e dettagli se abilitati) ------------------
    if fill_images or fill_details:
        def _core_code(tac: str) -> str:
            tac = (tac or "").strip()
            return tac.split("#", 1)[0] if "#" in tac else tac

        # mappa core -> [product_id...]
        prods_after = OTAProduct.query.with_entities(OTAProduct.id, OTAProduct.tour_activity_code).all()
        cores = {}
        for pid, tac in prods_after:
            cores.setdefault(_core_code(tac), []).append(pid)

        url_d = build_descriptive_endpoint(s.base_url)
        created_media = created_detail = 0

        print(f"[PRODUCTS][EAGER] start: cores={len(cores)}, maxcores={maxcores}, fill_images={fill_images}, fill_details={fill_details}", flush=True)

        for idx, (core, product_ids) in enumerate(cores.items(), start=1):
            if maxcores and idx > maxcores:
                break
            try:
                rq_xml_d = build_ota_descriptive_by_code_request(s, core)
                resp_d = requests.post(url_d, data=rq_xml_d, headers=headers, auth=auth, timeout=60)
                if resp_d.status_code != 200:
                    print(f"[PRODUCTS][EAGER] core={core} -> HTTP {resp_d.status_code}", flush=True)
                    continue

                detail = parse_ota_descriptive_detail(resp_d.content) or {}
                # unisci con info prodotto "rappresentativa"
                rep_row = db.session.get(OTAProduct, product_ids[0])
                merged = merge_detail_with_row(detail, rep_row)

                # se stiamo solo cercando immagini e non ce ne sono, salta
                if fill_images and not (merged.get("image_urls") or []):
                    print(f"[PRODUCTS][EAGER] core={core} no images", flush=True)
                    if not fill_details:
                        continue

                for pid in product_ids:
                    to_save = dict(merged)
                    if not fill_details:
                        to_save.update({
                            "name": "", "duration": "", "city": "", "country": "",
                            "categories": [], "types": [], "descriptions": [], "pickup_notes": [],
                        })
                    if not fill_images:
                        to_save["image_urls"] = []

                    _save_detail_and_media(pid, to_save)
                    if fill_details:
                        created_detail += 1
                    if fill_images:
                        created_media += len(to_save.get("image_urls") or [])

                if (idx % 25) == 0:
                    db.session.commit()

            except Exception as e:
                print(f"[PRODUCTS][EAGER] core={core} error: {e}", flush=True)

        db.session.commit()
        print(f"[PRODUCTS][EAGER] done: details={created_detail}, media_rows={created_media}", flush=True)

    return redirect(url_for("products.ota_products"))



# --- helper: persiste DETAIL + MEDIA per un prodotto ---------------------------------
def _save_detail_and_media(product_id: int, detail: dict):
    """Persisti OTAProductDetail e OTAProductMedia per il product_id."""
    # dettaglio
    db.session.query(OTAProductDetail).filter_by(product_id=product_id).delete()
    db.session.add(OTAProductDetail(
        product_id=product_id,
        name=(detail.get("name") or "").strip(),
        duration=(detail.get("duration") or "").strip(),
        city=(detail.get("city") or "").strip(),
        country=(detail.get("country") or "").strip(),
        categories_json=json.dumps(detail.get("categories") or []),
        types_json=json.dumps(detail.get("types") or []),
        descriptions_json=json.dumps(detail.get("descriptions") or []),
        pickup_notes_json=json.dumps(detail.get("pickup_notes") or []),
    ))
    # media
    db.session.query(OTAProductMedia).filter_by(product_id=product_id).delete()
    for i, u in enumerate(detail.get("image_urls") or []):
        if not u:
            continue
        db.session.add(OTAProductMedia(
            product_id=product_id,
            kind="image",
            url=u,
            sort_order=i
        ))
    db.session.commit()
# -------------------------------------------------------------------------------------


@bp.route("/ota_products", methods=["GET"], endpoint="ota_products")
@login_required
def ota_products():
    prods = OTAProduct.query.order_by(OTAProduct.tour_activity_name.asc()).all()
    media_rows = (db.session.query(OTAProductMedia)
                  .order_by(OTAProductMedia.product_id.asc(), OTAProductMedia.sort_order.asc(), OTAProductMedia.id.asc())
                  .all())
    first_img = {}
    for m in media_rows:
        if m.product_id not in first_img and (m.kind or "image") == "image":
            first_img[m.product_id] = m.url

    def core_code(tac: str) -> str:
        tac = (tac or "").strip()
        return tac.split("#", 1)[0] if "#" in tac else tac

    def dep_from_tac(tac: str) -> str:
        m = re.search(r"#([A-Z]{3})", tac or "")
        return m.group(1) if m else ""

    groups = {}
    for p in prods:
        core = core_code(p.tour_activity_code)
        dep  = dep_from_tac(p.tour_activity_code)
        g = groups.setdefault(core, {
            "core": core,
            "name": p.tour_activity_name,
            "city": p.city_code,
            "country": f"{p.country_iso or ''} {p.country_name or ''}".strip(),
            "items": [],
            "image": None,
        })
        if not g["image"]:
            g["image"] = first_img.get(p.id)

        g["items"].append({
            "id": p.id,
            "name": p.tour_activity_name,
            "code": p.tour_activity_code,
            "dep": dep,
            "image": first_img.get(p.id),
        })

    # groups è un dict: { core -> { "core":..., "name":..., "items":[...] } }

    groups_list = sorted(groups.values(), key=lambda x: (x.get("name") or x.get("core") or ""))

    total_groups = len(groups_list)
    total_items  = sum(len(g.get("items", [])) for g in groups_list)

    return render_template(
        "products/ota_products_grouped.html",
        groups=groups_list,          # <--- passa la LISTA al template
        total_groups=total_groups,
        total_items=total_items,
)

@bp.route("/ota_products/<int:product_id>", methods=["GET"], endpoint="ota_product_detail")
@login_required
def ota_product_detail(product_id: int):
    row = db.session.get(OTAProduct, product_id)
    if not row:
        return "Prodotto non trovato", 404

    s = get_setting_safe()

    # Headers + Auth (Bearer o Basic)
    headers = {"Content-Type": "application/xml; charset=utf-8", "Accept": "application/xml"}
    auth = None
    if getattr(s, "bearer_token", None):
        headers["Authorization"] = f"Bearer {s.bearer_token}"
    elif getattr(s, "basic_user", None) and getattr(s, "basic_pass", None):
        auth = HTTPBasicAuth(s.basic_user, s.basic_pass)

    debug_mode = request.args.get("debug") == "1"
    debug_dir = DEBUG_DIR if debug_mode else None

    def _save_dbg(code: str, label: str, resp):
        if not debug_mode:
            return
        try:
            safe = (code or "UNK").replace(os.sep, "_").replace("#", "_")
            pth = os.path.join(debug_dir, f"{safe}_{label}.xml")
            with open(pth, "wb") as f:
                f.write(resp.content or b"")
            print(f"[DBG] {label} status: {resp.status_code} saved: {pth}", flush=True)
        except Exception as e:
            print(f"[DBG] save error ({label}):", e, flush=True)

    # Se ho già cache e non chiedo refresh, uso quella (riempio immagini se mancano)
    rec = OTAProductDetail.query.filter_by(product_id=product_id).first()
    if rec and request.args.get("refresh") != "1":
        descs_raw = json.loads(rec.descriptions_json or "[]")
        descriptions = [Markup(html.unescape(x or "")) for x in descs_raw]
        image_urls = [m.url for m in OTAProductMedia.query
                      .filter_by(product_id=product_id)
                      .order_by(OTAProductMedia.sort_order.asc()).all()]
        if not image_urls:
            codes_to_try = [row.tour_activity_code]
            if "#" in (row.tour_activity_code or ""):
                codes_to_try.append(row.tour_activity_code.split("#", 1)[0])
            for code_try in codes_to_try:
                try:
                    rq_xml_d = build_ota_descriptive_by_code_request(s, code_try)
                    url_d = build_descriptive_endpoint(s.base_url)
                    print("DETAIL POST URL (DESCRIPTIVE IMG-FILL):", url_d, "code:", code_try, flush=True)
                    resp_d = requests.post(url_d, data=rq_xml_d, headers=headers, auth=auth, timeout=60)
                    _save_dbg(code_try, "DESCR_IMG", resp_d)
                    if resp_d.status_code == 200:
                        detail_tmp = parse_ota_descriptive_detail(resp_d.content)
                        if detail_tmp.get("image_urls"):
                            image_urls = detail_tmp["image_urls"]
                            break
                except Exception as e:
                    print("[detail] DESCR IMG-FILL error:", code_try, e, flush=True)

        detail = {
            "name": rec.name,
            "duration": rec.duration,
            "city": rec.city,
            "country": rec.country,
            "categories": json.loads(rec.categories_json or "[]"),
            "types": json.loads(rec.types_json or "[]"),
            "descriptions": descriptions,
            "pickup_notes": json.loads(rec.pickup_notes_json or "[]"),
            "image_urls": image_urls,
        }
        # 👉 URL calcolato QUI (niente url_for nei servizi)
        detail["quote_url"] = url_for("availability.availability_quote", product_id=product_id)
        return render_template("products/ota_product_detail.html", p=row, detail=detail)

    # --- Provo DESCRIPTIVE con TAC intero e "core" (senza #) ---
    codes_to_try = [row.tour_activity_code]
    if "#" in (row.tour_activity_code or ""):
        codes_to_try.append(row.tour_activity_code.split("#", 1)[0])

    for code_try in codes_to_try:
        try:
            rq_xml_d = build_ota_descriptive_by_code_request(s, code_try)
            url_d = build_descriptive_endpoint(s.base_url)
            print("DETAIL POST URL (DESCRIPTIVE):", url_d, "code:", code_try, flush=True)
            resp_d = requests.post(url_d, data=rq_xml_d, headers=headers, auth=auth, timeout=60)
            _save_dbg(code_try, "DESCR", resp_d)

            if resp_d.status_code == 200:
                detail = parse_ota_descriptive_detail(resp_d.content)
                if is_meaningful_detail(detail):
                    detail = merge_detail_with_row(detail, row)
                    detail["descriptions"] = [Markup(html.unescape(x or "")) for x in detail.get("descriptions", [])]
                    # 👉 URL calcolato QUI
                    detail["quote_url"] = url_for("availability.availability_quote", product_id=product_id)
                    return render_template("products/ota_product_detail.html", p=row, detail=detail)
        except Exception as e:
            print("[detail] DESCRIPTIVE call error:", code_try, e, flush=True)

    # --- PRODUCT FALLBACK: con e senza city_code ---
    try:
        def _product_call_with_code(code_try: str, no_city: bool = False):
            if no_city and getattr(s, "city_code", None):
                old_city = s.city_code
                try:
                    s.city_code = ""  # disabilita temporaneamente il filtro città
                    rq_xml_p = build_ota_product_request_by_code(s, code_try)
                finally:
                    s.city_code = old_city
            else:
                rq_xml_p = build_ota_product_request_by_code(s, code_try)

            url_p = build_endpoint(s.base_url)
            print("DETAIL POST URL (PRODUCT-FALLBACK):", url_p, "code:", code_try, "no_city:", no_city, flush=True)
            resp_p = requests.post(url_p, data=rq_xml_p, headers=headers, auth=auth, timeout=60)
            _save_dbg(code_try, "PROD_NOCITY" if no_city else "PROD", resp_p)

            if resp_p.status_code == 200:
                prods = parse_ota_products(resp_p.content)
                if prods:
                    d = product_dict_to_detail(prods[0])
                    d = merge_detail_with_row(d, row)
                    # 👉 URL calcolato QUI
                    d["quote_url"] = url_for("availability.availability_quote", product_id=product_id)
                    return d
            return None

        # 1) con city_code
        for code_try in codes_to_try:
            d = _product_call_with_code(code_try, no_city=False)
            if d:
                return render_template("products/ota_product_detail.html", p=row, detail=d)

        # 2) senza city_code
        for code_try in codes_to_try:
            d = _product_call_with_code(code_try, no_city=True)
            if d:
                return render_template("products/ota_product_detail.html", p=row, detail=d)

    except Exception as e:
        print("[detail] PRODUCT fallback call error:", e, flush=True)

    return "Product details not available (empty descriptive calls).", 502


