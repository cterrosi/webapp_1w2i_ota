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
from ..services.ota_endpoints import build_endpoint
from ..services.ota_detail import merge_detail_with_row  # usato nell'import
from ..services import parse_products
from ..services.ota_io import (
    build_ota_product_request,
    build_availability_xml_from_product,
    parse_availability_xml,
)
from ..settings import DEBUG_DIR

bp = Blueprint("products", __name__)

# ---------- helpers per DI TextItems (INCLUDED/NO_INCLUDED/NOTE) ----------
OTA_NS = "http://www.opentravel.org/OTA/2003/05"
_ns = {"ota": OTA_NS}

def _extract_textitems_DI(xml_bytes) -> dict:
    """Ritorna dict: {'INCLUDED': html, 'NO_INCLUDED': html, 'NOTE': html, ...}"""
    out = {}
    if not xml_bytes:
        return out
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return out
    for ti in root.findall(".//ota:TextItems/ota:TextItem", namespaces=_ns):
        sid = (ti.get("SourceID") or "").strip().upper()
        desc = ti.find("ota:Description", namespaces=_ns)
        if sid and desc is not None:
            txt = (desc.text or "").strip()
            if txt:
                out[sid] = html.unescape(txt)
    return out


def _extract_clean_descriptions_from_DI(xml_bytes) -> list[str]:
    """
    Raccoglie le <ota:Description> che NON sono dentro <ota:TextItems>,
    così escludiamo le sezioni 'Quota comprende / non comprende / Note' all'origine.
    """
    out = []
    if not xml_bytes:
        return out
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return out

    # Prendi tutte le Description che NON hanno antenati TextItems
    for d in root.findall(".//ota:Description[not(ancestor::ota:TextItems)]", namespaces=_ns):
        txt = "".join(d.itertext()).strip()
        if txt:
            out.append(txt)

    # de-dup mantenendo ordine
    seen = set()
    clean = []
    for s in out:
        if s not in seen:
            seen.add(s)
            clean.append(s)
    return clean


# ---------- helpers di persistenza (dettagli e media separati) ----------

def _save_detail_only(product_id: int, detail: dict | None, *, commit: bool = False):
    """
    Salva/aggiorna il dettaglio prodotto.
    - Aggiorna via ORM se possibile (solo colonne realmente esistenti nel model).
    - Fallback con SQL diretto se la riga non esiste ancora.
    """
    if not detail:
        return

    # colonne effettivamente mappate sul modello
    model_cols = set(OTAProductDetail.__table__.columns.keys())

    base_payload = dict(
        product_id=product_id,
        name=(detail.get("name") or "").strip(),
        duration=(detail.get("duration") or "").strip(),
        city=(detail.get("city") or "").strip(),
        country=(detail.get("country") or "").strip(),
        categories_json=json.dumps(detail.get("categories") or [], ensure_ascii=False),
        types_json=json.dumps(detail.get("types") or [], ensure_ascii=False),
        descriptions_json=json.dumps(detail.get("descriptions") or [], ensure_ascii=False),
        pickup_notes_json=json.dumps(detail.get("pickup_notes") or [], ensure_ascii=False),
    )

    extra_payload = {
        "included_html": detail.get("included_html") or None,
        "excluded_html": detail.get("excluded_html") or None,
        "notes_html":    detail.get("notes_html")    or None,
    }

    # tieni solo le chiavi che esistono nella tabella
    payload = {k: v for k, v in {**base_payload, **extra_payload}.items() if k in model_cols}

    # --- 1) Tentativo ORM
    rec = db.session.query(OTAProductDetail).filter_by(product_id=product_id).one_or_none()
    if rec:
        for k, v in payload.items():
            setattr(rec, k, v)
    else:
        db.session.add(OTAProductDetail(**payload))

    db.session.flush()  # fa emergere errori ORM subito

    # --- 2) Verifica presenza fisica
    from sqlalchemy import text as _sql
    try:
        row_exists = bool(
            db.session.execute(
                _sql("SELECT 1 FROM ota_product_detail WHERE product_id = :pid LIMIT 1"),
                {"pid": product_id}
            ).fetchone()
        )
    except Exception as e:
        print(f"[DETAIL][CHECK][ERR] {e}", flush=True)
        row_exists = True  # evita fallback se la SELECT fallisce

    if not row_exists:
        # Fallback con gestione timestamp se presenti
        try:
            if not hasattr(_save_detail_only, "_detail_cols"):
                cols = db.session.execute(_sql("PRAGMA table_info(ota_product_detail)")).fetchall()
                _save_detail_only._detail_cols = {c[1]: {"notnull": bool(c[3])} for c in cols}
            cols = getattr(_save_detail_only, "_detail_cols", {})

            extra_cols = []
            extra_vals = {}
            from datetime import datetime, timezone
            now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

            if "created_at" in cols:
                extra_cols.append("created_at"); extra_vals["created_at"] = now_iso
            if "updated_at" in cols:
                extra_cols.append("updated_at"); extra_vals["updated_at"] = now_iso

            # ripulisci eventuali avanzi
            db.session.execute(_sql("DELETE FROM ota_product_detail WHERE product_id = :pid"), {"pid": product_id})

            columns = list(payload.keys()) + extra_cols
            placeholders = ",".join([f":{k}" for k in columns])

            db.session.execute(
                _sql(f"INSERT INTO ota_product_detail ({', '.join(columns)}) VALUES ({placeholders})"),
                {**payload, **extra_vals}
            )
            db.session.flush()
        except Exception as e:
            print(f"[DETAIL][FALLBACK][ERR] product_id={product_id}: {e}", flush=True)

    if commit:
        db.session.commit()



def _replace_media_only(product_id: int, image_urls):
    db.session.query(OTAProductMedia).filter_by(product_id=product_id).delete()
    for i, u in enumerate(image_urls or []):
        if not u:
            continue
        db.session.add(OTAProductMedia(
            product_id=product_id,
            kind="image",
            url=u,
            sort_order=i
        ))
    # commit a blocchi nel chiamante


# ---------- util varie ----------

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


# ----------------- AVAILABILITY (unchanged behavior) -----------------

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
    end_date   = (request.form.get("end_date")   or "").strip()
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


# ----------------- IMPORT PRODOTTI + DETTAGLI + MEDIA -----------------

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

    # EAGER defaults: attiva sempre immagini; dettagli ON di default (disattivabile via querystring)
    fill_images  = True
    fill_details = str(request.args.get("fill_details", "1")).lower() in ("1", "true", "yes", "on")
    # tetto predefinito alle DESCRIPTIVE per non sovraccaricare
    try:
        maxcores = int(request.args.get("maxcores", "250") or 250)
    except Exception:
        maxcores = 250

    # build request
    from ..services.ota_endpoints import build_descriptive_endpoint  # import locale per evitare import inutilizzato sopra
    from ..services.ota_io import build_ota_descriptive_by_code_request, parse_ota_descriptive_detail

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

        # --- extractor locale per i TextItems del DescriptiveInfo ---
        OTA_NS = "http://www.opentravel.org/OTA/2003/05"
        _ns = {"ota": OTA_NS}

        def _extract_di_textitems(xml_bytes):
            out = {}
            if not xml_bytes:
                return out
            try:
                root = ET.fromstring(xml_bytes)
            except Exception:
                return out
            for ti in root.findall(".//ota:TextItems/ota:TextItem", namespaces=_ns):
                sid = (ti.get("SourceID") or "").strip().upper()
                desc = ti.find("ota:Description", namespaces=_ns)
                txt = (desc.text or "").strip() if desc is not None else ""
                if sid and txt:
                    out[sid] = html.unescape(txt)
            return out

        def _extract_clean_descriptions_from_DI(xml_bytes) -> list[str]:
            # Raccoglie le <ota:Description> che NON sono dentro <ota:TextItems>."""
            out = []
            if not xml_bytes:
                return out
            try:
                root = ET.fromstring(xml_bytes)
            except Exception:
                return out

            # ⚠️ Usa xpath (supporta l’asse ancestor), NON findall()
            try:
                desc_nodes = root.xpath(".//ota:Description[not(ancestor::ota:TextItems)]", namespaces=_ns)
            except Exception:
                # fallback ultra-conservativo: prendi tutte le Description (meglio che saltare tutto)
                desc_nodes = root.xpath(".//ota:Description", namespaces=_ns)

            for d in desc_nodes:
                # prendi il testo completo (anche con figli)
                txt = "".join(d.itertext()).strip()
                if txt:
                    out.append(txt)

            # de-dup preservando ordine
            seen, clean = set(), []
            for s in out:
                if s not in seen:
                    seen.add(s)
                    clean.append(s)
            return clean



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

                # --- nuovi text items (INCLUDED / NO_INCLUDED / NOTE) ---
                ti_map = _extract_di_textitems(resp_d.content)
                included_html = ti_map.get("INCLUDED") or ti_map.get("INCLUDE")
                excluded_html = ti_map.get("NO_INCLUDED") or ti_map.get("NOT_INCLUDED") or ti_map.get("EXCLUDED")
                notes_html    = ti_map.get("NOTE") or ti_map.get("NOTES")

                # Usa la versione "pulita" SOLO se non è vuota
                clean_desc = _extract_clean_descriptions_from_DI(resp_d.content)
                if clean_desc:
                    merged["descriptions"] = clean_desc

                if debug_flag:
                    lens = (len(included_html or ""), len(excluded_html or ""), len(notes_html or ""))
                    print(f"[PRODUCTS][EAGER] core={core} textItems lengths (inc/exc/note)={lens}", flush=True)

                # se non ci sono immagini logga, ma NON saltare (dobbiamo comunque salvare i dettagli)
                if fill_images and not (merged.get("image_urls") or []):
                    print(f"[PRODUCTS][EAGER] core={core} no images", flush=True)

                # persist per tutti i product_id del core
                batch_counter = 0
                for pid in product_ids:

                    # salva dettagli se richiesto
                    if fill_details:
                        try:
                            _save_detail_only(pid, {
                                "name":         merged.get("name") or "",
                                "duration":     merged.get("duration") or "",
                                "city":         merged.get("city") or "",
                                "country":      merged.get("country") or "",
                                "categories":   merged.get("categories") or [],
                                "types":        merged.get("types") or [],
                                "descriptions": merged.get("descriptions") or [],
                                "pickup_notes": merged.get("pickup_notes") or [],
                                "included_html": included_html,
                                "excluded_html": excluded_html,
                                "notes_html":    notes_html,
                            }, commit=False)
                            created_detail += 1
                        except Exception as e:
                            print(f"[PRODUCTS][EAGER] detail save error core={core} pid={pid}: {e}", flush=True)

                    # importa MEDIA SEMPRE (se richiesto), indipendentemente dal detail
                    if fill_images:
                        try:
                            _replace_media_only(pid, merged.get("image_urls") or [])
                            if merged.get("image_urls"):
                                created_media += len(merged.get("image_urls") or [])
                        except Exception as e:
                            print(f"[PRODUCTS][EAGER] media save error core={core} pid={pid}: {e}", flush=True)

                    # commit a blocchi
                    batch_counter += 1
                    if (batch_counter % 50) == 0:
                        db.session.commit()

            except Exception as e:
                print(f"[PRODUCTS][EAGER] core={core} error: {e}", flush=True)

        db.session.commit()
        print(f"[PRODUCTS][EAGER] done: details={created_detail}, media_rows={created_media}", flush=True)

    # --- VERIFICA POST-IMPORT (debug) ---
    try:
        from sqlalchemy import text as _sql
        cnt_det = db.session.query(OTAProductDetail).count()
        cnt_med = db.session.query(OTAProductMedia).count()
        print(f"[PRODUCTS][VERIFY] ota_product_detail rows = {cnt_det}, ota_product_media rows = {cnt_med}", flush=True)

        sample = db.session.execute(
            _sql("SELECT product_id, name, LENGTH(descriptions_json) AS dlen FROM ota_product_detail LIMIT 3")
        ).fetchall()
        print(f"[PRODUCTS][VERIFY] sample detail rows: {sample}", flush=True)
    except Exception as e:
        print(f"[PRODUCTS][VERIFY][ERR] {e}", flush=True)


    return redirect(url_for("products.ota_products"))



# ----------------- LISTA PRODOTTI (unchanged) -----------------

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

    groups_list = sorted(groups.values(), key=lambda x: (x.get("name") or x.get("core") or ""))

    total_groups = len(groups_list)
    total_items  = sum(len(g.get("items", [])) for g in groups_list)

    return render_template(
        "products/ota_products_grouped.html",
        groups=groups_list,
        total_groups=total_groups,
        total_items=total_items,
    )


# ----------------- DETTAGLIO PRODOTTO (solo da cache, niente fetch) -----------------

@bp.route("/ota_products/<int:product_id>", methods=["GET"], endpoint="ota_product_detail")
@login_required
def ota_product_detail(product_id: int):
    row = db.session.get(OTAProduct, product_id)
    if not row:
        return "Prodotto non trovato", 404

    # prendo SOLO dalla cache (niente chiamate live)
    rec = OTAProductDetail.query.filter_by(product_id=product_id).first()
    image_urls = [m.url for m in OTAProductMedia.query
                  .filter_by(product_id=product_id)
                  .order_by(OTAProductMedia.sort_order.asc()).all()]

    if rec:
        descs_raw = json.loads(rec.descriptions_json or "[]")
        descriptions = [Markup(html.unescape(x or "")) for x in descs_raw]
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

            # sezioni dedicate se presenti a DB (usa getattr per compatibilità schema)
            "included_html": Markup(html.unescape(rec.included_html)) if getattr(rec, "included_html", None) else None,
            "excluded_html": Markup(html.unescape(rec.excluded_html)) if getattr(rec, "excluded_html", None) else None,
            "notes_html":    Markup(html.unescape(rec.notes_html))    if getattr(rec, "notes_html", None)    else None,
        }
    else:
        # nessun dettaglio: struttura minimale per il template
        detail = {
            "name": row.tour_activity_name or "",
            "duration": "",
            "city": row.city_code or "",
            "country": f"{row.country_iso or ''} {row.country_name or ''}".strip(),
            "categories": [],
            "types": [],
            "descriptions": [],
            "pickup_notes": [],
            "image_urls": image_urls,

            "included_html": None,
            "excluded_html": None,
            "notes_html": None,
        }

    # link preventivi (se i template lo usano)
    detail["quote_url"] = url_for("availability.availability_quote", product_id=product_id)

    return render_template("products/ota_product_detail.html", p=row, detail=detail)
