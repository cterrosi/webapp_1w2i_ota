import requests
from requests.auth import HTTPBasicAuth
from datetime import date, timedelta
from lxml import etree as ET

from flask import (
    Blueprint, render_template, request, flash, jsonify, redirect, url_for, abort, current_app
)

from flask_login import login_required
from sqlalchemy import text as _sql

from ..services.runtime import get_setting_safe
from ..services.ota_io import (
    build_quote_xml, parse_quote_full,
    # se/quando servono:
    build_availability_xml_from_product, parse_availability_xml
)
from ..extensions import db
from ..models import OTAProduct
from sqlalchemy import text as _sql, bindparam

bp = Blueprint("availability", __name__, url_prefix="/availability")

def get_cfg():
    s = get_setting_safe()

    def _to_int(v, default=None):
        try:
            return int(v) if v is not None and str(v).strip() != "" else default
        except Exception:
            return default

    return {
        "base_url": (s.base_url or "").rstrip("/"),
        "requestor_id": s.requestor_id,
        "message_password": s.message_password,
        "market_country_code": s.market_country_code or "it",
        "primary_lang_id": s.primary_lang or "it",
        "target": s.target or "Production",
        "chain_code": s.chain_code or "SANDTOUR",
        "timeout": s.timeout_seconds or 40,
        "bearer": s.bearer_token or "",
        "basic_user": getattr(s, "basic_user", "") or "",
        "basic_pass": getattr(s, "basic_pass", "") or "",

        "product_type": getattr(s, "product_type", None) or "Tour",
        "category_code": getattr(s, "category_code", None) or "211",
        "departure_default": getattr(s, "departure_default", None) or "",
        "los_min": _to_int(getattr(s, "los_min", None)),
        "los_max": _to_int(getattr(s, "los_max", None)),
    }

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
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
            return xml_bytes.decode("utf-8", errors="ignore")
        except Exception:
            return str(xml_bytes)


# ------------------------------------------------------------
# Quote (già tua)
# ------------------------------------------------------------
@bp.route("/<product_id>/quote", methods=["GET", "POST"], endpoint="availability_quote")
@login_required
def availability_quote(product_id):
    # Shim: se arriva qui il form di availability, inoltra
    if request.method == "POST":
        form_preview = request.form.to_dict()
        print("[QUOTE POST]", form_preview, flush=True)
        if (not request.values.get("booking_code")) and (form_preview.get("action") == "availability"):
            return redirect(url_for("ota_product_availability", product_id=product_id), code=307)
    else:
        print("[QUOTE GET args]", request.args.to_dict(), flush=True)

    booking_code   = request.values.get("booking_code")
    start_date     = request.values.get("start_date")
    end_date       = request.values.get("end_date")
    rate_plan_code = request.values.get("rate_plan_code")
    chain_code     = request.values.get("chain_code")

    if request.method == "GET":
        default_guests = [
            {"rph": 1, "given": "TEST1", "surname": "SANDT", "email": "test@mail.com", "birthdate": "1998-04-02"},
            {"rph": 2, "given": "TEST2", "surname": "SANDT", "email": "test@mail.com", "birthdate": "1998-05-02"},
        ]
        return render_template(
            "availability/availability_quote.html",
            product_id=product_id,
            booking_code=booking_code,
            start_date=start_date,
            end_date=end_date,
            rate_plan_code=rate_plan_code,
            chain_code=chain_code,
            guests=default_guests,
        )

    cfg = get_cfg()
    if chain_code:
        cfg = dict(cfg, chain_code=chain_code)

    guests = []
    idx = 1
    while True:
        if not request.form.get(f"guest_{idx}_given"):
            break
        guests.append({
            "rph": idx,
            "given": (request.form.get(f"guest_{idx}_given") or "").strip(),
            "surname": (request.form.get(f"guest_{idx}_surname") or "").strip(),
            "email": (request.form.get(f"guest_{idx}_email") or "").strip(),
            "birthdate": (request.form.get(f"guest_{idx}_birthdate") or "").strip(),
        })
        idx += 1

    res_id_value = request.form.get("res_id_value", "123456789")

    xml_body = build_quote_xml(
        cfg,
        booking_code=booking_code,
        start_date=start_date,
        end_date=end_date,
        guests=guests,
        res_id_value=res_id_value,
        rate_plan_code=rate_plan_code
    )

    url = cfg["base_url"].rstrip("/") + "/TourActivityRes"
    headers = {"Content-Type": "application/xml; charset=utf-8", "Accept": "application/xml"}

    auth = None
    bearer = cfg.get("bearer")
    basic_user = cfg.get("basic_user")
    basic_pass = cfg.get("basic_pass")
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    elif basic_user and basic_pass:
        auth = HTTPBasicAuth(basic_user, basic_pass)

    try:
        resp = requests.post(url, data=xml_body, headers=headers, auth=auth, timeout=cfg["timeout"])
        resp.raise_for_status()
    except requests.RequestException as ex:
        err_body = ""
        try:
            if getattr(ex, "response", None) is not None:
                err_body = ex.response.text or ""
        except Exception:
            pass
        flash(f"Errore di rete sulla quotazione: {ex}", "danger")
        return render_template(
            "availability/availability_quote_result.html",
            product_id=product_id,
            request_xml=xml_body.decode("utf-8", errors="ignore"),
            response_xml=err_body,
            result={"success": False, "errors": [str(ex)]}
        )

    result = parse_quote_full(resp.content)
    return render_template(
        "availability/availability_quote_result.html",
        product_id=product_id,
        request_xml=xml_body.decode("utf-8", errors="ignore"),
        response_xml=resp.text,
        result=result
    )


# ------------------------------------------------------------
# Partenze da DB per product_id
# ------------------------------------------------------------
@bp.get("/<int:product_id>/departures", endpoint="departures_json")
@login_required
def departures_json(product_id):
    row = db.session.get(OTAProduct, product_id)
    if not row:
        return jsonify({"ok": False, "error": "Prodotto non trovato"}), 404
    code = row.tour_activity_code or ""
    rows = db.session.execute(
        _sql("SELECT depart_date, duration_days FROM departures_cache WHERE product_code = :code ORDER BY depart_date"),
        {"code": code},
    ).fetchall()
    out = [{"date": (r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0])), "duration_days": r[1]} for r in rows]
    return jsonify({"ok": True, "rows": out})


# ------------------------------------------------------------
# Partenze per destinazione ISO (DEST obbligatoria, APTFROM opzionale)
# Ritorna rows = [{date, duration_days}]
# ------------------------------------------------------------
from sqlalchemy import func, bindparam, text as _sql, inspect

@bp.route("/departures/by-dest", methods=["GET"], endpoint="departures_by_dest")
@login_required
def departures_by_dest():
    dest = (request.args.get("dest") or "").upper().strip()      # <-- ISO code richiesto
    aptfrom = (request.args.get("aptfrom") or "").upper().strip()
    if not dest:
        return jsonify({"ok": True, "rows": [], "min": None, "max": None})

    # 1) Campo destinazione: usiamo per primi i codici (ISO) reali presenti nel DB
    #    Ordine: city_code, dest_code, destination_code, destination, destina, city
    dest_attr_name = next(
        (a for a in (
            "city_code", "dest_code", "destination_code", "destination",
            "destina", "city"
        ) if hasattr(OTAProduct, a)),
        None
    )
    if not dest_attr_name:
        return jsonify({"ok": True, "rows": [], "min": None, "max": None})
    dest_attr = getattr(OTAProduct, dest_attr_name)

    # 2) Codici prodotto per DEST (match case-insensitive)
    codes_rows = (
        db.session.query(OTAProduct.tour_activity_code)
        .filter(OTAProduct.tour_activity_code.isnot(None))
        .filter(func.upper(dest_attr) == dest)
        .distinct()
        .all()
    )
    product_codes = [row[0] for row in codes_rows if row and row[0]]
    if not product_codes:
        # Il codice ISO non esiste nei prodotti → nessuna partenza
        return jsonify({"ok": True, "rows": [], "min": None, "max": None})

    # 3) Date + durata aggregate dal cache per quei product_code
    base_sql = """
        SELECT
            depart_date,
            MAX(COALESCE(duration_days,0)) AS duration_days,
            UPPER(SUBSTR(depart_airport,1,3)) AS depart_airport
        FROM departures_cache
        WHERE product_code IN :codes
    """
    params = {"codes": product_codes}


    # 4) Filtro opzionale per aeroporto (robusto: verifica colonne presenti + LIKE portabile)
    if aptfrom:
        insp = inspect(db.engine)
        try:
            cols = {c["name"] for c in insp.get_columns("departures_cache")}
        except Exception:
            cols = set()
        candidates = ["depart_airport", "aptfrom", "airport", "from_airport"]
        avail = [c for c in candidates if c in cols]
        if avail:
            conds = []
            for c in avail:
                conds.append(f"UPPER(TRIM(COALESCE({c},''))) = :aptfrom")
                conds.append(f"UPPER(COALESCE({c},'')) LIKE :aptfrom_like")  # copre descrittivi tipo "Milano Malpensa (MXP)"
            base_sql += " AND (" + " OR ".join(conds) + ")"
            params["aptfrom"] = aptfrom
            params["aptfrom_like"] = f"%{aptfrom}%"

    base_sql += """
        GROUP BY depart_date, UPPER(SUBSTR(depart_airport,1,3))
        ORDER BY depart_date
    """

    stmt = _sql(base_sql).bindparams(bindparam("codes", expanding=True))
    rows = db.session.execute(stmt, params).fetchall()

    def iso10(v):
        try:
            return v.date().isoformat()
        except Exception:
            return str(v)[:10]

    out = [
        {
            "date": iso10(d),
            "duration_days": int(dur or 0),
            "depart_airport": (apt or "").strip()
        }
        for d, dur, apt in rows
    ]
    dates_only = [r["date"] for r in out]

    return jsonify({
        "ok": True,
        "rows": out,
        "min": (dates_only[0] if dates_only else None),
        "max": (dates_only[-1] if dates_only else None),
    })



# ------------------------------------------------------------
# Ricerca disponibilità via OTAX_TourActivityAvailRQ (multi-departure)
# ------------------------------------------------------------
@bp.route("/search", methods=["GET"], endpoint="search")
@login_required
def availability_search():
    from datetime import date, datetime, timedelta   # 👈 aggiunto 'date'
    import re, requests
    from lxml import etree as ET
    from sqlalchemy import text as _sql
    from app import db

    OTA_NS = "http://www.opentravel.org/OTA/2003/05"

    def _parse_children_ages(s: str):
        if not s:
            return []
        out = []
        for p in re.split(r"[,\s;]+", s.strip()):
            if not p:
                continue
            try:
                age = int(p)
                if 0 <= age <= 17:
                    out.append(age)
            except Exception:
                pass
        return out

    # ----------------- Input -----------------
    aptfrom   = (request.args.get("aptfrom") or "").upper().strip()
    destina   = (request.args.get("destina") or "").upper().strip()
    start_date = (request.args.get("start_date") or "").strip()
    end_date   = (request.args.get("end_date") or "").strip()

    # notti: leggi param e, se nullo/0, ricalcola dalla differenza date
    try:
        nights = int(request.args.get("nights") or 0)
    except Exception:
        nights = 0
    if nights <= 0 and start_date and end_date:
        try:
            nights = max((date.fromisoformat(end_date) - date.fromisoformat(start_date)).days, 0)
        except Exception:
            nights = 0

    rooms         = int(request.args.get("rooms") or 1)
    adults        = int(request.args.get("adults") or 2)
    children_ages = _parse_children_ages(request.args.get("children_ages") or "")
    currency      = (request.args.get("currency") or "EUR").upper()   # 👈 aggiunto

    # ----------------- Validazioni -----------------
    if not destina or not start_date or nights <= 0:
        abort(400, description="Parametri ricerca non validi")

    try:
        dt_start = datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        abort(400, description="Formato data non valido (start_date)")

    if not end_date:
        end_date = (dt_start + timedelta(days=nights)).strftime("%Y-%m-%d")

    # ----------------- Settings (SOLO da get_cfg) -----------------
    cfg = get_cfg()
    base_url         = cfg.get("base_url")
    bearer           = cfg.get("bearer")
    target           = cfg.get("target")
    primary_lang_id  = cfg.get("primary_lang_id")
    market_country   = cfg.get("market_country_code")
    requestor_id     = cfg.get("requestor_id")
    message_password = cfg.get("message_password")
    chain_code       = cfg.get("chain_code")
    product_type     = cfg.get("product_type")
    category_code    = cfg.get("category_code")
    timeout_sec      = cfg.get("timeout", 40)

    if (not base_url or not bearer or not target or not primary_lang_id or not market_country
        or not requestor_id or not message_password or not chain_code):
        abort(500, description="Configurazione API mancante/incompleta")

    missing = [k for k, v in {
        "base_url": base_url, "bearer": bearer,
        "target": target, "primary_lang_id": primary_lang_id,
        "market_country_code": market_country,
        "requestor_id": requestor_id, "message_password": message_password,
        "chain_code": chain_code, "product_type": product_type, "category_code": category_code,
    }.items() if not v]
    if missing:
        abort(500, description=f"Config mancante: {', '.join(missing)}")

    # ----------------- Costruisci lista aeroporti dalla cache (robusta) -----------------
    from sqlalchemy import inspect as _inspect, text as _sql

    def _extract_core(pc: str) -> str:
        if not pc:
            return ""
        return (pc.split("#", 1)[0] or "").strip()

    product_core = _extract_core(
        request.args.get("product_core") or request.args.get("product_code") or ""
    )

    def _to3(a: str) -> str:
        a = (a or "").upper().strip()
        return a[:3] if len(a) >= 3 else a

    inspector = _inspect(db.engine)
    _dep_table = None
    for cand in ("departures_cache", "DeparturesCache"):
        try:
            if inspector.has_table(cand):
                _dep_table = cand
                break
        except Exception:
            pass

    def _deps_from_departures_cache(dest: str, start_iso: str, end_iso: str, nights_val: int, prod_core: str | None):
        """
        Ritorna un set di aeroporti (3 lettere) da departures_cache:
        - Se prod_core è valorizzato → filtra per product_code LIKE '{prod_core}%'
        - Altrimenti → filtra per city_code = dest
        """
        deps = set()
        if not _dep_table:
            return deps

        def _fetch(q: str, params: dict):
            try:
                rows = db.session.execute(_sql(q), params).fetchall()
                return {_to3(r[0]) for r in rows if r and r[0]}
            except Exception as ex:
                print(f"[availability.search] WARN departures_cache query: {ex}", flush=True)
                return set()

        base_where = []
        p = {"start": start_iso, "end": end_iso, "n": nights_val or None}

        if prod_core:
            base_where.append("product_code LIKE :pclike")
            p["pclike"] = f"{prod_core}%"
        else:
            base_where.append("UPPER(city_code) = :dest")
            p["dest"] = dest.upper()

        # preferisci stesso soggiorno
        q1 = f"""
            SELECT DISTINCT UPPER(SUBSTR(depart_airport,1,3)) AS dep
            FROM {_dep_table}
            WHERE {' AND '.join(base_where)}
              AND depart_date BETWEEN :start AND :end
              AND (duration_days = :n OR :n IS NULL)
        """
        deps |= _fetch(q1, p)

        # se vuoto, ignora le notti
        if not deps:
            q2 = f"""
                SELECT DISTINCT UPPER(SUBSTR(depart_airport,1,3)) AS dep
                FROM {_dep_table}
                WHERE {' AND '.join(base_where)}
                  AND depart_date BETWEEN :start AND :end
            """
            deps |= _fetch(q2, p)

        # se ancora vuoto, qualsiasi data
        if not deps:
            q3 = f"""
                SELECT DISTINCT UPPER(SUBSTR(depart_airport,1,3)) AS dep
                FROM {_dep_table}
                WHERE {' AND '.join(base_where)}
            """
            deps |= _fetch(q3, p)

        # pulizia: rimuovi NULL/blank
        deps = {d for d in deps if d and len(d) == 3}
        return deps

    if aptfrom:
        candidate_deps = [_to3(aptfrom)]
    else:
        # 1) prova dalla nuova departures_cache con city_code / product_core
        candidate_deps = sorted(
            _deps_from_departures_cache(destina, start_date, end_date, nights, product_core or None)
        )

        # 2) fallback: vecchio metodo su OTAProduct estraendo '#XXX' (se serve)
        if not candidate_deps:
            try:
                rows = db.session.execute(
                    _sql("""
                        SELECT DISTINCT UPPER(SUBSTR(Code, INSTR(Code, '#') + 1, 3)) AS Dep3
                        FROM OTAProduct
                        WHERE (:pcore != '' AND Code LIKE :pclike)
                           OR (:pcore = '' AND Code LIKE :pref)
                        ORDER BY Dep3
                    """),
                    {
                        "pcore": product_core,
                        "pclike": f"{product_core}%" if product_core else "",
                        "pref": f"0000{destina}%" if not product_core else "",
                    }
                ).fetchall()
                candidate_deps = [ (r[0] or "").strip() for r in rows if r and r[0] ]
            except Exception as ex:
                print(f"[availability.search] WARN OTAProduct fallback {destina}: {ex}", flush=True)

        # 3) normalizza/limita a 10
        candidate_deps = sorted({ _to3(a) for a in candidate_deps if a })[:10]

        # 4) ultimo paracadute
        if not candidate_deps:
            if cfg.get("departure_default"):
                candidate_deps = [_to3(cfg["departure_default"])]
                print("[availability.search] WARNING: cache vuota; uso departure_default", flush=True)
            else:
                abort(400, description="Nessun aeroporto di partenza noto in cache per la destinazione/prodotto selezionati")

    print(f"[availability.search] Departures to query: {', '.join(candidate_deps)}", flush=True)

    # 🔹 LOG riepilogo
    current_app.logger.info("AVAIL → dest=%s start=%s end=%s nights=%s apt=%s",
                            destina, start_date, end_date, nights, aptfrom)

    # ----------------- Helper: build + call + parse per un singolo airport -----------------
    def _build_payload(dep_code: str) -> bytes:
        E = ET.Element
        rq = E("{%s}OTAX_TourActivityAvailRQ" % OTA_NS,
               Target=target, PrimaryLangID=primary_lang_id, MarketCountryCode=market_country,
               nsmap={None: OTA_NS})

        # POS / Source
        pos = E("{%s}POS" % OTA_NS); src = E("{%s}Source" % OTA_NS)
        pos.append(src); rq.append(pos)
        src.append(E("{%s}RequestorID" % OTA_NS, ID=requestor_id, MessagePassword=message_password))

        # Avail segment
        av_segs = E("{%s}AvailRequestSegments" % OTA_NS); rq.append(av_segs)
        seg = E("{%s}AvailRequestSegment" % OTA_NS); av_segs.append(seg)

        # Criteri di ricerca
        tasc = E("{%s}TourActivitySearchCriteria" % OTA_NS); seg.append(tasc)
        crit = E("{%s}Criterion" % OTA_NS); tasc.append(crit)

        ref_attrs = {
            "ChainCode": chain_code,
            "ProductType": product_type,
            "CategoryCode": category_code,
            "TourActivityCityCode": destina
        }
        if dep_code:
            ref_attrs["DepartureLocation"] = dep_code
        crit.append(E("{%s}TourActivityRef" % OTA_NS, **ref_attrs))

        # Durata ESATTA
        node = E("{%s}LengthOfStay" % OTA_NS); node.text = str(nights); crit.append(node)

        # Date ESATTE
        seg.append(E("{%s}StayDateRange" % OTA_NS, Start=start_date, End=end_date))

        # Occupazione
        acs = E("{%s}ActivityCandidates" % OTA_NS); seg.append(acs)
        ac = E("{%s}ActivityCandidate" % OTA_NS, Quantity=str(max(rooms, 1)), RPH="01"); acs.append(ac)

        gcs = E("{%s}GuestCounts" % OTA_NS); ac.append(gcs)
        for _ in range(max(adults, 0)):
            gcs.append(E("{%s}GuestCount" % OTA_NS, Age="50", Count="1"))
        for age in children_ages:
            gcs.append(E("{%s}GuestCount" % OTA_NS, Age=str(age), Count="1"))

        return ET.tostring(rq, xml_declaration=True, encoding="utf-8", pretty_print=True)

    def _avail_url(base: str) -> str:
        base = (base or "").rstrip("/")
        if base.lower().endswith("/otaservice"):
            return f"{base}/TourActivityAvail"
        return f"{base}/OtaService/TourActivityAvail"

    url = _avail_url(base_url)
    headers = {
        "Authorization": f"Bearer {bearer}",
        "Content-Type": "application/xml; charset=utf-8",
        "Accept": "application/xml",
    }

    def _parse_response(content: bytes, dep_code: str):
        offers = []; warnings = []
        root = ET.fromstring(content)
        ns = {"ota": OTA_NS}

        for err in root.findall(".//ota:Errors/ota:Error", namespaces=ns):
            st = (err.get("ShortText") or "").strip()
            cd = (err.get("Code") or "").strip()
            if st or cd:
                warnings.append(f"{cd} {st}".strip())

        for act in root.findall(".//ota:Activities/ota:Activity", namespaces=ns):
            status = (act.get("AvailabilityStatus") or "").lower()

            bpi = act.find("ota:BasicPropertyInfo", namespaces=ns)
            name = bpi.get("TourActivityName") if bpi is not None else None
            tour_activity_code = bpi.get("TourActivityCode") if bpi is not None else None

            rp = act.find("ota:RatePlans/ota:RatePlan", namespaces=ns)
            rp_name = rp.get("RatePlanName") if rp is not None else None

            ar = act.find("ota:ActivityRates/ota:ActivityRate", namespaces=ns)
            booking_code = ar.get("BookingCode") if ar is not None else None
            room_code_from_ar = ar.get("ActivityTypeCode") if ar is not None else None
            tnode = ar.find("ota:Total", namespaces=ns) if ar is not None else None
            total = tnode.get("AmountAfterTax") if tnode is not None else None
            currency_node = tnode.get("CurrencyCode") if tnode is not None else None

            ts = act.find("ota:TimeSpan", namespaces=ns)
            start = ts.get("Start") if ts is not None else start_date
            end   = ts.get("End") if ts is not None else end_date

            img_url = None
            img_node = act.find(".//ota:TPA_Extensions/ota:ImageItems/ota:ImageItem/ota:ImageFormat/ota:URL", namespaces=ns)
            if img_node is not None and (img_node.text or "").strip():
                img_url = img_node.text.strip()

            # Descrizione breve
            short_desc = None
            n = act.find(".//ota:TPA_Extensions/ota:TextItems/ota:TextItem[@SourceID='NOTE']/ota:Description", namespaces=ns)
            if n is not None and (n.text or "").strip():
                short_desc = n.text.strip()
            if not short_desc:
                n2 = act.find(".//ota:TextItems/ota:TextItem/ota:Description", namespaces=ns)
                if n2 is not None and (n2.text or "").strip():
                    short_desc = n2.text.strip()
            if not short_desc:
                n3 = act.find(".//ota:Description", namespaces=ns)
                if n3 is not None and (n3.text or "").strip():
                    short_desc = n3.text.strip()

            service_map = {}
            for at in act.findall("ota:ActivityTypes/ota:ActivityType", namespaces=ns):
                code = at.get("ActivityTypeCode") or ""
                text_node = at.find("ota:ActivityDescription/ota:Text", namespaces=ns)
                desc = (text_node.text or "").strip() if text_node is not None and text_node.text else ""
                if code and desc:
                    service_map[code] = desc

            room_code = room_code_from_ar
            if not room_code and booking_code:
                parts = (booking_code or "").split("|")
                if len(parts) >= 2:
                    room_code = parts[1]
            room_name = service_map.get(room_code)

            # VOLI
            flights = []
            flight_direction = None
            aid = act.find(".//ota:TPA_Extensions/ota:AirItineraries/ota:AirItineraryDetail", namespaces=ns)
            if aid is not None:
                flight_direction = aid.get("DirectionInd")
                odos = aid.find("ota:OriginDestinationOptions", namespaces=ns)
                if odos is not None:
                    for od in odos.findall("ota:OriginDestinationOption", namespaces=ns):
                        od_rph = od.get("RPH")
                        for seg in od.findall("ota:FlightSegment", namespaces=ns):
                            dep = seg.find("ota:DepartureAirport", namespaces=ns)
                            arr = seg.find("ota:ArrivalAirport", namespaces=ns)
                            op  = seg.find("ota:OperatingAirline", namespaces=ns)
                            mk  = seg.find("ota:MarketingAirline", namespaces=ns)
                            bag = seg.find("ota:TPA_Extensions/ota:Baggage/ota:Weight", namespaces=ns)
                            flights.append({
                                "od_rph": od_rph,
                                "departure": {
                                    "datetime": seg.get("DepartureDateTime"),
                                    "airport":  (dep.get("LocationCode") if dep is not None else None),
                                    "name":     (dep.get("LocationName") if dep is not None else None),
                                },
                                "arrival": {
                                    "datetime": seg.get("ArrivalDateTime"),
                                    "airport":  (arr.get("LocationCode") if arr is not None else None),
                                    "name":     (arr.get("LocationName") if arr is not None else None),
                                },
                                "flight_number": seg.get("FlightNumber"),
                                "booking_class": seg.get("ResBookDesigCode"),
                                "operating": {
                                    "code": (op.get("Code") if op is not None else None),
                                    "name": (op.get("CompanyShortName") if op is not None else None),
                                },
                                "marketing": {
                                    "code": (mk.get("Code") if mk is not None else None),
                                    "name": (mk.get("CompanyShortName") if mk is not None else None),
                                },
                                "baggage": {
                                    "weight": (bag.get("Weight") if bag is not None else None),
                                    "unit":   ((bag.text.strip() if (bag is not None and bag.text) else None)),
                                },
                            })

            if status in ("availableforsale", "available"):
                offers.append({
                    "product_name": name,
                    "tour_activity_code": tour_activity_code,
                    "rate_plan": rp_name,
                    "booking_code": booking_code,
                    "room_code": room_code,
                    "room_name": room_name,
                    "services": [{"code": c, "name": n} for c, n in service_map.items()],
                    "services_display": [f"{n.upper()} ({c})" for c, n in service_map.items()],
                    "total_price": total,
                    "currency": currency_node or currency,  # 👈 usa currency rilevata o query
                    "start": start,
                    "end": end,
                    "status": status,
                    "image": img_url,
                    "flights": flights,
                    "flight_direction": flight_direction,
                    "departure_location": dep_code,
                    "short_desc": short_desc,
                })
        return offers, warnings

    # ----------------- Esegui più chiamate (una per airport) e unisci i risultati -----------------
    all_offers = []; all_warnings = []
    for dep in candidate_deps:
        payload_xml = _build_payload(dep)
        print(f"[availability.search] POST URL (AVAIL): {url}  | DEP={dep}", flush=True)
        try:
            resp = requests.post(url, data=payload_xml, headers=headers, timeout=timeout_sec)
        except requests.RequestException as ex:
            all_warnings.append(f"Network error {dep}: {ex}")
            continue
        if resp.status_code != 200:
            all_warnings.append(f"HTTP {resp.status_code} {dep}: {resp.text[:300]}")
            continue

        try:
            offers_dep, warnings_dep = _parse_response(resp.content, dep)
            # tieni solo offerte esattamente sulle date richieste
            offers_dep = [
                o for o in offers_dep
                if (o.get("start") or "")[:10] == start_date
                and (o.get("end") or "")[:10]   == end_date
            ]
            all_offers.extend(offers_dep)
            all_warnings.extend(warnings_dep)
        except Exception as ex:
            all_warnings.append(f"Parse error {dep}: {ex}")

    # ----------------- Meta -----------------
    meta = {
        "currency": None,
        "raw_errors": all_warnings,
        "request": {
            "aptfrom": aptfrom or "ANY",
            "destina": destina,
            "start_date": start_date,
            "end_date": end_date,
            "nights": nights,
            "rooms": rooms,
            "adults": adults,
            "children_ages": children_ages,
            "currency": currency,            # 👈 aggiunto
        },
    }

    # ----------------- Raggruppamento per prodotto + Ordinamento -----------------
    from decimal import Decimal, InvalidOperation

    def _parse_price(x):
        try:
            return Decimal(str(x))
        except (InvalidOperation, TypeError):
            return Decimal("Infinity")

    def _split_codes(booking_code: str):
        if not booking_code:
            return (None, None)
        left = booking_code.split("|", 1)[0]   # '0000RMFCORE#BGY1'
        parts = left.split("#", 1)
        product_core = (parts[0] or "").strip()
        dep_variant  = (parts[1].strip() if len(parts) > 1 else None)
        return (product_core, dep_variant)

    def _is_recommended(product_core: str) -> bool:
        try:
            row = db.session.execute(
                _sql("""
                    SELECT COALESCE(IsRecommended, Recommended, 0)
                    FROM OTAProduct
                    WHERE Code = :c
                    LIMIT 1
                """), {"c": product_core}
            ).fetchone()
            if not row:
                return False
            val = row[0]
            if isinstance(val, (int, bool)):
                return bool(val)
            s = str(val).strip().lower()
            return s in ("1", "true", "t", "yes", "y")
        except Exception:
            return False

    groups_map = {}
    for o in all_offers:
        pc, depv = _split_codes(o.get("booking_code"))
        if not pc:
            pc = "__MISC__"
        grp = groups_map.get(pc)
        price = _parse_price(o.get("total_price"))
        if not grp:
            grp = {
                "product_core": pc,
                "name": (o.get("product_name") or o.get("name") or pc),
                "image": o.get("image"),
                "is_recommended": _is_recommended(pc),
                "min_price": price,
                "currency": (o.get("currency") or meta.get("currency") or currency),
                "flights": {},
                "offers": [],
                "short_desc": o.get("short_desc"),
            }
            groups_map[pc] = grp
        else:
            if price < grp["min_price"]:
                grp["min_price"] = price
                grp["currency"] = (o.get("currency") or grp["currency"] or currency)
            if not grp.get("image") and o.get("image"):
                grp["image"] = o["image"]
            if not grp.get("short_desc") and o.get("short_desc"):
                grp["short_desc"] = o["short_desc"]

        if depv:
            sol = grp["flights"].get(depv)
            if not sol:
                grp["flights"][depv] = {
                    "package_code": f"{pc}#{depv}",
                    "min_price": price,
                    "samples": o.get("flights") or [],
                    "direction": o.get("flight_direction"),
                }
            else:
                if price < sol["min_price"]:
                    sol["min_price"] = price
                if o.get("flights"):
                    sol["samples"] = o["flights"]

        grp["offers"].append(o)

    groups = list(groups_map.values())
    groups.sort(key=lambda g: (
        0 if g["is_recommended"] else 1,
        g["min_price"],
        (g["name"] or "")
    ))
    for g in groups:
        g["flight_solutions"] = sorted(
            g["flights"].values(),
            key=lambda s: (s["min_price"], s["package_code"])
        )

    if not groups:
        all_warnings.append("Nessuna disponibilità trovata per gli aeroporti: " + ", ".join(candidate_deps))

    group_count = len(groups)
    total_packages = len(all_offers)
    meta["counts"] = {"groups": group_count, "packages": total_packages}

    # 👇 PASSO al template anche le variabili di intestazione per evitare "(0 nights)"
    return render_template(
        "availability/search_grouped.html",
        groups=groups,
        meta=meta,
        group_count=group_count,
        total_packages=total_packages,
        # header params
        aptfrom=aptfrom,
        destina=destina,
        start_date=start_date,
        end_date=end_date,
        nights=nights,
        rooms=rooms,
        adults=adults,
        children_ages=children_ages,
        currency=currency,
    )







# ------------------------------------------------------------
# Quote by booking_code (post-Avail) + image enrich
# ------------------------------------------------------------
@bp.route("/quote_by_code", methods=["POST"], endpoint="quote_by_code")
@login_required
def quote_by_code():
    import re, requests
    from datetime import date
    from lxml import etree as ET
    from sqlalchemy import text as _sql
    try:
        from app import db  # adatta se il tuo import è diverso
    except Exception:
        from app.extensions import db  # fallback, se usi questa convenzione

    booking_code = (request.form.get("booking_code") or "").strip()
    start_date   = (request.form.get("start_date") or "").strip()
    end_date     = (request.form.get("end_date") or "").strip()
    aptfrom      = (request.form.get("aptfrom") or "").upper().strip()
    adults       = int(request.form.get("adults") or 2)
    # opzionali (header pagina)
    destina      = (request.form.get("destina") or "").upper().strip()
    nights       = int(request.form.get("nights") or 0)
    rooms        = int(request.form.get("rooms") or 1)
    s_ages       = (request.form.get("children_ages") or "").strip()
    children_ages = []
    if s_ages:
        for p in re.split(r"[,\s;]+", s_ages):
            try: children_ages.append(int(p))
            except: pass

    # 👇 nuova: se la pagina Avail aveva già l'immagine, la portiamo qui
    image_from_form = (request.form.get("image") or "").strip() or None

    if not booking_code or not start_date or not end_date:
        abort(400, description="Parametri quote mancanti")

    cfg = get_cfg()
    base_url = cfg.get("base_url")
    bearer   = cfg.get("bearer")
    if not base_url or not bearer:
        abort(500, description="Configurazione API mancante (base_url/bearer)")

    # ---------- helpers ----------
    def bd_from_age_on(start_iso: str, years: int) -> str:
        y, m, d = map(int, start_iso.split("-")); by = y - years
        try: return date(by, m, d).isoformat()
        except Exception:
            while d > 28:
                d -= 1
                try: return date(by, m, d).isoformat()
                except Exception: pass
            return f"{by:04d}-{m:02d}-{max(d,1):02d}"

    def _core_from_booking_code(code: str) -> str:
        return (code or "").split("|", 1)[0].strip()

    def _db_image_for_core(core: str) -> str | None:
        if not core: return None
        for col in ("ImageUrl", "ThumbUrl", "image_url", "image", "thumb"):
            try:
                row = db.session.execute(
                    _sql(f"SELECT {col} FROM OTAProduct WHERE Code = :c LIMIT 1"), {"c": core}
                ).fetchone()
                if row and row[0]:
                    return (row[0] or "").strip()
            except Exception:
                continue
        return None

    def _di_url(base: str) -> str:
        base = (base or "").rstrip("/")
        if base.lower().endswith("/otaservice"):
            return f"{base}/TourActivityDescriptiveInfo"
        return f"{base}/OtaService/TourActivityDescriptiveInfo"

    def _api_image_for_core(core: str) -> str | None:
        if not core: return None
        OTA_NS = "http://www.opentravel.org/OTA/2003/05"
        E = ET.Element
        rq = E("{%s}OTAX_TourActivityDescriptiveInfoRQ" % OTA_NS,
               Target=cfg["target"],
               PrimaryLangID=cfg["primary_lang_id"],
               MarketCountryCode=cfg["market_country_code"],
               nsmap={None: OTA_NS})
        pos = E("{%s}POS" % OTA_NS); src = E("{%s}Source" % OTA_NS)
        pos.append(src); rq.append(pos)
        src.append(E("{%s}RequestorID" % OTA_NS,
                     ID=cfg["requestor_id"], MessagePassword=cfg["message_password"]))
        infos = E("{%s}TourActivityDescriptiveInfos" % OTA_NS); rq.append(infos)
        info = E("{%s}TourActivityDescriptiveInfo" % OTA_NS); infos.append(info)
        info.append(E("{%s}BasicPropertyInfo" % OTA_NS,
                      ChainCode=cfg["chain_code"], TourActivityCode=core))
        # opzionale: alcuni backend richiedono esplicitamente le immagini
        # tpa = E("{%s}TPA_Extensions" % OTA_NS); info.append(tpa)
        # tpa.append(E("{%s}ReturnImageItems" % OTA_NS)).text = "true"

        payload = ET.tostring(rq, xml_declaration=True, encoding="utf-8", pretty_print=True)
        url = _di_url(base_url)
        headers = {
            "Authorization": f"Bearer {bearer}",
            "Content-Type": "application/xml; charset=utf-8",
            "Accept": "application/xml",
        }
        try:
            r = requests.post(url, data=payload, headers=headers, timeout=cfg.get("timeout", 40))
            if r.status_code != 200:
                return None
            root = ET.fromstring(r.content)
            ns = {"ota": "http://www.opentravel.org/OTA/2003/05"}
            n = root.find(".//ota:ImageItems/ota:ImageItem/ota:ImageFormat/ota:URL", namespaces=ns)
            if n is not None and (n.text or "").strip():
                return n.text.strip()
        except Exception:
            return None
        return None

    def _resolve_image(core: str) -> str | None:
        # precedenza: immagine arrivata dal form → DB → API descrittiva
        return image_from_form or _db_image_for_core(core) or _api_image_for_core(core)

    # ---------- guests ----------
    guests = []
    rph = 1
    for i in range(max(adults, 0)):
        guests.append({
            "rph": str(rph), "birthdate": bd_from_age_on(start_date, 35),
            "given": f"Adult{i+1}", "surname": "Guest", "email": f"adult{i+1}@example.invalid"
        }); rph += 1
    for j, age in enumerate(children_ages, start=1):
        guests.append({
            "rph": str(rph), "birthdate": bd_from_age_on(start_date, age),
            "given": f"Child{j}", "surname": "Guest", "email": f"child{j}@example.invalid"
        }); rph += 1

    res_id_value = aptfrom or (cfg.get("departure_default") or "ANY")

    # ---------- build XML Quote ----------
    try:
        payload_xml: bytes = build_quote_xml(
            cfg,
            booking_code=booking_code,
            start_date=start_date,
            end_date=end_date,
            guests=guests,
            res_id_value=res_id_value,
        )
    except Exception as ex:
        abort(500, description=f"Errore build XML Quote: {ex}")

    # ---------- call RES ----------
    def _res_url(base: str) -> str:
        base = (base or "").rstrip("/")
        if base.lower().endswith("/otaservice"):
            return f"{base}/TourActivityRes"
        return f"{base}/OtaService/TourActivityRes"

    url = _res_url(base_url)
    headers = {
        "Authorization": f"Bearer {bearer}",
        "Content-Type": "application/xml; charset=utf-8",
        "Accept": "application/xml",
    }
    print(f"[quote_by_code] POST URL (RES): {url}", flush=True)

    try:
        resp = requests.post(url, data=payload_xml, headers=headers, timeout=cfg.get("timeout", 40))
    except requests.RequestException as ex:
        abort(502, description=f"Errore rete verso OCTO: {ex}")

    if resp.status_code != 200:
        abort(resp.status_code, description=f"Errore OCTO Quote ({resp.status_code}): {resp.text[:800]}")

    # ---------- parse + enrich ----------
    try:
        parsed = parse_quote_full(resp.content)
    except Exception as ex:
        abort(502, description=f"Impossibile interpretare la risposta Quote: {ex}")

    offers = (parsed.get("offers") if isinstance(parsed, dict) else None) or []

    core_from_booking = _core_from_booking_code(booking_code)
    for o in offers:
        o.setdefault("start", start_date)
        o.setdefault("end", end_date)
        bc = o.get("booking_code") or booking_code
        core = _core_from_booking_code(bc) or core_from_booking
        o.setdefault("tour_activity_code", core)
        if not o.get("image"):
            img = _resolve_image(core)
            if img:
                o["image"] = img

    meta = {
        "currency": (parsed.get("currency") if isinstance(parsed, dict) else None),
        "raw_errors": (parsed.get("errors") if isinstance(parsed, dict) else None),
        "request": {
            "aptfrom": aptfrom or "ANY",
            "destina": destina,
            "start_date": start_date,
            "end_date": end_date,
            "nights": nights,
            "rooms": rooms,
            "adults": adults,
            "children_ages": children_ages,
        },
    }

    return render_template("availability/search_results.html", offers=offers, meta=meta)


# ------------------------------------------------------------
# Dettaglio prodotto: da package_code (es. 0000RMFALEXAN#MXP2)
# ------------------------------------------------------------
@bp.route("/product_detail", methods=["POST"], endpoint="product_detail")
@login_required
def product_detail():
    import re, requests, json
    from decimal import Decimal, InvalidOperation
    from lxml import etree as ET
    from flask import abort, current_app, request, render_template

    OTA_NS = "http://www.opentravel.org/OTA/2003/05"
    ns = {"ota": OTA_NS}

    package_code = (request.form.get("package_code") or "").strip()
    product_core = (request.form.get("product_core") or "").strip()
    start_date   = (request.form.get("start_date") or "").strip()
    end_date     = (request.form.get("end_date") or "").strip()
    aptfrom      = (request.form.get("aptfrom") or "").upper().strip()

    try:
        adults = int(request.form.get("adults") or 2)
    except Exception:
        adults = 2

    # bambini
    s_ages = (request.form.get("children_ages") or "").strip()
    children_ages = []
    if s_ages:
        for p in re.split(r"[,\s;]+", s_ages):
            try:
                children_ages.append(int(p))
            except Exception:
                pass

    # immagine di fallback passata dal form
    image_from_form = (request.form.get("image") or "").strip() or None

    if not package_code or not start_date or not end_date:
        abort(400, description="Parametri mancanti per il dettaglio prodotto")

    cfg = get_cfg()
    base_url         = cfg["base_url"]
    bearer           = cfg["bearer"]
    target           = cfg["target"]
    primary_lang_id  = cfg["primary_lang_id"]
    market_country   = cfg["market_country_code"]
    requestor_id     = cfg["requestor_id"]
    message_password = cfg["message_password"]
    chain_code       = cfg["chain_code"]
    product_type     = cfg.get("product_type") or "Tour"
    category_code    = cfg.get("category_code") or "211"
    timeout_sec      = cfg.get("timeout", 40)

    flight_vettore = (request.form.get("flight_vettore") or "").strip() or None
    pp_price       = (request.form.get("pp_price") or "").strip() or None
    pp_currency    = (request.form.get("currency") or "").strip() or None

    try:
        flights_selected = json.loads(request.form.get("flights_json") or "[]")
    except Exception:
        flights_selected = []

    # -------- helpers --------
    def _avail_url(base: str) -> str:
        base = (base or "").rstrip("/")
        return f"{base}/TourActivityAvail" if base.lower().endswith("/otaservice") else f"{base}/OtaService/TourActivityAvail"

    def _di_url(base: str) -> str:
        base = (base or "").rstrip("/")
        return f"{base}/TourActivityDescriptiveInfo" if base.lower().endswith("/otaservice") else f"{base}/OtaService/TourActivityDescriptiveInfo"

    def _pretty_xml_bytes(b: bytes) -> str:
        try:
            parser = ET.XMLParser(remove_blank_text=True, recover=True)
            root = ET.fromstring(b, parser=parser)
            return ET.tostring(root, pretty_print=True, encoding="unicode")
        except Exception:
            try:
                return b.decode("utf-8", errors="ignore")
            except Exception:
                return str(b)

    def _to_dec(x):
        """Decimal robusto su stringhe tipo '2,048.00' o '2048.00'."""
        if x is None:
            raise InvalidOperation
        s = str(x).strip().replace(" ", "")
        # Se ci sono sia '.' che ',', prova a rimuovere i separatori migliaia più comuni
        if s.count(",") > 0 and s.count(".") > 0:
            # ipotesi: '.' migliaia, ',' decimale  ->  '2,048.00' non comune in EU, gestiamo entrambi i versi:
            # proviamo prima lo stile EU: '.' migliaia, ',' decimale
            try:
                return Decimal(s.replace(".", "").replace(",", "."))
            except InvalidOperation:
                pass
        # stile US/EU semplice
        try:
            return Decimal(s.replace(",", ""))
        except InvalidOperation:
            return Decimal("Infinity")

    # ---- Avail filtrata per TourActivityCode = package_code ----
    E = ET.Element
    rq = E("{%s}OTAX_TourActivityAvailRQ" % OTA_NS,
           Target=target, PrimaryLangID=primary_lang_id, MarketCountryCode=market_country,
           nsmap={None: OTA_NS})
    pos = E("{%s}POS" % OTA_NS); src = E("{%s}Source" % OTA_NS)
    pos.append(src); rq.append(pos)
    src.append(E("{%s}RequestorID" % OTA_NS, ID=requestor_id, MessagePassword=message_password))

    av_segs = E("{%s}AvailRequestSegments" % OTA_NS); rq.append(av_segs)
    seg = E("{%s}AvailRequestSegment" % OTA_NS); av_segs.append(seg)

    tasc = E("{%s}TourActivitySearchCriteria" % OTA_NS); seg.append(tasc)
    crit = E("{%s}Criterion" % OTA_NS); tasc.append(crit)

    crit.append(E("{%s}TourActivityRef" % OTA_NS,
                  ChainCode=chain_code, ProductType=product_type, CategoryCode=category_code,
                  TourActivityCode=package_code))

    seg.append(E("{%s}StayDateRange" % OTA_NS, Start=start_date, End=end_date))

    acs = E("{%s}ActivityCandidates" % OTA_NS); seg.append(acs)
    ac = E("{%s}ActivityCandidate" % OTA_NS, Quantity="1", RPH="01"); acs.append(ac)
    gcs = E("{%s}GuestCounts" % OTA_NS); ac.append(gcs)
    for _ in range(max(adults, 0)):
        gcs.append(E("{%s}GuestCount" % OTA_NS, Age="50", Count="1"))
    for age in children_ages:
        gcs.append(E("{%s}GuestCount" % OTA_NS, Age=str(age), Count="1"))

    payload_xml = ET.tostring(rq, xml_declaration=True, encoding="utf-8", pretty_print=True)

    url = _avail_url(base_url)
    headers = {"Authorization": f"Bearer {bearer}",
               "Content-Type": "application/xml; charset=utf-8",
               "Accept": "application/xml"}

    try:
        resp = requests.post(url, data=payload_xml, headers=headers, timeout=timeout_sec)
    except requests.RequestException as ex:
        abort(502, description=f"Errore rete verso OCTO: {ex}")
    if resp.status_code != 200:
        abort(resp.status_code, description=f"Errore OCTO Avail ({resp.status_code}): {resp.text[:600]}")

    request_xml_pretty  = payload_xml.decode("utf-8", errors="ignore")
    response_xml_pretty = _pretty_xml_bytes(resp.content or b"")

    # ---- Parse + GALLERY ----
    try:
        root = ET.fromstring(resp.content)
        product_name = None
        room_options = []

        # mappa rateplan: code -> {"name":..., "meal":...}
        def _rateplan_map(activity_el):
            m = {}
            for rp in activity_el.findall("ota:RatePlans/ota:RatePlan", namespaces=ns):
                rpc = (rp.get("RatePlanCode") or "").strip()     # es. "SS-FB"
                rpn = (rp.get("RatePlanName") or "") or None
                meal = None
                mi = rp.find("ota:MealsIncluded", namespaces=ns)
                if mi is not None:
                    meal = mi.get("MealPlanCodes") or mi.get("MealPlanCode") or mi.get("MealPlanIndicator")
                if rpc:
                    m[rpc] = {"name": rpn, "meal": meal}
            return m

        def _resolve_plan_info(rp_map, rate_plan_code):
            """Accetta 'SS-FB' o 'DBLR|SS-FB' e ritorna dict con name/meal + short code."""
            full = (rate_plan_code or "").strip()
            short = full.split("|")[-1] if full else ""
            info = rp_map.get(full) or rp_map.get(short) or {"name": None, "meal": None}
            info = dict(info)  # copy
            info["short"] = short
            info["full"] = full
            return info

        # parse attività
        for act in root.findall(".//ota:Activities/ota:Activity", namespaces=ns):
            bpi = act.find("ota:BasicPropertyInfo", namespaces=ns)
            if not product_name and bpi is not None:
                product_name = bpi.get("TourActivityName") or product_core

            # descrizione camera per codice (ActivityTypeCode -> Text)
            service_map = {}
            for at in act.findall("ota:ActivityTypes/ota:ActivityType", namespaces=ns):
                code = (at.get("ActivityTypeCode") or "").strip()
                tnode = at.find("ota:ActivityDescription/ota:Text", namespaces=ns)
                desc = (tnode.text or "").strip() if (tnode is not None and tnode.text) else ""
                if code and desc:
                    service_map[code] = desc

            rp_map = _rateplan_map(act)

            for ar in act.findall("ota:ActivityRates/ota:ActivityRate", namespaces=ns):
                # prezzo, currency
                def _extract_price_and_curr(ar_el):
                    tot = ar_el.find("ota:Total", namespaces=ns)
                    if tot is not None and (tot.get("AmountAfterTax") or tot.get("AmountBeforeTax")):
                        return (tot.get("AmountAfterTax") or tot.get("AmountBeforeTax")), (tot.get("CurrencyCode") or None)
                    # fallback: Rates/Rate/Base
                    base = ar_el.find("ota:Rates/ota:Rate/ota:Base", namespaces=ns)
                    if base is not None and (base.get("AmountAfterTax") or base.get("AmountBeforeTax")):
                        return (base.get("AmountAfterTax") or base.get("AmountBeforeTax")), (base.get("CurrencyCode") or None)
                    return None, None

                price_str, curr = _extract_price_and_curr(ar)

                # pricing type (se presente in Rates/Rate)
                rate = ar.find("ota:Rates/ota:Rate", namespaces=ns)
                pricing_type = (rate.get("PricingType") or None) if rate is not None else None

                rcode = (ar.get("ActivityTypeCode") or "").strip()      # es. DBLR
                rname = service_map.get(rcode)
                rp_code_full = (ar.get("RatePlanCode") or "").strip()   # es. DBLR|SS-FB (o solo SS-FB)
                plan = _resolve_plan_info(rp_map, rp_code_full)
                rp_name = plan.get("name")
                meal_codes = plan.get("meal")
                rp_short = plan.get("short")
                rp_full  = plan.get("full")

                bcode = (ar.get("BookingCode") or "").strip()

                room_options.append({
                    "room_code": rcode,
                    "room_name": rname,
                    "rate_plan_code": rp_full,       # completo del BE
                    "rate_plan_short": rp_short,     # SS-FB, SS-HB, ...
                    "rate_plan_name": rp_name,       # es. "MEZZA PENSIONE ..."
                    "meal_plan_codes": meal_codes,
                    "pricing_type": pricing_type,    # es. "Per stay"
                    "booking_code": bcode,
                    "total_price": price_str,
                    "currency": curr,
                })

        if not product_name:
            product_name = product_core or package_code

        # ---- DE-DUP sicura ----
        # 1) Se c'è BookingCode, è la chiave primaria (serve per distinguere FB/HB).
        # 2) Se manca, usa (room_code, rp_short, rate_plan_code) per non fondere trattamenti diversi.
        dedup = {}
        for r in room_options:
            key = r.get("booking_code") or (f"{r.get('room_code','')}|{r.get('rate_plan_short','')}|{r.get('rate_plan_code','')}")
            best = dedup.get(key)
            if best is None or _to_dec(r.get("total_price")) < _to_dec(best.get("total_price")):
                dedup[key] = r

        room_options = list(dedup.values())
        room_options.sort(key=lambda x: (_to_dec(x.get("total_price")), x.get("room_code") or "", x.get("rate_plan_short") or ""))

        # default: prima DBL* se esiste
        default_room_code = next((ro["room_code"] for ro in room_options if (ro.get("room_code") or "").startswith("DBL")), None)
        if not default_room_code and room_options:
            default_room_code = room_options[0].get("room_code")

        # -------- GALLERY: DescriptiveInfo (con immagini) + fallback core --------
        def _fetch_gallery(tac: str):
            E = ET.Element
            rq = E("{%s}OTAX_TourActivityDescriptiveInfoRQ" % OTA_NS,
                   Target=target, PrimaryLangID=primary_lang_id, MarketCountryCode=market_country,
                   nsmap={None: OTA_NS})
            pos = E("{%s}POS" % OTA_NS); src = E("{%s}Source" % OTA_NS)
            pos.append(src); rq.append(pos)
            src.append(E("{%s}RequestorID" % OTA_NS, ID=requestor_id, MessagePassword=message_password))

            infos = E("{%s}TourActivityDescriptiveInfos" % OTA_NS); rq.append(infos)
            info = E("{%s}TourActivityDescriptiveInfo" % OTA_NS, ChainCode=chain_code, TourActivityCode=tac)
            infos.append(info)

            tpa = E("{%s}TPA_Extensions" % OTA_NS); info.append(tpa)
            ret = E("{%s}ReturnImageItems" % OTA_NS); ret.text = "true"; tpa.append(ret)

            payload = ET.tostring(rq, xml_declaration=True, encoding="utf-8", pretty_print=True)
            try:
                r = requests.post(_di_url(base_url), data=payload, headers=headers, timeout=timeout_sec)
                if r.status_code != 200:
                    return []
                root2 = ET.fromstring(r.content)
                urls = []
                for n in root2.findall(".//ota:ImageItems/ota:ImageItem/ota:ImageFormat/ota:URL", namespaces=ns):
                    txt = (n.text or "").strip() if n is not None else ""
                    if txt:
                        urls.append(txt)
                return urls
            except Exception as e:
                current_app.logger.warning("Gallery fetch error: %s", e)
                return []

        gallery = _fetch_gallery(package_code) or []
        if not gallery and product_core:
            gallery = _fetch_gallery(product_core) or []

    except Exception as ex:
        abort(502, description=f"Errore nel parsing dettaglio: {ex}")

    # ====== render ======
    meta = {
        "request": {
            "package_code": package_code,
            "product_core": product_core,
            "start_date": start_date,
            "end_date": end_date,
            "aptfrom": aptfrom,
            "adults": adults,
            "children_ages": children_ages,
        }
    }

    product_name = product_name or (product_core or package_code)
    room_options = room_options or []
    default_room_code = default_room_code or (room_options[0]["room_code"] if room_options else None)
    gallery = gallery or ([image_from_form] if image_from_form else [])

    return render_template(
        "availability/product_detail.html",
        product_name=product_name,
        package_code=package_code,
        gallery=gallery,
        room_options=room_options,
        default_room_code=default_room_code,
        meta=meta,
        flights_selected=flights_selected,
        flight_vettore=flight_vettore,
        pp_price=pp_price,
        pp_currency=pp_currency,
        # ↓ per accordion debug a fondo pagina
        request_xml=request_xml_pretty,
        response_xml=response_xml_pretty,
    )


def pick_departure_airports(dest_code: str, aptfrom: str | None, start_date: str, nights: int) -> list[str]:
    # Se l'utente ha selezionato un aeroporto specifico, usalo e basta
    if aptfrom:
        return [aptfrom]

    # 1) Prova dalla cache partenze (richiede che departures_cache abbia city_code)
    rows = db.session.execute(_sql("""
        SELECT DISTINCT depart_airport
        FROM departures_cache
        WHERE city_code = :dest
          AND duration_days = :nights
          AND date(depart_date) = date(:start)
        ORDER BY depart_airport
    """), {"dest": dest_code, "nights": nights, "start": start_date}).fetchall()
    airports = [r[0] for r in rows if r[0]]

    if airports:
        return airports

    # 2) Fallback: deduci dagli OTAProduct (dopo il '#', 3 lettere)
    rows2 = db.session.execute(_sql("""
        SELECT DISTINCT SUBSTR(product_code, INSTR(product_code, '#') + 1, 3) AS apt
        FROM ota_products
        WHERE city_code = :dest
        ORDER BY apt
    """), {"dest": dest_code}).fetchall()
    airports2 = [r[0] for r in rows2 if r[0]]

    return airports2
