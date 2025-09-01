# app/web/quote.py

import uuid
from datetime import date, datetime, timedelta

from flask import Blueprint, request, render_template, current_app, flash
from flask_login import login_required
from sqlalchemy import text as _sql
from lxml import etree as ET

from ..extensions import db
from ..services.runtime import get_setting_safe
from ..services.ota_io import build_quote_xml, post_ota_xml

bp = Blueprint("quote", __name__, url_prefix="/quote")


# --------------------- helpers ---------------------


def _safe_dob_from_age_at(ref_date: date, age_years: int) -> date:
    """
    Restituisce una data di nascita tale che all ref_date l'eta compiuta sia age_years.
    Per infant (<1 anno) usa circa 6-7 mesi prima.
    Gestisce fine mese (31 -> 30/29/28).
    """
    if age_years <= 0:
        return ref_date - timedelta(days=200)  # ~6-7 mesi

    y, m, d = ref_date.year - age_years, ref_date.month, ref_date.day
    while True:
        try:
            dob = date(y, m, d)
            break
        except ValueError:
            d -= 1
            if d <= 27:
                dob = date(y, m, d)
                break
    # gia compiuti all ref_date
    return dob - timedelta(days=1)


def _build_fake_guests(adults_cnt: int, ch_ages, start_str: str):
    """
    Crea un set di ospiti fittizi nel formato atteso da build_quote_xml:
    chiavi: given, surname, birthdate (ISO), email, rph (string).
    """
    try:
        ref = date.fromisoformat(start_str)
    except Exception:
        ref = date.today()

    guests = []
    rph = 1

    # Adulti ~35 anni
    for i in range(max(0, adults_cnt)):
        guests.append({
            "rph": str(rph),
            "given": f"TEST{i+1}",
            "surname": "SANDT",
            "birthdate": _safe_dob_from_age_at(ref, 35).isoformat(),
            "email": "test@mail.com",
        })
        rph += 1

    # Bambini / Infant (eta alla partenza)
    for j, age in enumerate(ch_ages or [], start=1):
        try:
            age_i = int(age)
        except Exception:
            age_i = 8
        guests.append({
            "rph": str(rph),
            "given": f"{'INF' if age_i < 2 else 'CHILD'}{j}",
            "surname": "SANDT",
            "birthdate": _safe_dob_from_age_at(ref, max(age_i, 0)).isoformat(),
            "email": "test@mail.com",
        })
        rph += 1

    return guests


def _normalize_guests_for_build(guests: list[dict]) -> list[dict]:
    """
    Garantisce le chiavi usate da build_quote_xml:
    - given, surname, birthdate, email, rph (string)
    Esegue mapping da alias comuni (first_name/given_name, last_name, birth_date/dob).
    """
    norm = []
    seq = 1
    for g in guests or []:
        h = dict(g)

        # given
        if "given" not in h:
            h["given"] = h.get("given_name") or h.get("first_name") or "TEST"

        # surname
        if "surname" not in h:
            h["surname"] = h.get("last_name") or h.get("family_name") or "USER"

        # birthdate
        if "birthdate" not in h:
            bd = h.get("birth_date") or h.get("dob")
            if isinstance(bd, datetime):
                bd = bd.date().isoformat()
            elif isinstance(bd, date):
                bd = bd.isoformat()
            h["birthdate"] = bd or "1989-01-01"

        # email
        h["email"] = h.get("email") or "test@mail.com"

        # rph come stringa e sequenziale se mancante
        h["rph"] = str(h.get("rph") or seq)
        seq += 1

        norm.append(h)
    return norm


def _ensure_code_with_apt(code: str, apt: str, start_date: str, nights: int):
    code = (code or "").strip()
    apt = (apt or "").strip().upper()
    if "#" in code:
        return code
    if apt:
        return f"{code}#{apt}"
    row = db.session.execute(_sql("""
        SELECT depart_airport
        FROM departures_cache
        WHERE product_code = :code
          AND depart_date = :start
          AND duration_days = :nights
        LIMIT 1
    """), {"code": code, "start": start_date, "nights": nights}).fetchone()
    return f"{code}#{row.depart_airport}" if row else code


def _make_booking_code(product_code: str, start_date: str, nights: int) -> str:
    seed = uuid.uuid4().hex[:6].upper()
    return f"BK-{start_date.replace('-','')}-{nights}-{product_code.split('#')[0][-4:]}-{seed}"


def _build_fake_pax(adults: int, child_ages):
    """Pax fake congruenti con occupazione search (non usato nella RES, lasciato per utilita)."""
    pax = []
    now_year = datetime.utcnow().year

    # Adulti
    for i in range(adults):
        pax.append({
            "type": "ADT",
            "title": "MR" if i % 2 == 0 else "MS",
            "first_name": f"TEST{i+1}",
            "last_name": "USER",
            "birth_date": "1989-01-01",
        })

    # Bambini
    for j, age in enumerate(child_ages, start=1):
        age_i = int(age) if str(age).strip().isdigit() else 8
        year = max(2010, now_year - age_i)
        pax.append({
            "type": "CHD",
            "title": "MSTR" if age_i < 12 else "MISS",
            "first_name": f"CHILD{j}",
            "last_name": "USER",
            "birth_date": f"{year}-06-01",
            "age": age_i,
        })
    return pax


def _as_float(x, default=0.0):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return default


def _parse_quote_minimal(xml_bytes: bytes):
    """
    Parser ricco per OTAX_TourActivityResRS.
    Estrae: success, currency, grand_total, services, taxes/fees,
    property, room, rateplan, images, note, flights, itineraries,
    cancel_policies, price_age_bands, reservation_ids.
    """
    out = {
        "success": False,
        "currency": "EUR",
        "services": [],
        "taxes": [],
        "fees": [],
        "grand_total": None,
        "property": {},
        "room": {},
        "rateplan": {},
        "images": [],
        "note": None,
        "flights": [],
        "itineraries": [],
        "cancel_policies": [],
        "price_age_bands": [],
        "reservation_ids": [],
    }

    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return out

    ns = {"ota": "http://www.opentravel.org/OTA/2003/05"}

    # Success
    out["success"] = root.find(".//ota:Success", ns) is not None

    # Grand total e currency (globale)
    tot_global = root.find(".//ota:ResGlobalInfo/ota:Total", ns) or root.find(".//ota:Total", ns)
    if tot_global is not None:
        cur = tot_global.get("CurrencyCode")
        if cur:
            out["currency"] = cur
        amt = tot_global.get("AmountAfterTax") or tot_global.get("Amount")
        if amt:
            out["grand_total"] = _as_float(amt)

    # LineItem (generico, se il fornitore li usa)
    for li in root.findall(".//ota:LineItem", ns):
        name = (li.get("Name") or "").strip()
        if not name:
            desc = li.findtext(".//ota:Description", default="", namespaces=ns)
            name = (desc or "Service").strip()

        code = (li.get("Code") or "").strip()
        category = (li.get("Category") or "Service").strip()

        qty = 1
        qn = li.find(".//ota:Quantity", ns)
        if qn is not None and (qn.get("Quantity") or qn.text):
            try:
                qty = int((qn.get("Quantity") or qn.text).strip())
            except Exception:
                qty = 1

        unit = total = None
        price = li.find(".//ota:Price", ns)
        if price is not None:
            unit_attr = price.get("AmountBeforeTax") or price.get("Amount")
            total_attr = price.get("AmountAfterTax") or price.get("Amount")
            if unit_attr:
                unit = _as_float(unit_attr)
            if total_attr:
                total = _as_float(total_attr)
        if unit is None and total is not None and qty:
            unit = round(total / qty, 2)

        out["services"].append({
            "code": code or name[:16],
            "name": name,
            "qty": qty,
            "unit_price": unit,
            "total": total if total is not None else (unit * qty if unit is not None else None),
            "category": category,
        })

    # Attivita principale
    act = root.find(".//ota:Activity", ns)
    if act is not None:
        # Property info
        bpi = act.find("ota:BasicPropertyInfo", ns)
        if bpi is not None:
            prop = {
                "name": bpi.get("TourActivityName"),
                "code": bpi.get("TourActivityCode"),
                "city_code": bpi.get("TourActivityCityCode"),
                "product_type": bpi.get("ProductType"),
                "product_type_code": bpi.get("ProductTypeCode"),
                "product_type_name": bpi.get("ProductTypeName"),
                "category_code": bpi.get("CategoryCode"),
                "category_detail": bpi.get("CategoryCodeDetail"),
                "city_name": None,
                "country_code": None,
                "country_name": None,
            }
            addr = bpi.find("ota:Address", ns)
            if addr is not None:
                cn = addr.find("ota:CountryName", ns)
                if cn is not None:
                    prop["country_code"] = cn.get("Code")
                    prop["country_name"] = (cn.text or "").strip() if cn.text else None
                city = addr.find("ota:CityName", ns)
                if city is not None and city.text:
                    prop["city_name"] = city.text.strip()
            out["property"] = {k: v for k, v in prop.items() if v}

        # Room
        at = act.find("ota:ActivityTypes/ota:ActivityType", ns)
        if at is not None:
            out["room"] = {
                "code": at.get("ActivityTypeCode"),
                "name": (at.findtext("ota:ActivityDescription/ota:Text", default="", namespaces=ns) or None)
            }

        # RatePlan
        rp = act.find("ota:RatePlans/ota:RatePlan", ns)
        if rp is not None:
            meals = rp.find("ota:MealsIncluded", ns)
            out["rateplan"] = {
                "code": rp.get("RatePlanCode"),
                "name": rp.get("RatePlanName"),
                "meal_plan_codes": meals.get("MealPlanCodes") if meals is not None else None
            }

        # Images
        for u in act.findall(".//ota:TPA_Extensions/ota:ImageItems/ota:ImageItem/ota:ImageFormat/ota:URL", ns):
            if u is not None and u.text and u.text.strip():
                out["images"].append(u.text.strip())

        # Note (SourceID='NOTE' o prima description disponibile)
        note = act.find(".//ota:TPA_Extensions/ota:TextItems/ota:TextItem[@SourceID='NOTE']/ota:Description", ns)
        if note is not None and note.text and note.text.strip():
            out["note"] = note.text.strip()
        if not out["note"]:
            anydesc = act.find(".//ota:TextItems/ota:TextItem/ota:Description", ns)
            if anydesc is not None and anydesc.text and anydesc.text.strip():
                out["note"] = anydesc.text.strip()

        # PriceAgeBands
        for pab in act.findall(".//ota:TPA_Extensions/ota:PriceAgeBands/ota:PriceAgeBand", ns):
            out["price_age_bands"].append({
                "min": pab.get("min"),
                "max": pab.get("max"),
            })

        # Flights
        aid = act.find(".//ota:TPA_Extensions/ota:AirItineraries/ota:AirItineraryDetail", ns)
        if aid is not None:
            odos = aid.find("ota:OriginDestinationOptions", ns)
            if odos is not None:
                for od in odos.findall("ota:OriginDestinationOption", ns):
                    od_rph = od.get("RPH")
                    for seg in od.findall("ota:FlightSegment", ns):
                        dep = seg.find("ota:DepartureAirport", ns)
                        arr = seg.find("ota:ArrivalAirport", ns)
                        op = seg.find("ota:OperatingAirline", ns)
                        mk = seg.find("ota:MarketingAirline", ns)
                        bag = seg.find("ota:TPA_Extensions/ota:Baggage/ota:Weight", ns)
                        out["flights"].append({
                            "od_rph": od_rph,
                            "departure": {
                                "datetime": seg.get("DepartureDateTime"),
                                "airport": (dep.get("LocationCode") if dep is not None else None),
                                "name": (dep.get("LocationName") if dep is not None else None),
                            },
                            "arrival": {
                                "datetime": seg.get("ArrivalDateTime"),
                                "airport": (arr.get("LocationCode") if arr is not None else None),
                                "name": (arr.get("LocationName") if arr is not None else None),
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
                                "unit": ((bag.text.strip() if (bag is not None and bag.text) else None)),
                            },
                        })

        # Itineraries (giorno per giorno)
        for iti in act.findall(".//ota:TPA_Extensions/ota:Itineraries/ota:Itinerary", ns):
            item = {"label": iti.get("LocalityName")}
            dnode = iti.find(".//ota:TextItems/ota:TextItem/ota:Description", ns)
            if dnode is not None and dnode.text and dnode.text.strip():
                item["text"] = dnode.text.strip()
            dests = []
            for d in iti.findall(".//ota:Destinations/ota:Destination", ns):
                dests.append({
                    "code": d.get("Code"),
                    "name": d.get("Name"),
                    "country": d.get("CountryISOCode"),
                    "lat": d.get("Latitude"),
                    "lng": d.get("Longitude"),
                })
            if dests:
                item["destinations"] = dests
            out["itineraries"].append(item)

        # ActivityRate -> riga tariffa (se non gia coperta da LineItem)
        for ar in act.findall("ota:ActivityRates/ota:ActivityRate", ns):
            tnode = ar.find("ota:Total", ns)
            if tnode is not None:
                cur = tnode.get("CurrencyCode")
                if cur:
                    out["currency"] = cur
                amt = (tnode.get("AmountAfterTax") or
                       tnode.get("AmountBeforeTax") or
                       tnode.get("Amount"))
                if amt:
                    amt_f = _as_float(amt)
                    room_name = out["room"].get("name") if out.get("room") else None
                    rateplan_name = out["rateplan"].get("name") if out.get("rateplan") else None
                    display_name = (room_name or out.get("room", {}).get("code") or "Package Rate")
                    if rateplan_name:
                        display_name = f"{display_name} · {rateplan_name}"
                    code = ar.get("ActivityTypeCode") or (out.get("room", {}).get("code") or "")
                    rpc = out.get("rateplan", {}).get("code")
                    code = f"{code}|{rpc}" if code and rpc else (rpc or code or "RATE")
                    out["services"].append({
                        "code": code[:32],
                        "name": display_name,
                        "qty": 1,
                        "unit_price": amt_f,
                        "total": amt_f,
                        "category": "Rate",
                    })

            # Tasse/Fee a livello di rate
            for tx in ar.findall(".//ota:Taxes/ota:Tax", ns):
                name = tx.get("Description") or tx.get("Code") or "Tax"
                a = tx.get("Amount") or tx.text
                val = _as_float(a)
                out["taxes"].append({"name": name, "amount": val})
                out["services"].append({
                    "code": (tx.get("Code") or name[:12]),
                    "name": name,
                    "qty": 1,
                    "unit_price": val,
                    "total": val,
                    "category": "Tax",
                })
            for fee in ar.findall(".//ota:Fees/ota:Fee", ns):
                name = fee.get("Description") or fee.get("Code") or "Fee"
                a = fee.get("Amount") or fee.text
                val = _as_float(a)
                out["fees"].append({"name": name, "amount": val})
                out["services"].append({
                    "code": (fee.get("Code") or name[:12]),
                    "name": name,
                    "qty": 1,
                    "unit_price": val,
                    "total": val,
                    "category": "Fee",
                })

    # Tasse/Fee globali
    for tx in root.findall(".//ota:Taxes/ota:Tax", ns):
        name = tx.get("Description") or tx.get("Code") or "Tax"
        a = tx.get("Amount") or tx.text
        val = _as_float(a)
        out["taxes"].append({"name": name, "amount": val})
        out["services"].append({
            "code": (tx.get("Code") or name[:12]),
            "name": name,
            "qty": 1,
            "unit_price": val,
            "total": val,
            "category": "Tax",
        })

    for fee in root.findall(".//ota:Fees/ota:Fee", ns):
        name = fee.get("Description") or fee.get("Code") or "Fee"
        a = fee.get("Amount") or fee.text
        val = _as_float(a)
        out["fees"].append({"name": name, "amount": val})
        out["services"].append({
            "code": (fee.get("Code") or name[:12]),
            "name": name,
            "qty": 1,
            "unit_price": val,
            "total": val,
            "category": "Fee",
        })

    # Cancel policies
    for cp in root.findall(".//ota:ResGlobalInfo/ota:CancelPenalties/ota:CancelPenalty", ns):
        dl = cp.find("ota:Deadline", ns)
        ap = cp.find("ota:AmountPercent", ns)
        out["cancel_policies"].append({
            "non_refundable": (cp.get("NonRefundable") or "").lower() == "true",
            "deadline": {
                "unit": dl.get("OffsetTimeUnit") if dl is not None else None,
                "multiplier": dl.get("OffsetUnitMultiplier") if dl is not None else None,
                "drop_time": dl.get("OffsetDropTime") if dl is not None else None,
            },
            "amount_percent": {
                "basis": ap.get("BasisType") if ap is not None else None,
                "percent": ap.get("Percent") if ap is not None else None,
            },
        })

    # Reservation IDs
    for rid in root.findall(".//ota:ResGlobalInfo/ota:TourActivityReservationIDs/ota:TourActivityReservationID", ns):
        out["reservation_ids"].append({
            "type": rid.get("ResID_Type"),
            "value": rid.get("ResID_Value"),
        })

    # Dedup leggero
    def _dedup(items):
        seen = set()
        outl = []
        for it in items:
            key = (it.get("name"), it.get("amount"))
            if key in seen:
                continue
            seen.add(key)
            outl.append(it)
        return outl

    out["taxes"] = _dedup(out["taxes"])
    out["fees"] = _dedup(out["fees"])

    return out


# --------------------- route ---------------------


def _to_result_view(parsed: dict, booking_code: str, start_date: str, end_date: str, guests_for_req=None) -> dict:
    """
    Converte il dict 'parsed' di _parse_quote_minimal nello shape che usa il template quote_result.html.
    """
    p = parsed or {}
    prop = p.get("property") or {}
    room = p.get("room") or {}
    rate = p.get("rateplan") or {}

    # base object: DEFINITO SUBITO
    view = {
        "success": bool(p.get("success")),
        "errors": [],
        "total": p.get("grand_total"),
        "currency": p.get("currency") or "EUR",
        "booking_code": booking_code,
        "product": {
            "name": prop.get("name"),
            "code": prop.get("code"),
            "address": {"city": prop.get("city_name")},
            "city_code": prop.get("city_code"),
            "type": prop.get("product_type"),
            "type_name": prop.get("product_type_name"),
            "category_code": prop.get("category_code"),
            "category_detail": prop.get("category_detail"),
        },
        "timespan": {"start": start_date, "end": end_date},
        "room": {
            "name": room.get("name"),
            "code": room.get("code"),
        },
        "rateplan": {
            "name": rate.get("name"),
            "code": rate.get("code"),
            "meals": rate.get("meal_plan_codes"),
        },
        "flights": [],
        "itinerary": [],
        "note": p.get("note"),
        "cancel_policy": None,
        "age_bands": [],
        "guests": [],
        "res_ids": [],
        "images": p.get("images") or [],
        "services": [],
        "taxes": p.get("taxes") or [],
        "fees": p.get("fees") or [],
    }

    # flights → shape atteso dal template
    for f in (p.get("flights") or []):
        view["flights"].append({
            "dep": {"code": (f.get("departure") or {}).get("airport"),
                    "name": (f.get("departure") or {}).get("name")},
            "arr": {"code": (f.get("arrival") or {}).get("airport"),
                    "name": (f.get("arrival") or {}).get("name")},
            "dep_datetime": (f.get("departure") or {}).get("datetime"),
            "arr_datetime": (f.get("arrival") or {}).get("datetime"),
            "flight_number": f.get("flight_number"),
            "class": f.get("booking_class"),
            "oper": {"code": (f.get("operating") or {}).get("code"),
                     "name": (f.get("operating") or {}).get("name")},
            "mkt":  {"code": (f.get("marketing") or {}).get("code"),
                     "name": (f.get("marketing") or {}).get("name")},
            "baggage_kg": (f.get("baggage") or {}).get("weight"),
        })

    # itinerario: prendi il primo destination come 'dest' per compatibilità template
    for it in (p.get("itineraries") or []):
        dest = None
        dests = it.get("destinations") or []
        if dests:
            d0 = dests[0]
            dest = {"code": d0.get("code"), "name": d0.get("name")}
        view["itinerary"].append({
            "label": it.get("label"),
            "text": it.get("text"),
            "dest": dest,
        })

    # cancel policy: usa la prima se presente
    cps = p.get("cancel_policies") or []
    if cps:
        cp = cps[0]
        view["cancel_policy"] = {
            "non_ref": bool(cp.get("non_refundable")),
            "deadline": {
                "unit": (cp.get("deadline") or {}).get("unit"),
                "multiplier": (cp.get("deadline") or {}).get("multiplier"),
                "drop_time": (cp.get("deadline") or {}).get("drop_time"),
            },
            "penalty": {
                "percent": (cp.get("amount_percent") or {}).get("percent"),
                "basis": (cp.get("amount_percent") or {}).get("basis"),
            },
        }

    # fasce età
    for b in (p.get("price_age_bands") or []):
        view["age_bands"].append({"min": b.get("min"), "max": b.get("max")})

        # --- GUESTS: preferisci quelli dal parsing, poi fallback a quelli mandati in request ---
    def _mk_name(d: dict) -> str:
        return (" ".join([
            (d.get("name") or "").strip(),
            (d.get("given") or d.get("first") or d.get("given_name") or "").strip(),
            (d.get("surname") or d.get("last") or "").strip(),
        ])).strip()

    guests_out = []

    # 1) prova a leggere dal parsing (diversi parser usano chiavi diverse)
    for g in (p.get("guests") or p.get("res_guests") or []):
        guests_out.append({
            "rph": str(g.get("rph") or g.get("ResGuestRPH") or g.get("rph_id") or ""),
            "name": _mk_name(g) or "Guest",
            "birth": g.get("birth") or g.get("birth_date") or g.get("BirthDate") or "",
            "email": g.get("email") or "",
        })

    # 2) fallback: usa i guest che abbiamo mandato nella request alla RES
    if not guests_out and guests_for_req:
        for i, g in enumerate(guests_for_req, start=1):
            guests_out.append({
                "rph": str(g.get("rph") or i),
                "name": _mk_name(g) or f"Guest {i}",
                "birth": g.get("birth") or g.get("birthdate") or g.get("birth_date") or "",
                "email": g.get("email") or "",
            })

    # 3) se qualche rph manca, normalizzalo in sequenza
    for i, g in enumerate(guests_out, start=1):
        if not g.get("rph"):
            g["rph"] = str(i)

    view["guests"] = guests_out


    # reservation ids
    for rid in (p.get("reservation_ids") or []):
        view["res_ids"].append({"type": rid.get("type"), "value": rid.get("value")})

    # services (nome, categoria, qty, unitario, subtotale)
    for s in (p.get("services") or []):
        qty = s.get("qty") or 1
        unit = s.get("unit_price")
        line = s.get("total")
        if line is None and unit is not None:
            try:
                line = round(float(unit) * int(qty), 2)
            except Exception:
                line = unit
        view["services"].append({
            "name": s.get("name"),
            "code": s.get("code"),
            "category": s.get("category"),
            "qty": qty,
            "unit": unit,
            "subtotal": line,
        })

    # se non success aggiungo un messaggio generico, per sicurezza
    if not view["success"] and not view["errors"]:
        view["errors"].append("Quotation failed or empty response.")

    return view



# helper minimal per garantire che 'result' esista sempre
def _empty_result(msg: str, currency: str = "EUR"):
    return {
        "success": False,
        "errors": [msg],
        "total": None,
        "currency": currency,
        "booking_code": None,
        "product": {
            "name": None, "code": None,
            "address": {"city": None},
            "city_code": None,
            "type": None, "type_name": None,
            "category_code": None, "category_detail": None,
        },
        "timespan": {"start": None, "end": None},
        "room": {}, "rateplan": {},
        "flights": [], "itinerary": [],
        "note": None,
        "cancel_policy": None,
        "age_bands": [],
        "guests": [],
        "res_ids": [],
        "images": [],
        "services": [], "taxes": [], "fees": [],
    }


@login_required
@bp.route("/create", methods=["POST"])
def create():
    s = get_setting_safe()
    if not s:
        return "OTA settings missing", 400

    # -------- input --------
    product_code = (request.form.get("product_code") or "").strip()
    booking_code_in = (request.form.get("booking_code") or "").strip()  # es: 0000...#MXP2|QGDBL|SS-ALL
    depart_airport = (request.form.get("depart_airport") or "").strip().upper()
    start_date = (request.form.get("start_date") or "").strip()
    end_date = (request.form.get("end_date") or "").strip()

    try:
        product_id = int(request.form.get("product_id") or 0)
    except Exception:
        product_id = 0

    try:
        nights = int(request.form.get("nights") or 0)
    except Exception:
        nights = 0

    currency = (request.form.get("currency") or "EUR").upper()
    rooms = int(request.form.get("rooms") or 1)
    adults = int(request.form.get("adults") or 2)

    raw_children = (request.form.get("children_ages") or "").strip()
    children_ages = [a.strip() for a in raw_children.split(",") if a.strip()] if raw_children else (request.form.getlist("child_age[]") or [])
    children_ages = [int(a) for a in children_ages if str(a).strip().isdigit()]

    if not booking_code_in:
        current_app.logger.warning("[quote] booking_code missing; form keys=%s", list(request.form.keys()))

    # Deduci APT da product/booking code se non passato
    core_from_booking = (booking_code_in.split("|", 1)[0] if booking_code_in else "")
    if not product_code and core_from_booking:
        product_code = core_from_booking  # es: 0000RMFXXX#MXP2
    if not depart_airport and "#" in product_code:
        depart_airport = product_code.split("#", 1)[1][:3].upper()

    # completa end_date se manca ma hai nights
    if not end_date and start_date and nights > 0:
        try:
            end_date = (date.fromisoformat(start_date) + timedelta(days=nights)).isoformat()
        except Exception:
            end_date = ""

    # ricalcola nights se 0 ma hai entrambe le date
    if nights <= 0 and start_date and end_date:
        try:
            nights = max((date.fromisoformat(end_date) - date.fromisoformat(start_date)).days, 0)
        except Exception:
            nights = 0

    current_app.logger.info(
        "[quote] in -> code=%s apt=%s start=%s end=%s nights=%s rooms=%s adt=%s chd=%s cur=%s",
        (product_code or core_from_booking), depart_airport, start_date, end_date, nights, rooms, adults, children_ages, currency
    )

    # guard-rail: booking_code mancante
    if not booking_code_in:
        flash("Booking code mancante: seleziona una camera/offerta.", "warning")
        return render_template(
            "quote/quote_result.html",
            product_id=product_id,
            result=_empty_result("Booking code mancante: seleziona una camera/offerta.", currency),
            request_xml="",
            response_xml="",
        )

    # guard-rail: date/notti non valide
    if not start_date or not end_date or nights <= 0:
        flash("Date/notti non valide per la quotazione.", "warning")
        return render_template(
            "quote/quote_result.html",
            product_id=product_id,
            result=_empty_result("Date/notti non valide per la quotazione.", currency),
            request_xml="",
            response_xml="",
        )

    # Normalizza product_code con #APT (per display/log; la RES usa booking_code)
    full_code = _ensure_code_with_apt(product_code, depart_airport, start_date, nights) if product_code else product_code
    if full_code and "#" not in full_code:
        flash("Attenzione: product_code privo di #APT, la quotazione potrebbe fallire.", "warning")

    # Guests coerenti
    guests = _build_fake_guests(adults_cnt=adults, ch_ages=children_ages, start_str=start_date)

    # id tecnico per la RES
    res_id_value = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    # shim cfg per build_quote_xml
    class _CfgShim:
        def __init__(self, src):
            def g(*names, default=None):
                for n in names:
                    v = getattr(src, n, None)
                    if v not in (None, "", 0):
                        return v
                return default
            self.base_url = g("base_url", "BaseUrl")
            self.target = g("target", "env", "environment", "mode", default="Production")
            self.primary_lang_id = g("primary_lang_id", "primary_lang", "language", "lang", "PrimaryLangID", default="it")
            self.market_country_code = g("market_country_code", "market_country", "country_code", "MarketCountryCode", default="it")
            self.requestor_id = g("requestor_id", "RequestorID", "requestor")
            self.message_password = g("message_password", "MessagePassword", "password")
            self.chain_code = g("chain_code", "ChainCode", "chain")
            self.product_type = g("product_type", "ProductType")
            self.category_code = g("category_code", "CategoryCode")
            self.timeout_seconds = g("timeout_seconds", "timeout", "timeout_sec", default=40)
            self.bearer = g("bearer", "bearer_token", "token", "Bearer")

    cfg = _CfgShim(s)

    missing = [k for k in ("requestor_id", "message_password", "chain_code", "primary_lang_id", "market_country_code")
               if not getattr(cfg, k, None)]
    if missing:
        current_app.logger.warning("[quote] cfg incompleto (mancano: %s)", ", ".join(missing))

    # Build XML
    try:
        rq_xml = build_quote_xml(
            cfg=cfg,
            booking_code=booking_code_in,
            start_date=start_date,
            end_date=end_date,
            guests=guests,
            res_id_value=res_id_value,
        )  # bytes
    except Exception as e:
        current_app.logger.exception("[quote] build_quote_xml error")
        return render_template(
            "quote/quote_result.html",
            product_id=product_id,
            result=_empty_result(f"XML build error: {e}", currency),
            request_xml="",
            response_xml="",
        )

    # POST RES
    def _res_url_from_base(base: str) -> str:
        b = (base or "").rstrip("/")
        if "/OtaService/OtaService/" in b:
            b = b.replace("/OtaService/OtaService/", "/OtaService/").rstrip("/")
        if b.lower().endswith("/otaservice"):
            return f"{b}/TourActivityRes"
        return f"{b}/OtaService/TourActivityRes"

    base = (getattr(cfg, "base_url", None) or s.base_url or "").rstrip("/")
    url = _res_url_from_base(base)

    headers = {"Content-Type": "application/xml; charset=utf-8", "Accept": "application/xml"}
    if getattr(cfg, "bearer", None):
        headers["Authorization"] = f"Bearer {cfg.bearer}"

    try:
        current_app.logger.info("[quote][RES] POST %s", url)
        res_xml = post_ota_xml(url, rq_xml, settings=s, headers=headers, timeout=getattr(cfg, "timeout_seconds", 40))
        current_app.logger.info("[quote][RES] OK %s (%d bytes)", url, len(res_xml or b""))
    except Exception as e:
        current_app.logger.exception("[quote] RES call error")
        return render_template(
            "quote/quote_result.html",
            product_id=product_id,
            result=_empty_result(f"RES call error: {e}", currency),
            request_xml=rq_xml.decode("utf-8", errors="ignore"),
            response_xml="",
        )

    # parse
    try:
        parsed = _parse_quote_minimal(res_xml)
        current_app.logger.info(
            "[quote][parsed] services=%d taxes=%d fees=%d flights=%d images=%d prop=%s room=%s rateplan=%s note=%s",
            len(parsed.get("services", [])),
            len(parsed.get("taxes", [])),
            len(parsed.get("fees", [])),
            len(parsed.get("flights", [])),
            len(parsed.get("images", [])),
            bool(parsed.get("property")),
            bool(parsed.get("room")),
            bool(parsed.get("rateplan")),
            bool(parsed.get("note")),
        )
    except Exception as e:
        current_app.logger.exception("[quote] parse error")
        parsed = {"success": False, "services": [], "taxes": [], "fees": [], "grand_total": None, "currency": currency}
        flash(f"Parse error: {e}", "warning")

    if not parsed.get("success"):
        flash(parsed.get("message") or "Quotation failed or empty response.", "warning")

    # --- DEBUG: quale file Jinja sta usando? ---
    try:
        src, filename, uptodate = current_app.jinja_env.loader.get_source(
            current_app.jinja_env, "quote/quote_result.html"
        )
        current_app.logger.info("[quote][tpl] template file resolved to: %s (uptodate=%s)", filename, uptodate)
    except Exception as e:
        current_app.logger.warning("[quote][tpl] cannot resolve template 'quote/quote_result.html': %s", e)

    # costruisci lo shape 'result'
    result = _to_result_view(
        parsed=parsed,
        booking_code=(booking_code_in or res_id_value),
        start_date=start_date,
        end_date=end_date,
        guests_for_req=guests,  # quelli che abbiamo mandato nella request
    )

    return render_template(
        "quote/quote_result.html",
        product_id=product_id,
        result=result,
        request_xml=rq_xml.decode("utf-8", errors="ignore"),
        response_xml=res_xml.decode("utf-8", errors="ignore"),
    )
