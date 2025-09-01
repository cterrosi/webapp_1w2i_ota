# app/services/ota_xml.py
import html
import re
from typing import Optional
from datetime import datetime, timedelta, date
from lxml import etree as ET
import inspect 

OTA_NS = "http://www.opentravel.org/OTA/2003/05"
NSMAP = {"ota": OTA_NS}
etree = ET  # alias

# ---------- BUILDERS / PARSERS: PRODUCT ----------
def build_ota_product_request(s) -> bytes:
    root = etree.Element("{%s}OTAX_TourActivityProductRQ" % OTA_NS,
                         Target=s.target, PrimaryLangID=s.primary_lang, nsmap={None: OTA_NS})
    pos = etree.SubElement(root, "{%s}POS" % OTA_NS)
    source = etree.SubElement(pos, "{%s}Source" % OTA_NS)
    etree.SubElement(source, "{%s}RequestorID" % OTA_NS,
                     ID=s.requestor_id, MessagePassword=s.message_password)

    attrs = {"ChainCode": s.chain_code, "ProductType": s.product_type, "CategoryCode": s.category_code}
    if s.tour_activity_code:
        attrs["TourActivityCode"] = s.tour_activity_code
    if s.city_code:
        attrs["TourActivityCityCode"] = s.city_code

    etree.SubElement(root, "{%s}TourActivityProducts" % OTA_NS, **attrs)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def build_ota_product_request_by_code(s, code: str) -> bytes:
    root = etree.Element("{%s}OTAX_TourActivityProductRQ" % OTA_NS,
                         Target=s.target, PrimaryLangID=s.primary_lang, nsmap={None: OTA_NS})
    pos = etree.SubElement(root, "{%s}POS" % OTA_NS)
    source = etree.SubElement(pos, "{%s}Source" % OTA_NS)
    etree.SubElement(source, "{%s}RequestorID" % OTA_NS, ID=s.requestor_id, MessagePassword=s.message_password)

    attrs = {
        "ChainCode": s.chain_code,
        "ProductType": s.product_type,
        "CategoryCode": s.category_code,
        "TourActivityCode": code,
    }
    if s.city_code:
        attrs["TourActivityCityCode"] = s.city_code

    etree.SubElement(root, "{%s}TourActivityProducts" % OTA_NS, **attrs)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def product_dict_to_detail(p: dict) -> dict:
    return {
        "name": p.get("TourActivityName") or "",
        "descriptions": [],
        "categories": [p.get("CategoryCode")] if p.get("CategoryCode") else [],
        "types": [p.get("ProductTypeName") or p.get("ProductType")] if (p.get("ProductTypeName") or p.get("ProductType")) else [],
        "duration": "",
        "pickup_notes": [],
        "image_urls": [],
        "city": p.get("TourActivityCityCode") or "",
        "country": f"{p.get('CountryISOCode','')} {p.get('CountryName','')}".strip(),
    }

# ---------- BUILDERS / PARSERS: SEARCH & DESCRIPTIVE ----------
def build_ota_search_by_code_request(s, code: str) -> bytes:
    root = etree.Element("{%s}OTAX_TourActivitySearchRQ" % OTA_NS,
                         Target=s.target, PrimaryLangID=s.primary_lang, nsmap={None: OTA_NS})
    pos = etree.SubElement(root, "{%s}POS" % OTA_NS)
    source = etree.SubElement(pos, "{%s}Source" % OTA_NS)
    etree.SubElement(source, "{%s}RequestorID" % OTA_NS, ID=s.requestor_id, MessagePassword=s.message_password)

    sc = etree.SubElement(root, "{%s}SearchCriteria" % OTA_NS)
    etree.SubElement(sc, "{%s}BasicInfo" % OTA_NS, SupplierProductCode=code)
    if s.city_code:
        constraints = etree.SubElement(sc, "{%s}Constraints" % OTA_NS)
        etree.SubElement(constraints, "{%s}City" % OTA_NS, Code=s.city_code)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def build_ota_descriptive_by_code_request(s, code: str) -> bytes:
    root = etree.Element("{%s}OTAX_TourActivityDescriptiveInfoRQ" % OTA_NS,
                         Target=s.target, PrimaryLangID=s.primary_lang, nsmap={None: OTA_NS})
    pos = etree.SubElement(root, "{%s}POS" % OTA_NS)
    source = etree.SubElement(pos, "{%s}Source" % OTA_NS)
    etree.SubElement(source, "{%s}RequestorID" % OTA_NS,
                     ID=s.requestor_id, MessagePassword=s.message_password)

    infos = etree.SubElement(root, "{%s}TourActivityDescriptiveInfos" % OTA_NS)
    info = etree.SubElement(infos, "{%s}TourActivityDescriptiveInfo" % OTA_NS,
                            ChainCode=s.chain_code,
                            TourActivityCode=(code or "").strip())

    # --- qui aggiungiamo le estensioni per richiedere immagini ---
    tpa = etree.SubElement(info, "{%s}TPA_Extensions" % OTA_NS)
    ret = etree.Element("{%s}ReturnImageItems" % OTA_NS)
    ret.text = "true"
    tpa.append(ret)

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def parse_ota_descriptive_detail(xml_bytes: bytes) -> dict:
    try:
        root = etree.fromstring(xml_bytes)
    except Exception as e:
        print("[descr-parse] XML parse error:", e, flush=True)
        return {}

    errs = root.xpath(".//*[local-name()='Errors']/*")
    if errs:
        print("[DESCR ERR]", [f"{e.get('Code','?')}:{(e.get('ShortText') or (e.text or '')).strip()}" for e in errs], flush=True)
        return {}

    def _txt(el): return (el.text or "").strip() if el is not None else ""
    def _attr(el, k, d=""): return (el.get(k) or d).strip() if el is not None else d

    containers = (
        root.xpath(".//*[local-name()='TourActivityDescriptiveContent']") or
        root.xpath(".//*[local-name()='ActivityDescriptiveContent']") or
        root.xpath(".//*[local-name()='TourActivityDescriptiveInfo']") or
        root.xpath(".//*[local-name()='ActivityDescriptiveInfo']") or
        []
    )
    content = containers[0] if containers else None

    if content is None:
        tai = root.xpath(".//*[local-name()='TourActivityInfo']")
        content = tai[0] if tai else None
        if content is None:
            print("[descr-parse] Nessun contenitore descrittivo trovato", flush=True)
            return {}

    ctx = (content.xpath(".//*[local-name()='TourActivityInfo']") or [content])[0]

    name = _attr(content, "TourActivityName") or _attr(ctx, "Name")
    city = _attr(content, "TourActivityCityCode") or _attr(ctx, "CityCode")
    country = (f"{_attr(content, 'CountryISOCode')} {_attr(content, 'CountryName')}".strip()
               or f"{_attr(ctx, 'CountryISOCode')} {_attr(ctx, 'CountryName')}".strip())

    descriptions = []
    for dn in ctx.xpath(".//*[local-name()='TextItem']/*[local-name()='Description']"):
        val = _txt(dn)
        if val:
            try:
                val = html.unescape(val)
            except Exception:
                pass
            descriptions.append(val)
    if not descriptions:
        for dn in ctx.xpath(".//*[local-name()='Description' and not(*)]"):
            val = _txt(dn)
            if val:
                try:
                    val = html.unescape(val)
                except Exception:
                    pass
                descriptions.append(val)

    image_urls = []
    for u in content.xpath(".//*[local-name()='ImageItems']//*[local-name()='URL'] | .//*[local-name()='Image']//*[local-name()='URL']"):
        url = _txt(u)
        if url:
            image_urls.append(url)

    categories = []
    for c in ctx.xpath(".//*[local-name()='TourActivityCategory']"):
        code = _attr(c, "Code") or _attr(c, "CodeDetail") or _attr(c, "Name")
        if code:
            categories.append(code)

    types_ = []
    t = _attr(content, "ProductTypeName") or _attr(content, "ProductType") or _attr(ctx, "ProductTypeName") or _attr(ctx, "ProductType")
    if t:
        types_.append(t)

    def _dedupe(seq):
        seen, out = set(), []
        for x in seq:
            if x and x not in seen:
                out.append(x)
                seen.add(x)
        return out

    # ---- durata in notti (robusta) ----
    from datetime import datetime as _dt

    def _parse_iso_date(s: str):
        s = (s or "").strip()
        if not s:
            return None
        # accetta "YYYY-MM-DD" oppure "YYYY-MM-DDTHH:MM..."
        try:
            return _dt.fromisoformat(s[:19]).date()
        except Exception:
            try:
                return _dt.fromisoformat(s[:10]).date()
            except Exception:
                return None

    def _first(xpath_expr: str, nodes):
        for n in nodes:
            res = n.xpath(xpath_expr)
            if res:
                return res[0]
        return None

    search_roots = [ctx, content, root]
    nights_val = None

    # 1) attributo Nights su qualunque nodo
    el = _first(".//*[local-name()='*'][@Nights][1]", search_roots)
    if el is not None:
        try:
            nights_val = int(el.get("Nights"))
        except Exception:
            pass

    # 2) nodo <Nights>testo</Nights>
    if nights_val is None:
        node = _first(".//*[local-name()='Nights'][normalize-space(text())!=''][1]", search_roots)
        if node is not None:
            try:
                nights_val = int((node.text or "").strip())
            except Exception:
                pass

    # 3) qualsiasi Duration/Value/Days + unità
    if nights_val is None:
        durn = _first(
            ".//*[contains(translate(local-name(), 'DURATION', 'duration'), 'duration')][@Value or @Duration or @Days or normalize-space(text())!=''][1]",
            search_roots
        )
        if durn is not None:
            raw = durn.get("Value") or durn.get("Duration") or durn.get("Days") or (durn.text or "").strip()
            unit = (durn.get("Unit") or durn.get("Units") or "").strip().lower()
            try:
                v = int(re.findall(r"\d+", str(raw))[0])
                if "night" in unit or "not" in unit:
                    nights_val = v
                elif "day" in unit or "giorn" in unit:
                    nights_val = max(0, v - 1)
            except Exception:
                pass
            if nights_val is None and isinstance(raw, str):
                m = re.search(r"(\d+)\s*(nights?|notti?)", raw, flags=re.I)
                if m:
                    nights_val = int(m.group(1))

    # 4) LengthOfStay
    if nights_val is None:
        los = _first(".//*[local-name()='LengthOfStay'][normalize-space(text())!=''][1]", search_roots)
        if los is not None:
            try:
                nights_val = int((los.text or "").strip())
            except Exception:
                pass
        if nights_val is None:
            losa = _first(".//*[local-name()='LengthOfStay'][@Nights or @Duration or @Days][1]", search_roots)
            if losa is not None:
                raw = losa.get("Nights") or losa.get("Duration") or losa.get("Days")
                try:
                    v = int(re.findall(r"\d+", str(raw))[0])
                    if losa.get("Days") and not losa.get("Nights"):
                        v = max(0, v - 1)
                    nights_val = v
                except Exception:
                    pass

    # 5) intervallo date Start/End
    if nights_val is None:
        for xp in [
            ".//*[local-name()='StayDateRange'][@Start and @End][1]",
            ".//*[local-name()='DateRange'][@Start and @End][1]",
            ".//*[local-name()='TimeSpan'][@Start and @End][1]",
        ]:
            dr = _first(xp, search_roots)
            if dr is not None:
                sd = _parse_iso_date(dr.get("Start"))
                ed = _parse_iso_date(dr.get("End"))
                if sd and ed:
                    diff = (ed - sd).days
                    if diff >= 0:
                        nights_val = diff
                        break

    duration = ""
    if isinstance(nights_val, int) and nights_val >= 0:
        duration = f"{nights_val} notte" if nights_val == 1 else f"{nights_val} notti"

    return {
        "name": name or "",
        "descriptions": _dedupe(descriptions),
        "categories": _dedupe(categories),
        "types": _dedupe(types_),
        "duration": duration or "",
        "pickup_notes": [],
        "image_urls": _dedupe(image_urls),
        "city": city or "",
        "country": country or "",
    }

# ---------- AVAILABILITY ----------
def build_availability_xml_from_product(
    *,
    requestor_id: str,
    message_password: str,
    chain_code: str,
    product_type: str,
    category_code: str,
    city_code: str,
    departure_loc: str,
    start_date: str,
    units: int,
    lengths_of_stay=(7, 14),
    tour_activity_code: Optional[str] = None,
    target: str,
    primary_lang_id: str,
    market_country_code: str,
) -> str:
    end_date = (datetime.fromisoformat(start_date) + timedelta(days=lengths_of_stay[0])).date().isoformat()
    los_xml = "\n".join([f"          <LengthOfStay>{n}</LengthOfStay>" for n in lengths_of_stay])
    guests_xml = "\n".join(['            <GuestCount Age="50" Count="1"/>' for _ in range(units)])
    tac_attr = f' TourActivityCode="{tour_activity_code}"' if tour_activity_code else ""

    return f'''<OTAX_TourActivityAvailRQ Target="{target}" PrimaryLangID="{primary_lang_id}" MarketCountryCode="{market_country_code}" xmlns="http://www.opentravel.org/OTA/2003/05">
  <POS>
    <Source>
      <RequestorID ID="{requestor_id}" MessagePassword="{message_password}"/>
    </Source>
  </POS>
  <AvailRequestSegments>
    <AvailRequestSegment>
      <TourActivitySearchCriteria>
        <Criterion>
          <TourActivityRef ChainCode="{chain_code}" ProductType="{product_type}" CategoryCode="{category_code}" TourActivityCityCode="{city_code}" DepartureLocation="{departure_loc}"{tac_attr}/>
{los_xml}
        </Criterion>
      </TourActivitySearchCriteria>
      <StayDateRange Start="{start_date}" End="{end_date}"/>
      <ActivityCandidates>
        <ActivityCandidate Quantity="1" RPH="01">
          <GuestCounts>
{guests_xml}
          </GuestCounts>
        </ActivityCandidate>
      </ActivityCandidates>
    </AvailRequestSegment>
  </AvailRequestSegments>
</OTAX_TourActivityAvailRQ>'''.strip()


def parse_availability_xml(xml_bytes: bytes) -> dict:
    ns = {"ota": "http://www.opentravel.org/OTA/2003/05"}
    root = ET.fromstring(xml_bytes)

    err = root.find(".//ota:Errors/ota:Error", ns)
    if err is not None:
        return {
            "ok": False,
            "error_code": err.get("Code"),
            "error_text": err.get("ShortText") or (err.text or "").strip(),
            "rooms": [],
        }

    activities = root.findall(".//ota:Activities/ota:Activity", ns)
    if not activities:
        return {"ok": True, "rooms": []}
    first = activities[0]

    start = end = ""
    nights = ""
    ts0 = first.find("ota:TimeSpan", ns)
    if ts0 is not None:
        start = ts0.get("Start") or ""
        end = ts0.get("End") or ""
        try:
            from datetime import date
            d1 = date.fromisoformat(start[:10]) if start else None
            d2 = date.fromisoformat(end[:10]) if end else None
            if d1 and d2:
                nights = (d2 - d1).days
        except Exception:
            nights = ""

    market_code = first.get("MarketCode") or ""
    dep_loc_el = first.find(".//ota:DepartureLocations/ota:DepartureLocation", ns)
    departure_location = {
        "code": dep_loc_el.get("LocationCode") if dep_loc_el is not None else "",
        "name": (dep_loc_el.text or "").strip() if dep_loc_el is not None else "",
    }

    prop = {}
    bpi = first.find(".//ota:BasicPropertyInfo", ns)
    if bpi is not None:
        prop = {
            "chain_code": bpi.get("ChainCode", ""),
            "tour_activity_code": bpi.get("TourActivityCode", ""),
            "tour_activity_name": bpi.get("TourActivityName", ""),
            "tour_activity_city_code": bpi.get("TourActivityCityCode", ""),
            "product_type": bpi.get("ProductType", ""),
            "product_type_name": bpi.get("ProductTypeName", ""),
            "category_code": bpi.get("CategoryCode", ""),
            "category_detail": bpi.get("CategoryCodeDetail", ""),
            "address": {
                "city": bpi.findtext("ota:Address/ota:CityName", namespaces=ns) or "",
                "state": (bpi.find("ota:Address/ota:StateProv", ns).text or "").strip()
                         if bpi.find("ota:Address/ota:StateProv", ns) is not None else "",
                "country": (bpi.find("ota:Address/ota:CountryName", ns).text or "").strip()
                           if bpi.find("ota:Address/ota:CountryName", ns) is not None else "",
                "country_code": bpi.find("ota:Address/ota:CountryName", ns).get("Code")
                                if bpi.find("ota:Address/ota:CountryName", ns) is not None else "",
            },
        }

    images = [u.text.strip() for u in first.findall(".//ota:TPA_Extensions/ota:ImageItems//ota:URL", ns) if u.text]
    image_main = images[0] if images else ""
    note_el = first.find(".//ota:TPA_Extensions/ota:TextItems/ota:TextItem[@SourceID='NOTE']/ota:Description", ns)
    note = (note_el.text or "").strip() if note_el is not None else ""

    age_bands = []
    for band in first.findall(".//ota:TPA_Extensions/ota:PriceAgeBands/ota:PriceAgeBand", ns):
        age_bands.append({"min": band.get("min", ""), "max": band.get("max", "")})

    air = {"direction": "", "segments": []}
    air_det = first.find(".//ota:TPA_Extensions/ota:AirItineraries/ota:AirItineraryDetail", ns)
    if air_det is not None:
        air["direction"] = air_det.get("DirectionInd", "")
        for opt in air_det.findall(".//ota:OriginDestinationOption", ns):
            seg = opt.find("./ota:FlightSegment", ns)
            if seg is None:
                continue
            dep_el = seg.find("ota:DepartureAirport", ns)
            arr_el = seg.find("ota:ArrivalAirport", ns)
            oper = seg.find("ota:OperatingAirline", ns)
            mark = seg.find("ota:MarketingAirline", ns)
            bag = seg.find(".//ota:TPA_Extensions/ota:Baggage/ota:Weight", ns)

            air["segments"].append({
                "od_rph": opt.get("RPH", ""),
                "departure_datetime": seg.get("DepartureDateTime", ""),
                "arrival_datetime": seg.get("ArrivalDateTime", ""),
                "flight_number": seg.get("FlightNumber", ""),
                "booking_class": seg.get("ResBookDesigCode", ""),
                "dep": {
                    "airport": dep_el.get("LocationCode", "") if dep_el is not None else "",
                    "city": dep_el.get("LocationName", "") if dep_el is not None else "",
                },
                "arr": {
                    "airport": arr_el.get("LocationCode", "") if arr_el is not None else "",
                    "city": arr_el.get("LocationName", "") if arr_el is not None else "",
                },
                "operating_airline": oper.get("Code", "") if oper is not None else "",
                "operating_airline_name": oper.get("CompanyShortName", "") if oper is not None else "",
                "marketing_airline": mark.get("Code", "") if mark is not None else "",
                "marketing_airline_name": mark.get("CompanyShortName", "") if mark is not None else "",
                "baggage_kg": bag.get("Weight", "") if bag is not None else "",
            })

    rooms = []
    for a in activities:
        ts = a.find("ota:TimeSpan", ns)
        a_start = ts.get("Start") if ts is not None else ""
        a_end = ts.get("End") if ts is not None else ""
        a_nights = ""
        try:
            from datetime import date
            d1 = date.fromisoformat((a_start or "")[:10])
            d2 = date.fromisoformat((a_end or "")[:10])
            a_nights = (d2 - d1).days
        except Exception:
            pass

        at = a.find(".//ota:ActivityTypes/ota:ActivityType", ns)
        name = a.findtext(".//ota:ActivityTypes/ota:ActivityType/ota:ActivityDescription/ota:Text", namespaces=ns) or ""
        activity_type_code = at.get("ActivityTypeCode") if at is not None else ""
        number_of_units = at.get("NumberOfUnits") if at is not None else ""

        rp = a.find(".//ota:RatePlans/ota:RatePlan", ns)
        rate_plan_code = rp.get("RatePlanCode") if rp is not None else ""
        rate_plan_name = rp.get("RatePlanName") if rp is not None else ""
        meal_plan_codes = ""
        if rp is not None:
            mi = rp.find("ota:MealsIncluded", ns)
            if mi is not None:
                meal_plan_codes = mi.get("MealPlanCodes", "")

        ar = a.find(".//ota:ActivityRates/ota:ActivityRate", ns)
        booking_code = ar.get("BookingCode") if ar is not None else ""
        rate_plan_code2 = ar.get("RatePlanCode") if ar is not None else ""
        availability_status = ar.get("AvailabilityStatus") if ar is not None else (a.get("AvailabilityStatus") or "")
        units_rate = ar.get("NumberOfUnits") if ar is not None else ""
        total_el = ar.find("ota:Total", ns) if ar is not None else None
        amount = total_el.get("AmountAfterTax") or (total_el.get("AmountBeforeTax") if total_el is not None else "")
        currency = total_el.get("CurrencyCode") if total_el is not None else ""

        canc = a.find(".//ota:CancelPenalties/ota:CancelPenalty", ns)
        cancel = None
        if canc is not None:
            dl = canc.find("ota:Deadline", ns)
            ap = canc.find("ota:AmountPercent", ns)
            cancel = {
                "non_refundable": canc.get("NonRefundable", ""),
                "deadline": {
                    "unit": dl.get("OffsetTimeUnit", "") if dl is not None else "",
                    "multiplier": dl.get("OffsetUnitMultiplier", "") if dl is not None else "",
                    "drop_time": dl.get("OffsetDropTime", "") if dl is not None else "",
                },
                "penalty": {
                    "percent": ap.get("Percent", "") if ap is not None else "",
                    "basis": ap.get("BasisType", "") if ap is not None else "",
                },
            }

        rooms.append({
            "code": activity_type_code or booking_code or "",
            "name": name,
            "availability_status": availability_status,
            "start": a_start, "end": a_end, "nights": a_nights,
            "rate_plan": rate_plan_code or rate_plan_code2,
            "rate_plan_name": rate_plan_name,
            "meal_plan_codes": meal_plan_codes,
            "price": amount or "",
            "currency": currency or "",
            "booking_code": booking_code or "",
            "units": number_of_units or units_rate or "",
            "cancel": cancel,
        })

    return {
        "ok": True,
        "start": start, "end": end, "nights": nights,
        "market_code": market_code,
        "departure_location": departure_location,
        "hotel": prop,
        "images": images,
        "image_main": image_main,
        "note": note,
        "age_bands": age_bands,
        "air": air,
        "rooms": rooms,
    }

# ---------- RES/QUOTE ----------
def build_quote_xml(cfg, *, booking_code, start_date, end_date, guests, res_id_value, rate_plan_code=None):
    ota_ns = OTA_NS
    E = etree.Element

    # Se cfg è dict usa chiavi, altrimenti attributi
    def _get(v):
        if isinstance(cfg, dict):
            return cfg[v]
        return getattr(cfg, v)

    rq = E("{%s}OTAX_TourActivityResRQ" % ota_ns,
           nsmap={None: ota_ns},
           ResStatus="Quote",
           Target=_get("target"),
           PrimaryLangID=_get("primary_lang_id"),
           MarketCountryCode=_get("market_country_code"))

    pos = E("{%s}POS" % ota_ns)
    src = E("{%s}Source" % ota_ns)
    pos.append(src)
    rq.append(pos)
    src.append(E("{%s}RequestorID" % ota_ns,
                 ID=_get("requestor_id"),
                 MessagePassword=_get("message_password")))

    ta_reservations = E("{%s}TourActivityReservations" % ota_ns)
    rq.append(ta_reservations)
    ta_reservation = E("{%s}TourActivityReservation" % ota_ns)
    ta_reservations.append(ta_reservation)

    activities = E("{%s}Activities" % ota_ns)
    ta_reservation.append(activities)
    activity = E("{%s}Activity" % ota_ns)
    activities.append(activity)

    activity_rates = E("{%s}ActivityRates" % ota_ns)
    activity.append(activity_rates)
    activity_rate = E("{%s}ActivityRate" % ota_ns, BookingCode=booking_code)
    if rate_plan_code:
        activity_rate.set("RatePlanCode", rate_plan_code)
    activity_rates.append(activity_rate)
    activity_rate.append(E("{%s}Total" % ota_ns, AmountAfterTax="0.00", CurrencyCode="EUR"))

    activity.append(E("{%s}TimeSpan" % ota_ns, Start=start_date, End=end_date))
    activity.append(E("{%s}BasicPropertyInfo" % ota_ns, ChainCode=_get("chain_code")))

    rphs = E("{%s}ResGuestRPHs" % ota_ns)
    activity.append(rphs)
    for g in guests:
        n = E("{%s}ResGuestRPH" % ota_ns)
        n.text = str(g["rph"])
        rphs.append(n)

    resguests = E("{%s}ResGuests" % ota_ns)
    ta_reservation.append(resguests)
    for g in guests:
        rg = E("{%s}ResGuest" % ota_ns, ResGuestRPH=str(g["rph"]))
        profiles = E("{%s}Profiles" % ota_ns)
        pinfo = E("{%s}ProfileInfo" % ota_ns)
        profile = E("{%s}Profile" % ota_ns)
        cust = E("{%s}Customer" % ota_ns, BirthDate=g["birthdate"])
        pname = E("{%s}PersonName" % ota_ns)
        gvn = E("{%s}GivenName" % ota_ns)
        gvn.text = g["given"]
        srn = E("{%s}Surname" % ota_ns)
        srn.text = g["surname"]
        pname.extend([gvn, srn])
        eml = E("{%s}Email" % ota_ns)
        eml.text = g["email"]
        cust.extend([pname, eml])
        profile.append(cust)
        pinfo.append(profile)
        profiles.append(pinfo)
        rg.append(profiles)
        resguests.append(rg)

    rgi = E("{%s}ResGlobalInfo" % ota_ns)
    ta_reservation.append(rgi)
    ids = E("{%s}TourActivityReservationIDs" % ota_ns)
    rgi.append(ids)
    ids.append(E("{%s}TourActivityReservationID" % ota_ns, ResID_Type="16", ResID_Value=res_id_value))

    return etree.tostring(rq, xml_declaration=True, encoding="utf-8", pretty_print=True)


def parse_quote_full(xml_bytes: bytes) -> dict:
    q = {
        "success": False,
        "errors": [],
        "total": None,
        "currency": None,
        "product": {},
        "room": {},
        "rateplan": {},
        "flights": [],
        "itinerary": [],
        "images": [],
        "note": "",
        "age_bands": [],
        "cancel_policy": None,
        "res_ids": [],
        "guests": [],
        "timespan": {"start": "", "end": ""},
        "booking_code": "",
    }
    try:
        root = etree.fromstring(xml_bytes)
    except Exception as ex:
        q["errors"].append(f"Parse error: {ex}")
        return q

    for e in root.xpath(".//*[local-name()='Errors']/*"):
        code = e.get("Code") or ""
        msg = e.get("ShortText") or (e.text or "").strip()
        q["errors"].append(f"{code} {msg}".strip())

    tot = (root.xpath(".//*[local-name()='ActivityRate']/*[local-name()='Total']") or [None])[0]
    if tot is not None:
        q["total"] = tot.get("AmountAfterTax") or tot.get("AmountBeforeTax")
        q["currency"] = tot.get("CurrencyCode") or ""
    ar = (root.xpath(".//*[local-name()='ActivityRate']") or [None])[0]
    if ar is not None:
        q["booking_code"] = ar.get("BookingCode") or ""

    at = (root.xpath(".//*[local-name()='ActivityTypes']/*[local-name()='ActivityType']") or [None])[0]
    if at is not None:
        q["room"] = {
            "code": at.get("ActivityTypeCode") or "",
            "name": (root.xpath("string(.//*[local-name()='ActivityTypes']/*[local-name()='ActivityType']/*[local-name()='ActivityDescription']/*[local-name()='Text'])") or "").strip()
        }

    rp = (root.xpath(".//*[local-name()='RatePlans']/*[local-name()='RatePlan']") or [None])[0]
    if rp is not None:
        meals = (rp.xpath(".//*[local-name()='MealsIncluded']") or [None])[0]
        q["rateplan"] = {
            "code": rp.get("RatePlanCode") or "",
            "name": rp.get("RatePlanName") or "",
            "meals": meals.get("MealPlanCodes", "") if meals is not None else "",
        }

    ts = (root.xpath(".//*[local-name()='TimeSpan']") or [None])[0]
    if ts is not None:
        q["timespan"] = {"start": ts.get("Start") or "", "end": ts.get("End") or ""}

    bpi = (root.xpath(".//*[local-name()='BasicPropertyInfo']") or [None])[0]
    if bpi is not None:
        addr = (bpi.xpath(".//*[local-name()='Address']") or [None])[0]
        country_el = (addr.xpath("./*[local-name()='CountryName']") or [None])[0] if addr is not None else None
        pos = (bpi.xpath(".//*[local-name()='Position']") or [None])[0]
        q["product"] = {
            "chain_code": bpi.get("ChainCode", ""),
            "code": bpi.get("TourActivityCode", ""),
            "name": bpi.get("TourActivityName", ""),
            "city_code": bpi.get("TourActivityCityCode", ""),
            "type": bpi.get("ProductType", ""),
            "type_code": bpi.get("ProductTypeCode", ""),
            "type_name": bpi.get("ProductTypeName", ""),
            "category_code": bpi.get("CategoryCode", ""),
            "category_detail": bpi.get("CategoryCodeDetail", ""),
            "address": {
                "city": (addr.xpath("string(./*[local-name()='CityName'])") if addr is not None else "") or "",
                "state": (addr.xpath("string(./*[local-name()='StateProv'])") if addr is not None else "") or "",
                "country": (addr.xpath("string(./*[local-name()='CountryName'])") if addr is not None else "") or "",
                "country_code": country_el.get("Code", "") if country_el is not None else "",
            },
            "position": {
                "lat": pos.get("Latitude", "") if pos is not None else "",
                "lng": pos.get("Longitude", "") if pos is not None else "",
            }
        }

    q["images"] = [u.strip() for u in root.xpath(".//*[local-name()='ImageItems']//*[local-name()='URL']/text()") if u and u.strip()]
    desc = (root.xpath(".//*[local-name()='TextItems']/*[local-name()='TextItem'][@SourceID='NOTE']/*[local-name()='Description']/text()") or [])
    if desc:
        q["note"] = "\n".join([d.strip() for d in desc if d and d.strip()])

    for pb in root.xpath(".//*[local-name()='PriceAgeBands']/*[local-name()='PriceAgeBand']"):
        q["age_bands"].append({"min": pb.get("min", ""), "max": pb.get("max", "")})

    for seg in root.xpath(".//*[local-name()='AirItineraryDetail']//*[local-name()='FlightSegment']"):
        dep = (seg.xpath("./*[local-name()='DepartureAirport']") or [None])[0]
        arr = (seg.xpath("./*[local-name()='ArrivalAirport']") or [None])[0]
        oper = (seg.xpath("./*[local-name()='OperatingAirline']") or [None])[0]
        mark = (seg.xpath("./*[local-name()='MarketingAirline']") or [None])[0]
        bagw = (seg.xpath(".//*[local-name()='TPA_Extensions']/*[local-name()='Baggage']/*[local-name()='Weight']") or [None])[0]
        q["flights"].append({
            "dep_datetime": seg.get("DepartureDateTime", ""),
            "arr_datetime": seg.get("ArrivalDateTime", ""),
            "flight_number": seg.get("FlightNumber", ""),
            "class": seg.get("ResBookDesigCode", ""),
            "dep": {"code": dep.get("LocationCode", "") if dep is not None else "", "name": dep.get("LocationName", "") if dep is not None else ""},
            "arr": {"code": arr.get("LocationCode", "") if arr is not None else "", "name": arr.get("LocationName", "") if arr is not None else ""},
            "oper": {"code": oper.get("Code", "") if oper is not None else "", "name": oper.get("CompanyShortName", "") if oper is not None else ""},
            "mkt":  {"code": mark.get("Code", "") if mark is not None else "", "name": mark.get("CompanyShortName", "") if mark is not None else ""},
            "baggage_kg": bagw.get("Weight", "") if bagw is not None else "",
        })

    for it in root.xpath(".//*[local-name()='Itineraries']/*[local-name()='Itinerary']"):
        dest = (it.xpath(".//*[local-name()='Destinations']/*[local-name()='Destination']") or [None])[0]
        q["itinerary"].append({
            "label": it.get("LocalityName", ""),
            "text": (it.xpath("string(.//*[local-name()='TextItems']/*[local-name()='TextItem']/*[local-name()='Description'])") or "").strip(),
            "dest": {
                "code": dest.get("Code", "") if dest is not None else "",
                "name": dest.get("Name", "") if dest is not None else "",
            }
        })

    can = (root.xpath(".//*[local-name()='CancelPenalties']/*[local-name()='CancelPenalty']") or [None])[0]
    if can is not None:
        dl = (can.xpath("./*[local-name()='Deadline']") or [None])[0]
        ap = (can.xpath("./*[local-name()='AmountPercent']") or [None])[0]
        q["cancel_policy"] = {
            "non_ref": (can.get("NonRefundable", "") or "").lower() == "true",
            "deadline": {
                "unit": dl.get("OffsetTimeUnit", "") if dl is not None else "",
                "multiplier": dl.get("OffsetUnitMultiplier", "") if dl is not None else "",
                "drop_time": dl.get("OffsetDropTime", "") if dl is not None else "",
            },
            "penalty": {
                "basis": ap.get("BasisType", "") if ap is not None else "",
                "percent": ap.get("Percent", "") if ap is not None else "",
            },
        }

    for rid in root.xpath(".//*[local-name()='TourActivityReservationIDs']/*[local-name()='TourActivityReservationID']"):
        q["res_ids"].append({"type": rid.get("ResID_Type", ""), "value": rid.get("ResID_Value", "")})

    for rg in root.xpath(".//*[local-name()='ResGuests']/*[local-name()='ResGuest']"):
        rph = rg.get("ResGuestRPH", "")
        cust = (rg.xpath(".//*[local-name()='Customer']") or [None])[0]
        given = (rg.xpath(".//*[local-name()='PersonName']/*[local-name()='GivenName']/text()") or [""])[0]
        sur = (rg.xpath(".//*[local-name()='PersonName']/*[local-name()='Surname']/text()") or [""])[0]
        mail = (rg.xpath(".//*[local-name()='Email']/text()") or [""])[0]
        q["guests"].append({
            "rph": rph,
            "birth": cust.get("BirthDate", "") if cust is not None else "",
            "name": f"{given} {sur}".strip(),
            "email": mail.strip(),
        })

    q["success"] = len(q["errors"]) == 0 and bool(q["total"])
    return q

    # app/services/ota_io.py
import requests
from requests.auth import HTTPBasicAuth

def post_ota_xml(url: str, xml_bytes: bytes, settings, headers=None, timeout: int = 40) -> bytes:
    """
    Esegue POST OTA con XML:
    - Supporta Bearer (settings.bearer_token)
    - Supporta Basic (settings.http_user/http_password, altrimenti requestor_id/message_password)
    - headers opzionali mergiati
    - ritorna bytes della risposta (raise per HTTP != 2xx)
    """
    if headers is None:
        headers = {}
    # header base
    hdrs = {
        "Content-Type": "application/xml; charset=utf-8",
        "Accept": "application/xml",
    }
    hdrs.update(headers or {})

    auth = None

    # Bearer
    bearer = getattr(settings, "bearer_token", None)
    if bearer:
        hdrs["Authorization"] = f"Bearer {bearer}"

    # Basic (se non c'è Bearer)
    if "Authorization" not in hdrs:
        http_user = getattr(settings, "http_user", None)
        http_pwd  = getattr(settings, "http_password", None)
        if http_user and http_pwd:
            auth = HTTPBasicAuth(http_user, http_pwd)
        else:
            # fallback “legacy”: usa requestor_id/message_password se disponibili
            rid = getattr(settings, "requestor_id", None)
            mpw = getattr(settings, "message_password", None)
            if rid and mpw:
                auth = HTTPBasicAuth(str(rid), str(mpw))

    resp = requests.post(url, data=xml_bytes, headers=hdrs, auth=auth, timeout=timeout)
    resp.raise_for_status()
    return resp.content

def _call_build_quote_xml(func, **payload):
    """
    Chiamata compatibile: passa a build_quote_xml solo i parametri che accetta.
    Supporta alias 'settings'/'s'.
    """
    sig = inspect.signature(func)
    params = sig.parameters.keys()
    accepted = {k: v for k, v in payload.items() if k in params and v is not None}
    # alias settings/s
    if "settings" in params and "settings" not in accepted and "s" in accepted:
        accepted["settings"] = accepted["s"]
    if "s" in params and "s" not in accepted and "settings" in accepted:
        accepted["s"] = accepted["settings"]
    return func(**accepted)
