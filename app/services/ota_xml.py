from datetime import datetime, timedelta, date
import html

OTA_NS = "http://www.opentravel.org/OTA/2003/05"

import xml.etree.ElementTree as ET

def _localname(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag

def _gather_inclusions_exclusions(node: ET.Element) -> tuple[list[str], list[str]]:
    include_keys = {
        "Included", "Includes", "Inclusions", "IncludedServices",
        "IncludedInRate", "QuotaComprende"
    }
    exclude_keys = {
        "Excluded", "Exclusions", "NotIncluded", "ExcludedServices",
        "QuotaNonComprende", "NotInRate"
    }

    def _collect_texts(el: ET.Element) -> list[str]:
        out = []
        for t in el.findall(".//{*}Text"):
            txt = (t.text or "").strip()
            if txt:
                out.append(txt)
        if not out:
            txt = (el.text or "").strip()
            if txt:
                out.append(txt)
        seen, uniq = set(), []
        for s in out:
            if s not in seen:
                seen.add(s)
                uniq.append(s)
        return uniq

    inc, exc = [], []
    if node is None:
        return inc, exc

    for el in node.iter():
        name = _localname(el.tag)
        if name in include_keys:
            inc.extend(_collect_texts(el))
        elif name in exclude_keys:
            exc.extend(_collect_texts(el))

    def _unique(items: list[str]) -> list[str]:
        seen, res = set(), []
        for x in items:
            x = x.strip()
            if x and x not in seen:
                seen.add(x)
                res.append(x)
        return res

    return _unique(inc), _unique(exc)


# prima era: def build_availability_xml_with_guests(*, requestor_id: str, ...)
# => ora accetta anche eventuali posizionali (es. self) e usa solo kwargs
def build_availability_xml_with_guests(*_args, **kwargs) -> bytes:
    requestor_id        = kwargs["requestor_id"]
    message_password    = kwargs["message_password"]
    chain_code          = kwargs["chain_code"]
    product_type        = kwargs["product_type"]
    category_code       = kwargs["category_code"]
    city_code           = kwargs["city_code"]
    departure_loc       = kwargs["departure_loc"]
    start_date          = kwargs["start_date"]
    duration_days       = int(kwargs["duration_days"])
    tour_activity_code  = kwargs.get("tour_activity_code")
    target              = kwargs["target"]
    primary_lang_id     = kwargs["primary_lang_id"]
    market_country_code = kwargs["market_country_code"]
    adults              = int(kwargs.get("adults", 2))
    children_ages       = list(kwargs.get("children_ages", []) or [])

    from datetime import datetime, timedelta, date as _date
    try:
        sd = datetime.fromisoformat(str(start_date)[:10]).date()
    except Exception:
        sd = _date.fromisoformat(str(start_date)[:10])
    end_date = (sd + timedelta(days=duration_days)).isoformat()

    los_xml = f"<LengthOfStay>{duration_days}</LengthOfStay>"

    # ⬇️ CAMBIO: un <GuestCount> per adulto, Count="1" (molti OTA lo richiedono)
    guests_items = []
    for _ in range(max(0, adults)):
        guests_items.append('<GuestCount Age="50" Count="1"/>')
    for age in children_ages:
        guests_items.append(f'<GuestCount Age="{int(age)}" Count="1"/>')
    guests_xml = "\n".join(f"            {x}" for x in guests_items) or '            <GuestCount Age="50" Count="1"/>'

    tac_attr = f' TourActivityCode="{(tour_activity_code or "").strip()}"' if tour_activity_code else ""

    xml = f'''<OTAX_TourActivityAvailRQ Target="{target}" PrimaryLangID="{primary_lang_id}" MarketCountryCode="{market_country_code}" xmlns="{OTA_NS}">
  <POS>
    <Source>
      <RequestorID ID="{html.escape(str(requestor_id))}" MessagePassword="{html.escape(str(message_password))}"/>
    </Source>
  </POS>
  <AvailRequestSegments>
    <AvailRequestSegment>
      <TourActivitySearchCriteria>
        <Criterion>
          <TourActivityRef ChainCode="{html.escape(str(chain_code))}" ProductType="{html.escape(str(product_type))}" CategoryCode="{html.escape(str(category_code))}" TourActivityCityCode="{html.escape(str(city_code))}" DepartureLocation="{html.escape(str(departure_loc))}"{tac_attr}/>
          {los_xml}
        </Criterion>
      </TourActivitySearchCriteria>
      <StayDateRange Start="{str(start_date)[:10]}" End="{end_date}"/>
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
    return xml.encode("utf-8")



import xml.etree.ElementTree as ET

NS = {"ota": OTA_NS}

def parse_availability_xml(xml_bytes: bytes) -> dict:
    try:
        root = ET.fromstring(xml_bytes or b"")
    except Exception as ex:
        return {"ok": False, "error_code": "PARSE", "error_text": str(ex), "rooms": []}

    err = root.find(".//ota:Errors/ota:Error", NS)
    if err is not None:
        return {
            "ok": False,
            "error_code": err.get("Code"),
            "error_text": err.get("ShortText") or (err.text or "").strip(),
            "rooms": [],
        }

    activities = root.findall(".//ota:Activities/ota:Activity", NS)
    if not activities:
        return {"ok": True, "start": "", "end": "", "nights": "", "rooms": []}

    first = activities[0]
    start = end = ""
    nights = ""
    ts0 = first.find("ota:TimeSpan", NS)
    if ts0 is not None:
        start = ts0.get("Start") or ""
        end = ts0.get("End") or ""
        try:
            d1 = date.fromisoformat(start[:10]) if start else None
            d2 = date.fromisoformat(end[:10]) if end else None
            nights = (d2 - d1).days if (d1 and d2) else ""
        except Exception:
            nights = ""

    def _pick_price(node):
        # Cerca Total in vari punti noti
        if node is None:
            return "", ""
        # 1) ActivityRate/Total
        tot = node.find(".//ota:ActivityRates/ota:ActivityRate/ota:Total", NS)
        if tot is None:
            # 2) ActivityPrices/ActivityPrice/Total
            tot = node.find(".//ota:ActivityPrices/ota:ActivityPrice/ota:Total", NS)
        if tot is None:
            # 3) Qualsiasi Total sotto Activity
            tot = node.find(".//ota:Total", NS)
        if tot is None:
            # 4) Alcuni provider mettono sotto TPA_Extensions (TotalPrice/GrossAmount/Amount)
            ext = node.find(".//ota:TPA_Extensions", NS)
            if ext is not None:
                for tag in ["Total", "TotalPrice", "GrossAmount", "Amount"]:
                    cand = ext.find(f".//ota:{tag}", NS)
                    if cand is not None:
                        # prova attributi classici
                        a = cand.get("AmountAfterTax") or cand.get("AmountBeforeTax") or cand.get("Price") or cand.get("Amount")
                        c = cand.get("CurrencyCode") or cand.get("Currency") or ""
                        if a:
                            return a, c
        if tot is not None:
            amt = tot.get("AmountAfterTax") or tot.get("AmountBeforeTax") or ""
            ccy = tot.get("CurrencyCode") or ""
            return amt, ccy
        return "", ""

    rooms = []
    for a in activities:
        at = a.find(".//ota:ActivityTypes/ota:ActivityType", NS)
        name = a.findtext(".//ota:ActivityTypes/ota:ActivityType/ota:ActivityDescription/ota:Text", default="", namespaces=NS) or ""
        code = at.get("ActivityTypeCode") if at is not None else ""

        # booking_code può stare su ActivityRate o (raramente) su ActivityPrice
        ar = a.find(".//ota:ActivityRates/ota:ActivityRate", NS)
        ap = a.find(".//ota:ActivityPrices/ota:ActivityPrice", NS)
        booking_code = ""
        if ar is not None:
            booking_code = ar.get("BookingCode") or ""
        if not booking_code and ap is not None:
            booking_code = ap.get("BookingCode") or ""

        # rate plan
        rp = a.find(".//ota:RatePlans/ota:RatePlan", NS)
        rate_plan_code = rp.get("RatePlanCode") if rp is not None else ""
        rate_plan_name = rp.get("RatePlanName") if rp is not None else ""
        meal_plan_codes = ""
        if rp is not None:
            mi = rp.find("ota:MealsIncluded", NS)
            if mi is not None:
                meal_plan_codes = mi.get("MealPlanCodes", "") or ""

        # availability status
        availability_status = ""
        if ar is not None:
            availability_status = ar.get("AvailabilityStatus") or ""
        if not availability_status:
            availability_status = a.get("AvailabilityStatus") or ""

        # prezzo
        amount, currency = _pick_price(a)

        rooms.append({
            "code": code or booking_code or "",
            "name": name,
            "availability_status": availability_status or "",
            "rate_plan": rate_plan_code or "",
            "rate_plan_name": rate_plan_name or "",
            "meal_plan_codes": meal_plan_codes or "",
            "price": amount or "",
            "currency": currency or "",
            "booking_code": booking_code or "",
            "units": (ar.get("NumberOfUnits") if ar is not None else "") or "",
        })

    return {"ok": True, "start": start, "end": end, "nights": nights, "rooms": rooms}

# === Fallback QUOTE: builder minimale + parser del totale ===
from xml.etree.ElementTree import Element as _E, tostring as _tostring

def build_quote_xml_simple(
    *,
    requestor_id: str,
    message_password: str,
    chain_code: str,
    target: str,
    primary_lang_id: str,
    market_country_code: str,
    booking_code: str,
    start_date: str,  # "YYYY-MM-DD"
    end_date: str,    # "YYYY-MM-DD"
    guests: list[dict],  # [{rph:int, birthdate:"YYYY-MM-DD", given:"A", surname:"B", email:"x@y"}]
) -> bytes:
    rq = _E(f"{{{OTA_NS}}}OTAX_TourActivityResRQ",
            ResStatus="Quote", Target=target, PrimaryLangID=primary_lang_id, MarketCountryCode=market_country_code)
    pos = _E(f"{{{OTA_NS}}}POS")
    src = _E(f"{{{OTA_NS}}}Source")
    rid = _E(f"{{{OTA_NS}}}RequestorID", ID=str(requestor_id), MessagePassword=str(message_password))
    src.append(rid); pos.append(src); rq.append(pos)

    ta_res = _E(f"{{{OTA_NS}}}TourActivityReservations")
    ta_r = _E(f"{{{OTA_NS}}}TourActivityReservation")
    acts = _E(f"{{{OTA_NS}}}Activities")
    act = _E(f"{{{OTA_NS}}}Activity")
    rates = _E(f"{{{OTA_NS}}}ActivityRates")
    rate = _E(f"{{{OTA_NS}}}ActivityRate", BookingCode=str(booking_code))
    rate.append(_E(f"{{{OTA_NS}}}Total", AmountAfterTax="0.00", CurrencyCode="EUR"))
    rates.append(rate); act.append(rates)
    act.append(_E(f"{{{OTA_NS}}}TimeSpan", Start=f"{start_date}", End=f"{end_date}"))
    act.append(_E(f"{{{OTA_NS}}}BasicPropertyInfo", ChainCode=str(chain_code)))
    acts.append(act); ta_r.append(acts)

    # RPHs
    rphs = _E(f"{{{OTA_NS}}}ResGuestRPHs")
    for g in guests:
        n = _E(f"{{{OTA_NS}}}ResGuestRPH"); n.text = str(g["rph"]); rphs.append(n)
    act.append(rphs)

    # Guests minimi
    resguests = _E(f"{{{OTA_NS}}}ResGuests")
    for g in guests:
        rg = _E(f"{{{OTA_NS}}}ResGuest", ResGuestRPH=str(g["rph"]))
        profiles = _E(f"{{{OTA_NS}}}Profiles")
        pinfo = _E(f"{{{OTA_NS}}}ProfileInfo")
        profile = _E(f"{{{OTA_NS}}}Profile")
        cust = _E(f"{{{OTA_NS}}}Customer", BirthDate=g["birthdate"])
        pname = _E(f"{{{OTA_NS}}}PersonName")
        gn = _E(f"{{{OTA_NS}}}GivenName"); gn.text = g["given"]
        sn = _E(f"{{{OTA_NS}}}Surname"); sn.text = g["surname"]
        em = _E(f"{{{OTA_NS}}}Email"); em.text = g["email"]
        pname.extend([gn, sn]); cust.extend([pname, em]); profile.append(cust); pinfo.append(profile); profiles.append(pinfo)
        rg.append(profiles); resguests.append(rg)
    ta_r.append(resguests)

    rq.append(ta_r)
    return _tostring(rq, encoding="utf-8", xml_declaration=True)

def parse_quote_total(xml_bytes: bytes) -> dict:
    try:
        root = ET.fromstring(xml_bytes or b"")
    except Exception as ex:
        return {"ok": False, "total": None, "currency": "", "error": f"PARSE {ex}"}
    err = root.find(".//ota:Errors/ota:Error", NS)
    if err is not None:
        return {"ok": False, "total": None, "currency": "", "error": (err.get("ShortText") or err.text or "").strip()}
    tot = root.find(".//ota:ActivityRate/ota:Total", NS)
    if tot is None:
        return {"ok": True, "total": None, "currency": ""}
    amount = tot.get("AmountAfterTax") or tot.get("AmountBeforeTax") or ""
    ccy = tot.get("CurrencyCode") or ""
    try:
        val = float(str(amount).replace(",", "."))
    except Exception:
        val = None
    return {"ok": True, "total": val, "currency": ccy}