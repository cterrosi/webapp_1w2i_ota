# app/services/ota_detail.py

def is_meaningful_detail(detail: dict) -> bool:
    if not isinstance(detail, dict):
        return False
    return bool(
        (detail.get("name")) or
        (detail.get("descriptions") and any(detail["descriptions"])) or
        (detail.get("image_urls") and any(detail["image_urls"]))
    )

def merge_detail_with_row(detail: dict, row) -> dict:
    d = dict(detail or {})
    d.setdefault("name", row.tour_activity_name or "")
    d.setdefault("city", row.city_code or "")
    d.setdefault(
        "country",
        f"{(row.country_iso or '').strip()} {(row.country_name or '').strip()}".strip()
    )
    d["product_id"] = getattr(row, "id", None)
    d["tour_activity_code"] = getattr(row, "tour_activity_code", "") or ""
    # default sicuri per i template
    d.setdefault("descriptions", d.get("descriptions") or [])
    d.setdefault("image_urls", d.get("image_urls") or [])
    d.setdefault("categories", d.get("categories") or [])
    d.setdefault("types", d.get("types") or [])
    d.setdefault("duration", d.get("duration") or "")
    d.setdefault("pickup_notes", d.get("pickup_notes") or [])
    return d
