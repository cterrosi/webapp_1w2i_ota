# app/services/parse_products.py
from lxml import etree as ET

def ota_products(xml_bytes: bytes) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    ns = {"ota": "http://www.opentravel.org/OTA/2003/05"}
    out = []
    for n in root.findall(".//ota:TourActivityProducts/ota:TourActivityProduct", ns):
        out.append({
            "TourActivityCode": n.get("TourActivityCode", ""),
            "TourActivityName": n.get("TourActivityName", ""),
            "TourActivityCityCode": n.get("TourActivityCityCode", ""),
            "AreaID": n.get("AreaID", ""),
            "CountryISOCode": n.get("CountryISOCode", ""),
            "CountryName": n.get("CountryName", ""),
            "ProductType": n.get("ProductType", ""),
            "ProductTypeCode": n.get("ProductTypeCode", ""),
            "ProductTypeName": n.get("ProductTypeName", ""),
            "CategoryCode": n.get("CategoryCode", ""),
            "CategoryCodeDetail": n.get("CategoryCodeDetail", ""),
        })
    return out
