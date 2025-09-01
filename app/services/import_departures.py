# app/services/import_departures.py
import os, json, sqlite3
from functools import lru_cache
from datetime import datetime
from pathlib import Path

# ------------------------ Root & default paths (assoluti) ------------------------
# /app/app/services/import_departures.py -> ROOT = /app (root del repo)
ROOT = Path(__file__).resolve().parents[2]

DB_PATH_DEFAULT = ROOT / "data" / "ota.db"
JSON_DIR_DEFAULT = ROOT / "data" / "downloads"

# ------------------------ Path utils ------------------------

def _resolve_db_path(db_path: str | None) -> str:
    """
    Restituisce un path ASSOLUTO all'SQLite DB:
    - preferisce argomento esplicito
    - poi env DB_PATH
    - fallback a DB_PATH_DEFAULT
    Se relativo, lo risolve rispetto a ROOT.
    """
    value = db_path or os.environ.get("DB_PATH")
    p = Path(value) if value else DB_PATH_DEFAULT
    if not p.is_absolute():
        p = (ROOT / p).resolve()
    return str(p)

def _pick_json_roots(db_path_abs: str, json_dir_hint: str | None) -> list[Path]:
    """
    Restituisce cartelle da scansionare (in ordine di priorità):
    - hint esplicito
    - JSON_DIR / JSON_TEMP_DIR da env (risolti vs ROOT se relativi)
    - downloads/temp e downloads accanto al DB
    - fallback default (JSON_DIR_DEFAULT)
    """
    def _as_path(x: str | None) -> Path | None:
        if not x:
            return None
        p = Path(x)
        return (ROOT / p).resolve() if not p.is_absolute() else p

    roots: list[Path] = []
    if json_dir_hint:
        roots.append(_as_path(json_dir_hint))

    jenv = _as_path(os.environ.get("JSON_DIR"))
    jtmp = _as_path(os.environ.get("JSON_TEMP_DIR"))
    if jenv: roots.append(jenv)
    if jtmp: roots.append(jtmp)

    data_base = Path(db_path_abs).parent
    roots.append(data_base / "downloads" / "temp")
    roots.append(data_base / "downloads")
    roots.append(JSON_DIR_DEFAULT)

    # dedup mantenendo l'ordine
    out, seen = [], set()
    for r in roots:
        if not r:
            continue
        try:
            rp = r.resolve()
        except Exception:
            rp = r
        key = str(rp)
        if key not in seen:
            seen.add(key)
            out.append(rp)
    return out

def _list_candidate_files(roots: list[Path]) -> list[Path]:
    files: list[Path] = []
    exts = {".json"}  # importiamo solo JSON
    for r in roots:
        if not r.exists():
            continue
        found = [p for p in r.rglob("*") if p.is_file() and p.suffix.lower() in exts]
        print(f"[dep] scan {r} -> {len(found)} files", flush=True)
        files.extend(found)
    return files

# ------------------------ Parsing helpers ------------------------

def parse_durations(val):
    if val is None:
        return []
    if isinstance(val, int):
        return [int(val)]
    if isinstance(val, (list, tuple)):
        out = []
        for x in val:
            try:
                out.append(int(str(x).strip()))
            except Exception:
                pass
        return sorted({d for d in out})
    s = str(val)
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except Exception:
            pass
    return sorted({d for d in out})

def extract_airport_from_code(code: str):
    # Se il tuo product_code contiene '#XXX', qui lo estrai.
    if "#" in (code or ""):
        s = (code or "").strip()
        after = s.split("#", 1)[1]
        return (after[:3] or "").upper() if after else None
    return None

# ------------------------ DB schema & meta lookup ------------------------

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS departures_cache (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_code TEXT NOT NULL,
      depart_airport TEXT,                 -- es. "FCO" preso dopo il '#'
      depart_date TEXT NOT NULL,           -- ISO yyyy-mm-dd
      duration_days INTEGER NOT NULL,
      source_file TEXT NOT NULL,           -- nome file da cui proviene
      loaded_at TEXT NOT NULL DEFAULT (datetime('now')),

      -- campi per multi-search (devono esistere)
      city_code    VARCHAR(20),
      area_id      VARCHAR(50),
      country_iso  VARCHAR(5),
      country_name VARCHAR(80),
      product_type VARCHAR(20),

      UNIQUE(product_code, depart_date, duration_days) ON CONFLICT REPLACE
    );
    CREATE INDEX IF NOT EXISTS idx_departures_cache_date ON departures_cache(depart_date);
    CREATE INDEX IF NOT EXISTS idx_departures_cache_prod ON departures_cache(product_code);
    CREATE INDEX IF NOT EXISTS idx_departures_city_date  ON departures_cache(city_code, depart_date);
    """)

@lru_cache(maxsize=50000)
def get_product_meta(db_path: str, product_code: str) -> dict:
    """
    Legge i metadati da ota_product usando la join:
      ota_product.tour_activity_code = product_code
    """
    if not product_code:
        return {}
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute("""
            SELECT city_code, area_id, country_iso, country_name, product_type
            FROM ota_product
            WHERE tour_activity_code = ?
            LIMIT 1
        """, (product_code,))
        r = cur.fetchone()
    finally:
        con.close()
    return {} if not r else {
        "city_code": r[0], "area_id": r[1], "country_iso": r[2],
        "country_name": r[3], "product_type": r[4],
    }

def upsert_departure(con, db_path: str, product_code: str, depart_airport: str,
                     depart_date: str, duration_days: int, source_file: str):
    """
    Inserisce/aggiorna una riga in departures_cache, valorizzando i metadati
    dalla tabella ota_product con join su product_code = tour_activity_code.
    """
    meta = get_product_meta(db_path, product_code)
    dep3 = (depart_airport or "").upper().strip()[:3] or None
    con.execute("""
        INSERT INTO departures_cache
            (product_code, depart_airport, depart_date, duration_days, source_file,
             city_code, area_id, country_iso, country_name, product_type)
        VALUES
            (?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?)
        ON CONFLICT(product_code, depart_date, duration_days) DO UPDATE SET
            depart_airport = excluded.depart_airport,
            source_file    = excluded.source_file,
            city_code      = COALESCE(excluded.city_code, city_code),
            area_id        = COALESCE(excluded.area_id, area_id),
            country_iso    = COALESCE(excluded.country_iso, country_iso),
            country_name   = COALESCE(excluded.country_name, country_name),
            product_type   = COALESCE(excluded.product_type, product_type),
            loaded_at      = datetime('now')
    """, (
        product_code, dep3, depart_date, int(duration_days), source_file,
        meta.get("city_code"), meta.get("area_id"), meta.get("country_iso"),
        meta.get("country_name"), meta.get("product_type"),
    ))

def backfill_departures_cache(conn: sqlite3.Connection, db_path_abs: str):
    """
    Se alcune righe erano state inserite senza metadati, le completa ora.
    """
    conn.execute("""
        UPDATE departures_cache AS d
        SET
          city_code    = COALESCE((SELECT p.city_code    FROM ota_product p WHERE p.tour_activity_code = d.product_code), city_code),
          area_id      = COALESCE((SELECT p.area_id      FROM ota_product p WHERE p.tour_activity_code = d.product_code), area_id),
          country_iso  = COALESCE((SELECT p.country_iso  FROM ota_product p WHERE p.tour_activity_code = d.product_code), country_iso),
          country_name = COALESCE((SELECT p.country_name FROM ota_product p WHERE p.tour_activity_code = d.product_code), country_name),
          product_type = COALESCE((SELECT p.product_type FROM ota_product p WHERE p.tour_activity_code = d.product_code), product_type)
        WHERE city_code IS NULL
           OR area_id IS NULL
           OR country_iso IS NULL
           OR country_name IS NULL
           OR product_type IS NULL
    """)

# ------------------------ Import principale ------------------------

def import_departures(json_dir: str | None = None,
                      db_path: str | None = None,
                      on_begin=None, on_step=None):
    """
    Legge tutti i .json nelle cartelle candidate e importa periodi
    in departures_cache. Valorizza i metadati da ota_product.
    """
    db_path_abs = _resolve_db_path(db_path)
    roots = _pick_json_roots(db_path_abs, json_dir)
    files = _list_candidate_files(roots)

    # Solo file .json contano come "total"
    json_files = [f for f in files if f.suffix.lower() == ".json"]
    if on_begin:
        on_begin(len(json_files))

    conn = sqlite3.connect(db_path_abs)
    try:
        ensure_schema(conn)

        imported_rows = 0
        done_files = 0

        # (facoltativo) completa eventuali vecchie righe senza metadati
        backfill_departures_cache(conn, db_path_abs)
        conn.commit()

        for fpath in json_files:
            fname = fpath.name
            added = 0

            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as ex:
                print(f"[dep] skip {fname}: JSON error: {ex}", flush=True)
                done_files += 1
                if on_step: on_step(done=done_files)
                continue

            product_code = data.get("productCode")
            if not product_code:
                done_files += 1
                if on_step: on_step(done=done_files)
                continue

            airport = extract_airport_from_code(product_code)
            periods = data.get("periods") or []

            for p in periods:
                date_from = p.get("dateFrom")
                durations_raw = p.get("validDurations", "")
                if not date_from or not durations_raw:
                    continue

                try:
                    # gestisce "2025-09-27" o "2025-09-27T00:00:00Z"
                    dt = date_from.replace("Z", "")
                    depart_date = datetime.fromisoformat(dt[:19] if "T" in dt else dt).date().isoformat()
                except Exception:
                    continue

                for d in parse_durations(durations_raw):
                    upsert_departure(conn, db_path_abs, product_code, airport, depart_date, d, fname)
                    added += 1

            conn.commit()
            imported_rows += added
            done_files += 1
            if on_step: on_step(file=fname, done=done_files, rows_added=added, rows=imported_rows)

        return {"files": len(json_files), "rows": imported_rows}

    finally:
        conn.close()

# ------------------------ CLI minimale ------------------------

def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="Importa partenze dai JSON in departures_cache (SQLite)")
    ap.add_argument("--db", dest="db_path", help="Path SQLite DB (default: data/ota.db)", default=None)
    ap.add_argument("--json-dir", dest="json_dir", help="Cartella JSON (override)", default=None)
    args = ap.parse_args(argv)
    res = import_departures(json_dir=args.json_dir, db_path=args.db_path)
    print(f"[dep] done: files={res['files']} rows={res['rows']}")

if __name__ == "__main__":
    main()
