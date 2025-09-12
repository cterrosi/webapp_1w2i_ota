from __future__ import annotations
import csv, io
from sqlalchemy import text as _sql

TABLE = "wp_product_master"

def ensure_table(db) -> None:
    exists = db.session.execute(
        _sql("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:t"),
        {"t": TABLE},
    ).first()
    if exists:
        return
    db.session.execute(_sql(f'''
        CREATE TABLE {TABLE} (
            ID          INTEGER PRIMARY KEY,
            Tipo        TEXT,
            SKU         TEXT,
            Nome        TEXT,
            Pubblicato  TEXT
        );
    '''))
    db.session.execute(_sql(f'CREATE INDEX idx_wp_master_sku_lookup ON {TABLE} (SKU);'))
    db.session.execute(_sql(f'''
        CREATE UNIQUE INDEX ux_wp_master_sku_not_empty
        ON {TABLE} (SKU)
        WHERE SKU IS NOT NULL AND length(trim(SKU)) > 0;
    '''))
    db.session.execute(_sql(f'CREATE INDEX idx_wp_master_nome ON {TABLE} (Nome);'))
    db.session.commit()

def clear_all(db) -> int:
    res = db.session.execute(_sql(f'DELETE FROM {TABLE}'))
    db.session.commit()
    try:
        return res.rowcount or 0
    except Exception:
        return 0

def import_csv_bytes(db, csv_bytes: bytes) -> dict:
    ensure_table(db)

    # --- 1) Decodifica robusta ---
    text = None
    if b"\x00" in csv_bytes[:400]:
        for enc in ("utf-16", "utf-16le", "utf-16be"):
            try:
                text = csv_bytes.decode(enc)
                break
            except Exception:
                pass
    if text is None:
        for enc in ("utf-8-sig", "utf-8", "cp1252"):
            try:
                text = csv_bytes.decode(enc)
                break
            except Exception:
                pass
    if text is None:
        text = csv_bytes.decode("utf-8", errors="ignore")

    # --- 2) Rilevazione delimitatore ---
    sample = text[:4096]
    delim_counts = {
        ";": sample.count(";"),
        ",": sample.count(","),
        "\t": sample.count("\t"),
    }
    delim = max(delim_counts, key=delim_counts.get) or ","

    # --- 3) Lettura CSV con DictReader ---
    buf = io.StringIO(text)
    reader = csv.DictReader(buf, delimiter=delim)

    used = 0
    skipped = 0

    def norm_key(k: str) -> str:
        # normalizza le intestazioni: togli spazi/BOM e portale in lower
        return (k or "").strip().lstrip("\ufeff").lower()

    # mappa chiavi normalizzate -> nome originale (per robustezza)
    header_map = {norm_key(k): k for k in (reader.fieldnames or [])}

    def pick(row, *keys):
        # cerca nell'ordine varianti possibile (già in lower)
        for k in keys:
            real = header_map.get(k)
            if real is not None:
                v = row.get(real)
                if v not in (None, ""):
                    return v
        return ""

    for row in reader:
        # chiavi normalizzate che vogliamo supportare
        raw_id = pick(row, "id", "Id".lower())
        try:
            _id = int(str(raw_id).strip())
        except Exception:
            skipped += 1
            continue

        _tipo = (pick(row, "tipo", "type") or "").strip() or None
        _sku  = (pick(row, "sku", "codice", "codice articolo") or "").strip() or None
        _nome = (pick(row, "nome", "name", "titolo") or "").strip() or None
        _pubb = (pick(row, "pubblicato", "published") or "").strip() or None

        db.session.execute(
            _sql(f'''
                INSERT OR REPLACE INTO {TABLE} (ID, Tipo, SKU, Nome, Pubblicato)
                VALUES (:ID, :Tipo, :SKU, :Nome, :Pubblicato)
            '''),
            {"ID": _id, "Tipo": _tipo, "SKU": _sku, "Nome": _nome, "Pubblicato": _pubb},
        )
        used += 1

    db.session.commit()
    total = db.session.execute(_sql(f"SELECT COUNT(*) FROM {TABLE}")).scalar_one()
    return {"inserted_or_updated": used, "skipped": skipped, "total_after": total}


def fetch_page(db, limit=100, offset=0):
    ensure_table(db)
    rows = db.session.execute(
        _sql(f'''
            SELECT ID, Tipo, SKU, Nome, Pubblicato
            FROM {TABLE}
            ORDER BY Nome COLLATE NOCASE
            LIMIT :limit OFFSET :offset
        '''),
        {"limit": limit, "offset": offset},
    ).mappings().all()
    total = db.session.execute(_sql(f'SELECT COUNT(*) FROM {TABLE}')).scalar_one()
    return {"rows": rows, "total": total}
