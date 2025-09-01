from threading import Thread, Lock
from datetime import datetime
from flask import current_app
from ..settings import JSON_DIR, DB_PATH
from app.services.import_departures import import_departures

progress = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "total": 0,
    "done": 0,
    "rows": 0,
    "current_file": None,
    "last_msg": None,
    "error": None,
    "hb": None,
    "_last_rows_seen": 0,
}

_progress_lock: Lock = Lock()
RUNNING_FLAG = False

def _reset_progress():
    progress.update({
        "running": False, "started_at": None, "finished_at": None,
        "total": 0, "done": 0, "rows": 0,
        "current_file": None, "last_msg": None, "error": None,
        "hb": None, "_last_rows_seen": 0,
    })

def _on_begin(*args, **kwargs):
    total = None
    if args:
        if isinstance(args[0], int):
            total = args[0]
        elif isinstance(args[0], dict):
            d = args[0]
            total = d.get("total") or d.get("files_total") or d.get("file_count")
    if total is None:
        total = kwargs.get("total") or kwargs.get("files_total") or kwargs.get("file_count") or 0

    with _progress_lock:
        progress["total"] = int(total or 0)
        progress["done"] = 0
        progress["current_file"] = None
        progress["last_msg"] = f"Inizio import di {progress['total']} file"
        progress["hb"] = datetime.now().isoformat(timespec="seconds")
        progress["_last_rows_seen"] = 0

def _on_step(*args, **kwargs):
    file_name = None; idx = None; rows_added = None; rows_cum = None
    if kwargs:
        file_name  = kwargs.get("file") or kwargs.get("file_name") or kwargs.get("current_file")
        idx        = kwargs.get("done") or kwargs.get("idx") or kwargs.get("files_done")
        rows_added = kwargs.get("rows_added") or kwargs.get("delta")
        rows_cum   = kwargs.get("rows") or kwargs.get("total_rows") or kwargs.get("count")
    if args:
        if len(args) == 1:
            if isinstance(args[0], int): idx = args[0]
            elif isinstance(args[0], dict):
                d = args[0]
                file_name  = file_name  or d.get("file") or d.get("file_name") or d.get("current_file")
                idx        = idx        or d.get("done") or d.get("idx") or d.get("files_done")
                rows_added = rows_added or d.get("rows_added") or d.get("delta")
                rows_cum   = rows_cum   or d.get("rows") or d.get("total_rows") or d.get("count")
        elif len(args) == 2:
            file_name, idx = args
        else:
            file_name = args[0]; idx = args[1]; rows_added = args[2]
            if len(args) >= 4: rows_cum = args[3]

    try:
        if idx is not None: idx = int(idx)
    except Exception: idx = None
    try:
        if rows_added is not None: rows_added = int(rows_added)
    except Exception: rows_added = None
    try:
        if rows_cum is not None: rows_cum = int(rows_cum)
    except Exception: rows_cum = None

    inc = 0
    with _progress_lock:
        if rows_added is not None:
            inc = max(0, rows_added)
        elif rows_cum is not None:
            prev = progress.get("_last_rows_seen", 0) or 0
            inc = max(0, rows_cum - prev)
            progress["_last_rows_seen"] = rows_cum

        progress["rows"] += inc
        if idx is not None: progress["done"] = idx
        if file_name: progress["current_file"] = file_name

        total = progress.get("total") or 0
        base = f"[{progress['done']}/{total}]" if total else f"[{progress['done']}]"
        tail = f" (+{inc})" if inc else ""
        progress["last_msg"] = f"{base} {file_name or ''}{tail}".strip()
        progress["hb"] = datetime.now().isoformat(timespec="seconds")

def _run_import_job(app):
    global RUNNING_FLAG
    try:
        with app.app_context():
            print(f"[dep] job starting JSON_DIR={JSON_DIR} DB_PATH={DB_PATH}", flush=True)
            res = import_departures(json_dir=JSON_DIR, db_path=DB_PATH, on_begin=_on_begin, on_step=_on_step)

            # >>> backfill automatico post-import <<<
            updated = _run_backfill(DB_PATH)
            print(f"[dep] backfill updated={updated}", flush=True)

            with _progress_lock:
                progress["rows"] = int(res.get("rows", progress["rows"])) if isinstance(res, dict) else progress["rows"]
                progress["last_msg"] = f"Completato (backfill {updated} righe)"
                progress["error"] = None
            print(f"[dep] job done rows={progress['rows']}", flush=True)

    except Exception as e:
        with _progress_lock:
            progress["error"] = str(e)
            progress["last_msg"] = "Errore"
        print(f"[dep] job ERROR: {e}", flush=True)
    finally:
        with _progress_lock:
            progress["running"] = False
            progress["finished_at"] = datetime.now().isoformat(timespec="seconds")
        RUNNING_FLAG = False


def start_thread():
    global RUNNING_FLAG
    with _progress_lock:
        if progress["running"] or RUNNING_FLAG:
            return False, "Import già in esecuzione"
        _reset_progress()
        progress["running"] = True
        progress["started_at"] = datetime.now().isoformat(timespec="seconds")
        progress["last_msg"] = "Avviato"
        RUNNING_FLAG = True
    Thread(target=_run_import_job, args=(current_app._get_current_object(),), daemon=True).start()
    return True, "Import avviato"

import sqlite3

def _run_backfill(db_path: str) -> int:
    sql = """
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
       OR product_type IS NULL;
    """
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(sql)
        con.commit()
        return cur.rowcount or 0
    finally:
        con.close()
