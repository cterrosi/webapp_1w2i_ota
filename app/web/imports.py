from flask import Blueprint, jsonify
from flask_login import login_required
from datetime import datetime
from ..services.import_job import progress, start_thread, _progress_lock

bp = Blueprint("imports", __name__, url_prefix="/import_departures")

@bp.post("/start", endpoint="start_import")
@login_required
def start_import():
    ok, msg = start_thread()
    return jsonify({"ok": ok, "msg": msg}), (202 if ok else 409)

@bp.get("/progress", endpoint="get_progress")
@login_required
def get_progress():
    with _progress_lock:
        try:
            if progress.get("running") and progress.get("hb"):
                last = datetime.fromisoformat(progress["hb"])
                if (datetime.now() - last).total_seconds() > 60:
                    progress["error"] = "Job bloccato (nessun avanzamento da >60s)."
                    progress["running"] = False
                    progress["finished_at"] = datetime.now().isoformat(timespec="seconds")
        except Exception:
            pass
        return jsonify(progress), 200
