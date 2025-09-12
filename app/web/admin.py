# app/web/admin.py
import os, io, zipfile, requests

from flask import Blueprint, jsonify, render_template, render_template_string, request, redirect, url_for, flash, current_app
from flask_login import login_required

from sqlalchemy import text as _sql
from sqlalchemy.exc import OperationalError

from ..extensions import db
from ..services.runtime import get_setting_safe
from ..services.ota_endpoints import build_admin_calendar_url
from ..settings import JSON_DIR, JSON_TEMP_DIR
from ..models import OTAProduct, OTAProductMedia, OTAProductDetail

# --- WordPress mapping service ---
try:
    from app.services import wp_mapping
except Exception:
    from ..services import wp_mapping


bp = Blueprint("admin", __name__, url_prefix="/admin")


@bp.get("/dep_diag", endpoint="dep_diag")
@login_required
def dep_diag():
    """Diagnostica minima su cartelle JSON e tabella departures_cache."""
    info = {
        "APP_DB_PATH": os.environ.get("APP_DB_PATH", "/data/ota.db"),
        "JSON_DIR": JSON_DIR,
        "JSON_TEMP_DIR": JSON_TEMP_DIR,
        "json_dir_exists": os.path.isdir(JSON_DIR),
        "json_temp_dir_exists": os.path.isdir(JSON_TEMP_DIR),
        "json_count": 0,
        "sample_files": [],
        "db_count": None,
        "schema": [],
        "error": None,
    }
    try:
        files = [f for f in os.listdir(JSON_DIR) if f.endswith(".json")]
        info["json_count"] = len(files)
        info["sample_files"] = files[:10]
    except Exception as e:
        info["error"] = f"ls(JSON_DIR) -> {e}"

    try:
        info["db_count"] = db.session.execute(_sql("SELECT COUNT(*) FROM departures_cache")).scalar_one()
    except Exception as e:
        info["error"] = (info["error"] or "") + f" | db count -> {e}"

    try:
        rows = db.session.execute(_sql("PRAGMA table_info(departures_cache)")).fetchall()
        info["schema"] = [{"cid": r[0], "name": r[1], "type": r[2]} for r in rows]
    except Exception:
        pass

    return jsonify(info), 200


@bp.get("/dep_run_sync", endpoint="dep_run_sync")
@login_required
def dep_run_sync():
    """Esegue l'import delle partenze dai JSON in JSON_DIR, poi ritorna un breve JSON di riepilogo."""
    from import_departures import import_departures

    try:
        # percorsi robusti e coerenti con prod
        db_path = os.environ.get("APP_DB_PATH", "/data/ota.db")
        os.makedirs(JSON_DIR, exist_ok=True)
        os.makedirs(JSON_TEMP_DIR, exist_ok=True)

        res = import_departures(json_dir=JSON_DIR, db_path=db_path)

        db_count = db.session.execute(_sql("SELECT COUNT(*) FROM departures_cache")).scalar_one()
        sample = db.session.execute(_sql(
            "SELECT product_code, depart_date, duration_days, source_file "
            "FROM departures_cache ORDER BY depart_date ASC LIMIT 10"
        )).fetchall()

        sample_list = [{
            "product_code": r[0],
            "depart_date": (r[1].isoformat() if hasattr(r[1], "isoformat") else r[1]),
            "duration_days": r[2],
            "source_file": r[3],
        } for r in sample]

        return jsonify({
            "ok": True,
            "import_result": res,
            "db_count_after": db_count,
            "sample_rows": sample_list,
            "json_dir": JSON_DIR,
        }), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "json_dir": JSON_DIR}), 500


@bp.get("/download_departures_zip", endpoint="download_departures_zip")
@login_required
def download_departures_zip():
    """Scarica lo ZIP dall’endpoint admin, estrae i JSON in JSON_TEMP_DIR e mostra un breve report HTML."""
    s = get_setting_safe()
    url = build_admin_calendar_url(s)

    # 1) Download
    try:
        r = requests.get(url, timeout=180)
        r.raise_for_status()
    except requests.RequestException as ex:
        body = ""
        try:
            if getattr(ex, "response", None) is not None:
                body = (ex.response.text or "")[:2000]
        except Exception:
            pass
        return render_template_string("""
        {% extends "base.html" %}{% block content %}
          <div class="alert alert-danger">
            <div><strong>Download fallito</strong></div>
            <div class="small">URL: <code>{{ url }}</code></div>
            <div class="mt-2"><pre class="small mb-0">{{ body }}</pre></div>
          </div>
          <a href="{{ url_for('home.home') }}" class="btn btn-secondary btn-sm">Back to Home</a>
        {% endblock %}""", url=url, body=body), 502

    # 2) Extract
    extracted = []
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            os.makedirs(JSON_TEMP_DIR, exist_ok=True)
            for info in zf.infolist():
                if info.is_dir():
                    continue
                name = os.path.basename(info.filename)
                if not name:
                    continue
                dst_path = os.path.join(JSON_TEMP_DIR, name)
                with zf.open(info) as src, open(dst_path, "wb") as dst:
                    dst.write(src.read())
                extracted.append(name)
    except zipfile.BadZipFile:
        body = ""
        try:
            body = r.text[:2000]
        except Exception:
            pass
        return render_template_string("""
        {% extends "base.html" %}{% block content %}
          <div class="alert alert-danger">
            <strong>La risposta non è uno ZIP valido.</strong>
          </div>
          <pre class="small">{{ body }}</pre>
          <a href="{{ url_for('home.home') }}" class="btn btn-secondary btn-sm">Back to home</a>
        {% endblock %}""", body=body), 502

    # 3) Report
    return render_template_string("""
    {% extends "base.html" %}{% block content %}
      <div class="alert alert-success">
        <div><strong>Scaricato e scompattato con successo.</strong></div>
        <div>File estratti: {{ extracted|length }} in <code>{{ json_temp_dir }}</code></div>
      </div>
      <div class="d-flex gap-2">
        <a href="/" class="btn btn-primary btn-sm">Back to Home</a>
      </div>
      {% if extracted %}
      <details class="mt-3">
        <summary>Dettagli (prime 30):</summary>
        <ul class="small mt-2 mb-0">
        {% for f in extracted[:30] %}<li>{{ f }}</li>{% endfor %}
        </ul>
      </details>
      {% endif %}
    {% endblock %}""", extracted=extracted, json_temp_dir=JSON_TEMP_DIR)


from sqlalchemy.exc import OperationalError
from sqlalchemy import text as _sql
from flask import flash, redirect, url_for
from ..extensions import db
from ..models import OTAProduct, OTAProductMedia, OTAProductDetail

@bp.post("/clear_products_cache", endpoint="clear_products_cache")
@login_required
def clear_products_cache():
    """
    Cancella TUTTA la cache: media, dettagli e prodotti.
    Ordine: figli -> padre per evitare orfani/sfalsamenti.
    Tollerante se una tabella non esiste.
    """
    deleted_media = deleted_detail = deleted_prod = 0

    # 1) MEDIA
    try:
        deleted_media = db.session.query(OTAProductMedia).delete(synchronize_session=False)
    except OperationalError as e:
        db.session.rollback()
        flash(f"Tabella ota_product_media non trovata: {e.__class__.__name__}", "warning")

    # 2) DETTAGLI
    try:
        deleted_detail = db.session.query(OTAProductDetail).delete(synchronize_session=False)
    except OperationalError as e:
        db.session.rollback()
        flash(f"Tabella ota_product_detail non trovata: {e.__class__.__name__}", "warning")

    # 3) PRODOTTI
    try:
        deleted_prod = db.session.query(OTAProduct).delete(synchronize_session=False)
    except OperationalError as e:
        db.session.rollback()
        flash(f"Tabella ota_product non trovata: {e.__class__.__name__}", "warning")

    # Commit unico
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        flash(f"Errore durante la cancellazione cache: {e}", "danger")
        return redirect(url_for("home.home"))

    # Messaggio finale
    msg = []
    msg.append(f"media: {deleted_media}")   # potrà essere 0 se tabella assente
    msg.append(f"dettagli: {deleted_detail}")
    msg.append(f"prodotti: {deleted_prod}")
    flash("Cache OTA pulita → " + ", ".join(msg) + ".", "success")

    return redirect(url_for("home.home"))


@bp.post("/clear_departures_cache", endpoint="clear_departures_cache")
@login_required
def clear_departures_cache():
    """Cancella TUTTA la cache partenze (departures_cache), tollerante se la tabella non esiste."""
    try:
        count = db.session.execute(_sql("SELECT COUNT(*) FROM departures_cache")).scalar() or 0
        db.session.execute(_sql("DELETE FROM departures_cache"))
        db.session.commit()
        flash(f"Cancellati {count} record dalla cache partenze.", "success")
    except OperationalError as e:
        db.session.rollback()
        flash(f"Tabella departures_cache non trovata: nulla da cancellare. ({e.__class__.__name__})", "warning")
    return redirect(url_for("home.home"))

@bp.get("/prod_diag", endpoint="prod_diag")
@login_required
def prod_diag():
    info = {
        "engine_url": str(db.engine.url),
        "counts": {},
        "sample_detail": [],
        "error": None,
    }
    try:
        info["counts"]["ota_product"] = db.session.execute(_sql("SELECT COUNT(*) FROM ota_product")).scalar_one()
        info["counts"]["ota_product_detail"] = db.session.execute(_sql("SELECT COUNT(*) FROM ota_product_detail")).scalar_one()
        info["counts"]["ota_product_media"] = db.session.execute(_sql("SELECT COUNT(*) FROM ota_product_media")).scalar_one()

        rows = db.session.execute(_sql(
            "SELECT product_id, name, LENGTH(COALESCE(descriptions_json,'')) AS dlen "
            "FROM ota_product_detail LIMIT 5"
        ))
        info["sample_detail"] = [dict(r._mapping) for r in rows]

    except Exception as e:
        info["error"] = str(e)
    return jsonify(info), 200

# ================== WORDPRESS MAPPING ==================

@bp.get("/wpmap/view", endpoint="wpmap_view")
@login_required
def wpmap_view():
    wp_mapping.ensure_table(db)
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 100))
    offset = (page - 1) * per_page
    data = wp_mapping.fetch_page(db, limit=per_page, offset=offset)
    return render_template("admin/wpmap_list.html", data=data, page=page, per_page=per_page)

@bp.post("/wpmap/clear", endpoint="wpmap_clear")
@login_required
def wpmap_clear():
    wp_mapping.ensure_table(db)
    deleted = wp_mapping.clear_all(db)
    flash(f"Mapping svuotato. Righe cancellate: {deleted}", "warning")
    return redirect(url_for("home.home"))

@bp.post("/wpmap/import", endpoint="wpmap_import")
@login_required
def wpmap_import():
    wp_mapping.ensure_table(db)

    f = request.files.get("wp_csv")
    if not f or f.filename == "":
        flash("Seleziona un file CSV esportato da WooCommerce.", "danger")
        return redirect(url_for("home.home"))

    csv_bytes = f.read()
    try:
        res = wp_mapping.import_csv_bytes(db, csv_bytes)
        flash(f"Import completato: {res['inserted_or_updated']} righe usate, {res['skipped']} scartate. Totale: {res['total_after']}.", "success")
    except Exception as ex:
        current_app.logger.exception("Errore import mapping WooCommerce")
        flash(f"Errore durante l'import: {ex}", "danger")

    return redirect(url_for("home.home"))
