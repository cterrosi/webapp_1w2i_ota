from flask import Blueprint, render_template, request, redirect, url_for, current_app
from flask_login import login_required, current_user
from ..services.runtime import get_setting_safe
from ..utils import normalize_base_url
from ..extensions import db
from jinja2 import TemplateNotFound


bp = Blueprint("home", __name__)  # nome blueprint corretto

@bp.before_app_request
def guard():
    ep = (request.endpoint or "")
    # consenti static e tutto l'auth (login/logout/callback ecc.)
    if ep == "static" or ep.startswith("auth."):
        return
    # forza login per tutto il resto
    if not current_user.is_authenticated:
        return redirect(url_for("auth.login", next=request.path))

@bp.after_app_request
def no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

@bp.route("/", methods=["GET"], endpoint="home")
def home_index():
    candidates = ["home.html", "index.html", "home/home.html"]
    try:
        tmpl = current_app.jinja_env.get_or_select_template(candidates)
        # Usa render_template per avere i context processor (es. current_user)
        return render_template(tmpl.name)
    except TemplateNotFound:
        current_app.logger.warning("Home template non trovato. Candidati=%s", candidates)
        return redirect(url_for("home.settings"))


@bp.route("/settings", methods=["GET", "POST"], endpoint="settings")
@login_required
def settings():
    s = get_setting_safe()
    if request.method == "POST":
        s.base_url = normalize_base_url(request.form.get("base_url",""))
        env = (request.form.get("env") or "TEST").upper()
        s.target = "Production" if env == "PRODUCTION" else "Test"
        s.primary_lang = request.form.get("primary_lang","it").strip()
        s.requestor_id = request.form.get("requestor_id","").strip()
        s.message_password = request.form.get("message_password","").strip()
        s.chain_code = request.form.get("chain_code","").strip()
        s.market_country_code = request.form.get("market_country_code","it").strip()
        s.product_type = request.form.get("product_type","Tour").strip()
        s.category_code = request.form.get("category_code","211").strip()
        s.city_code = request.form.get("city_code","").strip()
        s.tour_activity_code = request.form.get("tour_activity_code","").strip()
        s.bearer_token = request.form.get("bearer_token","").strip()
        s.basic_user = request.form.get("basic_user","").strip()
        s.basic_pass = request.form.get("basic_pass","").strip()
        try:
            s.timeout_seconds = int(request.form.get("timeout_seconds","40") or 40)
        except:
            s.timeout_seconds = 40
        s.departure_default = request.form.get("departure_default","VCE").strip()
        try:
            s.los_min = int(request.form.get("los_min","7") or 7)
        except:
            s.los_min = 7
        try:
            s.los_max = int(request.form.get("los_max","14") or 14)
        except:
            s.los_max = 14
        db.session.commit()

    # questa è la pagina impostazioni (non più “home” app)
    return render_template("settings.html", setting=s)
