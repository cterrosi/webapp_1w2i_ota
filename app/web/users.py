# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from flask_login import login_required, current_user
from sqlalchemy.exc import IntegrityError
from functools import wraps

from ..extensions import db
from ..models import User

bp = Blueprint("users", __name__, url_prefix="/users")

# --- autorizzazione: limita al solo admin (username 'admin' o flag is_admin) ---
def _is_admin_user() -> bool:
    if getattr(current_user, "is_authenticated", False) is not True:
        return False
    if getattr(current_user, "is_admin", False):
        return True
    return getattr(current_user, "username", "") == "admin"

def admin_required(fn):
    @wraps(fn)
    @login_required
    def inner(*args, **kwargs):
        if not _is_admin_user():
            flash("Solo l'utente amministratore può gestire gli utenti.", "warning")
            return redirect(url_for("home.home"))
        return fn(*args, **kwargs)
    return inner

# --- LISTA ---
@bp.route("/", endpoint="list")
@login_required
def list_users():
    users = User.query.order_by(User.username.asc()).all()
    # Se vuoi usare nei template la visibilità admin:
    return render_template("users/list.html", users=users, is_admin=_is_admin_user())


# --- CREATE ---
@bp.route("/create", methods=["GET", "POST"], endpoint="create")
@admin_required
def create_user():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        confirm  = (request.form.get("confirm")  or "").strip()

        if not username or not password:
            flash("Username e Password sono obbligatori.", "warning")
            return redirect(url_for("users.create"))

        if password != confirm:
            flash("Le password non coincidono.", "warning")
            return redirect(url_for("users.create"))

        u = User(username=username)
        if hasattr(u, "set_password"):
            u.set_password(password)
        else:
            from werkzeug.security import generate_password_hash
            u.password_hash = generate_password_hash(password)

        db.session.add(u)
        try:
            db.session.commit()

            from flask import current_app
            current_app.logger.info(
                "[users] commit ok. users=%s",
                [u.username for u in User.query.order_by(User.id).all()]
            )

            flash(f"Utente '{username}' creato.", "success")
            return redirect(url_for("users.list"))
        except IntegrityError:
            db.session.rollback()
            flash("Username già esistente.", "danger")

    return render_template("users/form.html", mode="create", user=None)

# --- EDIT ---
@bp.route("/<int:user_id>/edit", methods=["GET", "POST"], endpoint="edit")
@admin_required
def edit_user(user_id: int):
    u = User.query.get_or_404(user_id)

    if request.method == "POST":
        new_username = (request.form.get("username") or "").strip()
        new_password = (request.form.get("password") or "").strip()
        confirm      = (request.form.get("confirm")  or "").strip()

        if not new_username:
            flash("Username è obbligatorio.", "warning")
            return redirect(url_for("users.edit", user_id=user_id))

        u.username = new_username

        if new_password:
            if new_password != confirm:
                flash("Le password non coincidono.", "warning")
                return redirect(url_for("users.edit", user_id=user_id))
            if hasattr(u, "set_password"):
                u.set_password(new_password)
            else:
                from werkzeug.security import generate_password_hash
                u.password_hash = generate_password_hash(new_password)

        try:
            db.session.commit()

            current_app.logger.info(
                "[users] commit ok. users=%s",
                [u.username for u in User.query.order_by(User.id).all()]
            )

            flash("Utente aggiornato.", "success")
            return redirect(url_for("users.list"))
        except IntegrityError:
            db.session.rollback()
            flash("Username già esistente.", "danger")

    return render_template("users/form.html", mode="edit", user=u)

# --- DELETE (con conferma) ---
@bp.route("/<int:user_id>/delete", methods=["GET", "POST"], endpoint="delete")
@admin_required
def delete_user(user_id: int):
    u = User.query.get_or_404(user_id)

    total = User.query.count()
    if request.method == "POST":
        if total <= 1:
            flash("Non puoi eliminare l'unico utente rimasto.", "warning")
            return redirect(url_for("users.list"))

        if getattr(current_user, "id", None) == u.id and total == 2:
            flash("Non puoi eliminare te stesso se resta un solo utente.", "warning")
            return redirect(url_for("users.list"))

        db.session.delete(u)
        db.session.commit()

        current_app.logger.info(
                "[users] commit ok. users=%s",
                [u.username for u in User.query.order_by(User.id).all()]
        )

        flash(f"Utente '{u.username}' eliminato.", "success")
        return redirect(url_for("users.list"))

    return render_template("users/delete.html", user=u, total=total)

@bp.route("/_debug_db")
@admin_required
def debug_db():
    from flask import current_app, jsonify
    uri = current_app.config.get("SQLALCHEMY_DATABASE_URI")
    rows = db.session.execute(db.text(
        "SELECT id, username, length(password_hash) AS ph_len FROM user ORDER BY id"
    )).mappings().all()
    return jsonify({
        "uri": uri,
        "rows": [dict(r) for r in rows],
    })

