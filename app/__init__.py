# __init__.py
import os
from flask import Flask
from .extensions import db, login_manager
from .models import ensure_setting_columns, User
from .web.price_export import bp as price_export_bp

def create_app():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    templates_dir = os.path.join(base_dir, "templates")
    static_dir = os.path.join(base_dir, "static")
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    app = Flask(__name__, template_folder=templates_dir, static_folder=static_dir)
    app.secret_key = os.getenv("SECRET_KEY", "supersecretkey-ota")

    db_path = os.path.join(data_dir, "ota.db")
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{db_path}"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
    app.config["LOGIN_DISABLED"] = False

    # Estensioni
    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = "auth.login" 

    # Schema + colonne
    with app.app_context():
        db.create_all()
        ensure_setting_columns()

    # Blueprint
    from app.web import auth, home, products, admin, imports, availability, booking, quote, users

    app.register_blueprint(auth.bp)
    app.register_blueprint(home.bp)
    app.register_blueprint(products.bp)
    app.register_blueprint(admin.bp)
    app.register_blueprint(imports.bp)
    app.register_blueprint(availability.bp)
    app.register_blueprint(booking.bp)
    app.register_blueprint(quote.bp)
    app.register_blueprint(users.bp)
    app.register_blueprint(price_export_bp)

    # Seed admin
    with app.app_context():
        admin_user = User.query.filter_by(username="admin").first()
        if not admin_user:
            admin_user = User(username="admin")
            admin_user.set_password("admin")
            db.session.add(admin_user)
        elif not admin_user.password_hash or len(admin_user.password_hash) < 20:
            admin_user.set_password("admin")
        db.session.commit()

    @login_manager.unauthorized_handler
    def _unauth():
        from flask import request, redirect, url_for
        return redirect(url_for("auth.login", next=request.path))

    # Debug: stampa rotte
    with app.app_context():
        for r in app.url_map.iter_rules():
            print(f"{r.endpoint:30} -> {r.rule}")

    return app

@login_manager.user_loader
def load_user(user_id: str):
    try:
        return db.session.get(User, int(user_id))
    except Exception:
        return None
