# wsgi.py (root progetto)
from app import create_app   # <-- importa dalla __init__.py del package 'app'
app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
