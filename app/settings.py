import os

BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
JSON_DIR = os.path.join(DATA_DIR, "downloads")
JSON_TEMP_DIR = os.path.join(JSON_DIR, "temp")
DEBUG_DIR = os.path.join(DATA_DIR, "debug")
DB_PATH = os.path.join(DATA_DIR, "ota.db")

for d in (DATA_DIR, JSON_DIR, JSON_TEMP_DIR, DEBUG_DIR):
    os.makedirs(d, exist_ok=True)

DEBUG_SAVE = True
SECRET_KEY = "supersecretkey-ota"
