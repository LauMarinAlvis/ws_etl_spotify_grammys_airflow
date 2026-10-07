"""rutas y conexión al warehouse

mi PC localhost:5433 y Docker warehouse:5432
"""
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# En Docker: /opt/airflow | En tu PC: la raíz del proyecto
BASE_DIR = Path(os.environ.get("PIPELINE_BASE_DIR", Path(__file__).resolve().parents[1]))
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "output"
QUARANTINE_DIR = OUTPUT_DIR / "quarantine"


def _read_env_file() -> dict:
    """Lee el .env del proyecto (solo se usa fuera de Docker)."""
    env = {}
    path = Path(__file__).resolve().parents[1] / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env[key.strip()] = value.strip()
    return env


def get_engine():
    env = {**_read_env_file(), **os.environ}
    url = URL.create(
        "postgresql+psycopg2",
        username=env["WAREHOUSE_USER"],
        password=env["WAREHOUSE_PASSWORD"],
        host=env.get("WAREHOUSE_HOST", "localhost"),
        port=int(env.get("WAREHOUSE_PORT", 5433)),
        database=env["WAREHOUSE_DB"],
    )
    return create_engine(url)