"""Fase EXTRACT (fuente 2): lee Grammys desde la base de datos SQL (warehouse).

Entradas : tabla grammys_raw (cargada con scripts/load_grammys_to_db.py)
Salidas  : DataFrame con las columnas necesarias
"""
import pandas as pd
from sqlalchemy import text

from config.settings import get_engine

# SELECT explícito (no SELECT *): si la tabla agrega columnas, el pipeline no se rompe.
QUERY = text("SELECT year, category, nominee, artist, workers, winner FROM grammys_raw")


def read_grammys_db(engine=None) -> pd.DataFrame:
    engine = engine or get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(QUERY, conn)
    if df.empty:
        raise ValueError("La tabla grammys_raw está vacía: ejecuta scripts/load_grammys_to_db.py")
    print(f"[read_db] grammys_raw: {len(df)} filas")
    return df
