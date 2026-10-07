"""Fase LOAD: carga el dataset final en el warehouse (Postgres).

Carga completa e IDEMPOTENTE: ejecutar el pipeline dos veces deja la misma tabla,
sin duplicar filas.
"""
import pandas as pd
from sqlalchemy import text

from config.settings import get_engine

FINAL_TABLE = "artist_grammy_spotify"


def load_to_postgres(df: pd.DataFrame, table: str = FINAL_TABLE, engine=None) -> int:
    engine = engine or get_engine()
    with engine.begin() as conn:
        df.to_sql(table, conn, if_exists="replace", index=False, chunksize=2000)
        conn.execute(text(f"ALTER TABLE {table} ADD PRIMARY KEY (artist_norm)"))
        total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    if total != len(df):
        raise ValueError(f"Carga incompleta: {len(df)} filas enviadas, {total} en la tabla")
    print(f"[load] {total} filas en {table}")
    return total
