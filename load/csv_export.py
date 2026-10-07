"""Fase STORE: exporta la tabla final del warehouse a un archivo CSV."""
import pandas as pd
from sqlalchemy import text

from config.settings import OUTPUT_DIR, get_engine
from load.postgres import FINAL_TABLE

OUTPUT_CSV = OUTPUT_DIR / "artist_grammy_spotify.csv"


def export_table_to_csv(table: str = FINAL_TABLE, path=None, engine=None):
    """Lee la tabla desde la BD (no desde memoria) para que CSV y BD sean idénticos."""
    engine = engine or get_engine()
    path = path or OUTPUT_CSV
    path.parent.mkdir(parents=True, exist_ok=True)
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {table} ORDER BY grammy_wins DESC, n_tracks DESC"), conn)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[store] {len(df)} filas -> {path}")
    return path
