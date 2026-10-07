import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from sqlalchemy import text

from config.settings import DATA_RAW, get_engine

CSV_PATH = DATA_RAW / "the_grammy_awards.csv"
TABLE = "grammys_raw"


def main():
    df = pd.read_csv(CSV_PATH)
    print(f"Leído {CSV_PATH.name}: {df.shape[0]} filas, {df.shape[1]} columnas")

    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(TABLE, conn, if_exists="replace", index=False)
        total = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
    print(f" OKI DOKI Tabla {TABLE} cargada en el warehouse: {total} filas")


if __name__ == "__main__":
    main()