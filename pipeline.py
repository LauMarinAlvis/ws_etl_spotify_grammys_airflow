import sys
import uuid

from extract.read_csv import read_spotify_csv
from extract.read_db import read_grammys_db
from load.csv_export import export_table_to_csv
from load.postgres import load_to_postgres
from transform.grammys import transform_grammys
from transform.merge import merge_artists
from transform.spotify import transform_spotify
from validate.quarantine import save_quarantine
from validate.spotify_schema import validate_spotify


def run(run_id: str | None = None) -> None:
    run_id = run_id or f"local_{uuid.uuid4().hex[:8]}"

    # 1) EXTRACT (CSV) + VALIDATE
    spotify = read_spotify_csv()
    result = validate_spotify(spotify)
    s = result.summary
    print(f"[validate] {s['valid_rows']}/{s['total_rows']} válidas | "
          f"inválidas: {s['invalid_rows']} ({s['invalid_pct']} %) | umbral: {s['threshold_pct']} %")
    for rule in s["failed_rules"]:
        print(f"           - {rule['rule']}: {rule['rows']} filas")
    save_quarantine(result.invalid_df)
    if not result.passed:
        print("Validación FALLÓ: el pipeline se detiene (rama de alerta en Airflow).")
        sys.exit(1)
    print("Validación OK")

    # 2) TRANSFORM (CSV)
    sp_art = transform_spotify(result.valid_df)

    # 3) EXTRACT (BD) + TRANSFORM
    gr_art = transform_grammys(read_grammys_db())

    # 4) MERGE -> LOAD -> STORE
    merged = merge_artists(sp_art, gr_art, run_id=run_id)
    load_to_postgres(merged)
    export_table_to_csv()
    print(f"Pipeline terminado (run_id={run_id})")


if __name__ == "__main__":
    run()
