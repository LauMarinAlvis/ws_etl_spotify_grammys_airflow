""" Merge de DB Spotify en csv con DB Grammys en SQL y guarda el resultado en Postgres y en un CSV """
import json
from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG


def _stg(name: str):
    """Ruta de un archivo temporal staging en data/processed"""
    from config.settings import DATA_PROCESSED

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    return DATA_PROCESSED / f"stg_{name}.pkl"


def _json_default(obj):
    return obj.item() if hasattr(obj, "item") else str(obj)


def read_csv_task():
    from extract.read_csv import read_spotify_csv

    df = read_spotify_csv()
    df.to_pickle(_stg("spotify_raw"))


def validate_csv_task(**context):
    from config.settings import OUTPUT_DIR
    import pandas as pd
    from validate.quarantine import save_quarantine
    from validate.spotify_schema import validate_spotify

    df = pd.read_pickle(_stg("spotify_raw"))

    if context["params"].get("simulate_bad_data"):
        df = df.copy()
        bad = df.sample(frac=0.10, random_state=1).index
        df.loc[bad, "popularity"] = -5
        print("simulate_bad_data=True: se corrompió el 10 % de la columna popularity")

    result = validate_spotify(df)
    s = result.summary

    save_quarantine(result.invalid_df)
    result.valid_df.to_pickle(_stg("spotify_valid"))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report = {**s, "run_id": context["run_id"], "simulate_bad_data": bool(context["params"].get("simulate_bad_data"))}
    (OUTPUT_DIR / "validation_report.json").write_text(
        json.dumps(report, indent=2, default=_json_default, ensure_ascii=False), encoding="utf-8"
    )

    print(f"[validate] {s['valid_rows']}/{s['total_rows']} filas válidas | "
          f"inválidas: {s['invalid_rows']} ({s['invalid_pct']} %) | umbral: {s['threshold_pct']} %")
    for rule in s["failed_rules"]:
        print(f"   - {rule['rule']}: {rule['rows']} filas")

    if result.passed:
        print("OKI DOKI VALIDACIÓN OK -> se continúa con transform_csv")
        return "transform_csv"
    print("VALIDACIÓN FALLÓ -> rama de alerta; no se cargará nada al warehouse")
    return "alert_validation_failed"


def transform_csv_task():
    import pandas as pd
    from transform.spotify import transform_spotify

    sp_art = transform_spotify(pd.read_pickle(_stg("spotify_valid")))
    sp_art.to_pickle(_stg("spotify_artists"))


def read_db_task():
    from extract.read_db import read_grammys_db

    read_grammys_db().to_pickle(_stg("grammys_raw"))


def transform_db_task():
    import pandas as pd
    from transform.grammys import transform_grammys

    gr_art = transform_grammys(pd.read_pickle(_stg("grammys_raw")))
    gr_art.to_pickle(_stg("grammys_artists"))


def merge_task(**context):
    import pandas as pd
    from transform.merge import merge_artists

    merged = merge_artists(
        pd.read_pickle(_stg("spotify_artists")),
        pd.read_pickle(_stg("grammys_artists")),
        run_id=context["run_id"],
    )
    merged.to_pickle(_stg("merged"))


def load_task():
    import pandas as pd
    from load.postgres import load_to_postgres

    load_to_postgres(pd.read_pickle(_stg("merged")))


def store_task():
    from load.csv_export import export_table_to_csv

    export_table_to_csv()


def alert_task():
    from config.settings import OUTPUT_DIR

    report = json.loads((OUTPUT_DIR / "validation_report.json").read_text(encoding="utf-8"))
    print("=" * 70)
    print("OJO ALERTA DE CALIDAD DE DATOS: la validación de Spotify FALLÓ")
    print(f"   Filas inválidas : {report['invalid_rows']} de {report['total_rows']} ({report['invalid_pct']} %)")
    print(f"   Umbral          : {report['threshold_pct']} %")
    print(f"   Fallo estructural: {report['structural_failure']}")
    for rule in report["failed_rules"]:
        print(f"   - {rule['rule']}: {rule['rows']} filas")
    print("   Revisar: output/quarantine/spotify_invalid.csv")
    print("=" * 70)


default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="etl_spotify_grammys",
    description="ETL Spotify (CSV) + Grammys (SQL) -> Postgres + CSV, con validación Pandera",
    doc_md=__doc__,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    params={"simulate_bad_data": False},
    tags=["workshop", "etl", "spotify", "grammys"],
) as dag:

    read_csv = PythonOperator(task_id="read_csv", python_callable=read_csv_task)

    validate_csv = BranchPythonOperator(
        task_id="validate_csv", python_callable=validate_csv_task, retries=0
    )

    transform_csv = PythonOperator(task_id="transform_csv", python_callable=transform_csv_task)

    alert_validation_failed = PythonOperator(
        task_id="alert_validation_failed", python_callable=alert_task, retries=0
    )

    read_db = PythonOperator(task_id="read_db", python_callable=read_db_task)
    transform_db = PythonOperator(task_id="transform_db", python_callable=transform_db_task)

    merge = PythonOperator(task_id="merge", python_callable=merge_task)
    load = PythonOperator(task_id="load", python_callable=load_task)
    store = PythonOperator(task_id="store", python_callable=store_task)

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    read_csv >> validate_csv >> [transform_csv, alert_validation_failed]
    read_db >> transform_db
    [transform_csv, transform_db] >> merge >> load >> store >> end
    alert_validation_failed >> end