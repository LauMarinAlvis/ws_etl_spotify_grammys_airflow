import uuid
from datetime import datetime, timezone

import pandas as pd


def merge_artists(sp_art: pd.DataFrame, gr_art: pd.DataFrame, run_id: str | None = None) -> pd.DataFrame:
    """LEFT JOIN desde Spotify: se conservan todos los artistas y se marca cuáles tienen Grammy."""
    n_before = len(sp_art)
    if sp_art["artist_norm"].duplicated().any() or gr_art["artist_norm"].duplicated().any():
        raise ValueError("Claves de artista duplicadas antes del merge")

    merged = sp_art.merge(gr_art, on="artist_norm", how="left", validate="one_to_one")

    # Control de integridad: no continuar si el merge pierde o duplica registros.
    if len(merged) != n_before:
        raise ValueError(f"El merge cambió el número de filas: {n_before} -> {len(merged)}")

    merged["grammy_wins"] = merged["grammy_wins"].fillna(0).astype(int)
    merged["first_win"] = merged["first_win"].astype("Int64")
    merged["last_win"] = merged["last_win"].astype("Int64")
    merged["has_grammy"] = merged["grammy_wins"] > 0

    # Metadatos de trazabilidad
    merged["_pipeline_run_id"] = run_id or str(uuid.uuid4())
    merged["_extracted_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    unmatched = len(gr_art) - int(merged["has_grammy"].sum())
    print(f"[merge] {len(merged)} artistas | con Grammy: {int(merged['has_grammy'].sum())} "
          f"| artistas Grammy sin match en Spotify: {unmatched}")
    return merged
