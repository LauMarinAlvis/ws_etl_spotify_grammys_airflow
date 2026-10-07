import pandas as pd

from transform.text_utils import EXCLUDED_ARTISTS, normalize_name


def transform_spotify(df: pd.DataFrame) -> pd.DataFrame:
    """Una fila por artista con métricas agregadas de sus canciones."""
    songs = (
        df.dropna(subset=["artists"])
          .drop_duplicates(subset="track_id")          # misma canción repetida en varios géneros
          .assign(artist_norm=lambda d: d["artists"].str.split(";"))
          .explode("artist_norm")                       # 'A;B' -> una fila por artista
    )
    songs["artist_norm"] = songs["artist_norm"].map(normalize_name)
    songs = songs[songs["artist_norm"].notna() & ~songs["artist_norm"].isin(EXCLUDED_ARTISTS)]

    out = (
        songs.groupby("artist_norm")
             .agg(n_tracks=("track_id", "nunique"),
                  avg_popularity=("popularity", "mean"),
                  avg_danceability=("danceability", "mean"),
                  avg_energy=("energy", "mean"),
                  avg_valence=("valence", "mean"))
             .reset_index()
    )
    num_cols = [c for c in out.columns if c.startswith("avg_")]
    out[num_cols] = out[num_cols].round(4)
    print(f"[transform_spotify] {len(df)} filas -> {len(out)} artistas")
    return out
