import re

import pandas as pd

from transform.text_utils import EXCLUDED_ARTISTS, normalize_name


def _names_in_parentheses(workers):
    """'Amy Winehouse, songwriter (Amy Winehouse)' -> ['amy winehouse']."""
    if workers is None or (not isinstance(workers, str) and pd.isna(workers)):
        return []
    names = (normalize_name(x) for x in re.findall(r"\(([^)]*)\)", workers))
    return [n for n in names if n]


def transform_grammys(df: pd.DataFrame) -> pd.DataFrame:
    """Una fila por artista con sus premios.

    El artista sale de la columna `artist` y, cuando esta viene vacía (≈38 %),
    de los nombres entre paréntesis de `workers`.
    """
    gr = df.copy()
    gr["artist_norm"] = gr["artist"].map(normalize_name)
    gr["paren"] = gr["workers"].map(_names_in_parentheses)
    gr["key"] = [
        ([a] if isinstance(a, str) else []) + p
        for a, p in zip(gr["artist_norm"], gr["paren"])
    ]

    keys = (
        gr.explode("key")
          .dropna(subset=["key"])
          .reset_index(names="row_id")
          .drop_duplicates(["row_id", "key"])           # un premio cuenta una vez por artista
    )
    keys = keys[~keys["key"].isin(EXCLUDED_ARTISTS)]

    out = (
        keys.groupby("key")
            .agg(grammy_wins=("category", "count"),
                 first_win=("year", "min"),
                 last_win=("year", "max"))
            .reset_index()
            .rename(columns={"key": "artist_norm"})
    )
    print(f"[transform_grammys] {len(df)} premios -> {len(out)} artistas")
    return out
