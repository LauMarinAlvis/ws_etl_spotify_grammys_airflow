import warnings
from dataclasses import dataclass, field

import pandas as pd
import pandera.pandas as pa
from pandera import Check

# Pandera emite un FutureWarning interno (inofensivo) al reportar fallos.
warnings.filterwarnings("ignore", category=FutureWarning, module="pandera")

INVALID_THRESHOLD = 0.05  # 5 %

_UNIT_COLS = ["danceability", "energy", "speechiness", "acousticness",
              "instrumentalness", "liveness", "valence"]

SPOTIFY_SCHEMA = pa.DataFrameSchema(
    {
        "track_id": pa.Column(str, nullable=False),
        "artists": pa.Column(str, nullable=False),
        "album_name": pa.Column(str, nullable=False),
        "track_name": pa.Column(str, nullable=False),
        "popularity": pa.Column(int, Check.in_range(0, 100)),
        "duration_ms": pa.Column(int, Check.gt(0)),
        "explicit": pa.Column(bool),
        "key": pa.Column(int, Check.in_range(-1, 11)),
        "loudness": pa.Column(float, Check.in_range(-60, 5)),
        "mode": pa.Column(int, Check.isin([0, 1])),
        "tempo": pa.Column(float, Check.gt(0)),
        "time_signature": pa.Column(int, Check.in_range(0, 7)),
        "track_genre": pa.Column(str, nullable=False),
        **{c: pa.Column(float, Check.in_range(0, 1)) for c in _UNIT_COLS},
    },
    # Una canción puede estar en varios géneros, pero (track_id, track_genre) es único.
    unique=["track_id", "track_genre"],
    report_duplicates="exclude_first",
    strict=False,
)


@dataclass
class ValidationResult:
    valid_df: pd.DataFrame
    invalid_df: pd.DataFrame
    summary: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return bool(self.summary["passed"])


def validate_spotify(df: pd.DataFrame, threshold: float = INVALID_THRESHOLD) -> ValidationResult:
    total = len(df)
    structural_failure = False
    reasons = pd.Series(dtype=object)
    rules = []

    try:
        SPOTIFY_SCHEMA.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        fc = exc.failure_cases.copy()
        # Fallos a nivel de DataFrame (p. ej. duplicados) no traen columna: se rotulan aparte.
        fc["rule"] = (fc["column"].fillna("(tabla)").astype(str) + ":"
                      + fc["check"].fillna("desconocido").astype(str))
        with_index = fc.dropna(subset=["index"])
        # Fallos sin fila asociada = fallo estructural (columna faltante, tipo incorrecto...)
        structural_failure = len(with_index) < len(fc)
        if len(with_index):
            with_index = with_index.assign(index=with_index["index"].astype(int))
            reasons = with_index.groupby("index")["rule"].agg("; ".join)
        rules = (fc.groupby("rule").size().sort_values(ascending=False)
                   .reset_index(name="rows").to_dict("records"))

    bad_idx = reasons.index
    invalid_df = df.loc[bad_idx].copy()
    invalid_df["failed_rules"] = reasons.loc[bad_idx].values
    valid_df = df.drop(index=bad_idx)

    invalid_pct = (len(invalid_df) / total) if total else 1.0
    passed = (not structural_failure) and invalid_pct <= threshold and len(valid_df) > 0

    summary = {
        "total_rows": total,
        "valid_rows": len(valid_df),
        "invalid_rows": len(invalid_df),
        "invalid_pct": round(invalid_pct * 100, 3),
        "threshold_pct": threshold * 100,
        "structural_failure": structural_failure,
        "passed": passed,
        "failed_rules": rules,
    }
    return ValidationResult(valid_df=valid_df, invalid_df=invalid_df, summary=summary)
