import pandas as pd

from config.settings import QUARANTINE_DIR

QUARANTINE_CSV = QUARANTINE_DIR / "spotify_invalid.csv"


def save_quarantine(invalid_df: pd.DataFrame, path=None):
    """Sobrescribe el archivo en cada ejecución (idempotente)."""
    path = path or QUARANTINE_CSV
    path.parent.mkdir(parents=True, exist_ok=True)
    invalid_df.to_csv(path, index=False, encoding="utf-8")
    print(f"[quarantine] {len(invalid_df)} filas -> {path}")
    return path
