import re
import unicodedata

import pandas as pd

EXCLUDED_ARTISTS = {"various artists"}


def normalize_name(value):
    """Minúsculas, sin tildes y sin espacios extra. Devuelve None si queda vacío."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text or None
