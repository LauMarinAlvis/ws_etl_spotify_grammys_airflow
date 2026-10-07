"""Fase EXTRACT (fuente 1): lee el dataset de Spotify desde un archivo CSV.

Entradas : data/raw/spotify_tracks.csv
Salidas  : DataFrame crudo (sin la columna índice 'Unnamed: 0')
"""
from pathlib import Path

import pandas as pd

from config.settings import DATA_RAW

SPOTIFY_CSV = DATA_RAW / "spotify_tracks.csv"


def read_spotify_csv(path=None) -> pd.DataFrame:
    path = Path(path) if path else SPOTIFY_CSV
    if not path.exists():
        # Falla de forma visible: nunca continuar con un DataFrame vacío.
        raise FileNotFoundError(f"No existe el archivo de Spotify: {path}")

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"El archivo {path.name} está vacío")

    # Columna índice heredada del CSV original; no aporta información.
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    print(f"[read_csv] {path.name}: {len(df)} filas, {df.shape[1]} columnas")
    return df
