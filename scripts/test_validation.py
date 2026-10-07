import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from extract.read_csv import read_spotify_csv
from validate.spotify_schema import validate_spotify


def show(title, result):
    s = result.summary
    estado = "OKI PASA" if result.passed else "FALLA"
    print(f"\n=== {title} ===")
    print(f"{estado} | inválidas: {s['invalid_rows']} de {s['total_rows']} ({s['invalid_pct']} %) "
          f"| umbral {s['threshold_pct']} % | fallo estructural: {s['structural_failure']}")
    for r in s["failed_rules"][:6]:
        print(f"   - {r['rule']}: {r['rows']}")


df = read_spotify_csv()

r1 = validate_spotify(df)
show("Caso 1: datos reales de Spotify", r1)

malo = df.copy()
malo.loc[malo.sample(frac=0.10, random_state=1).index, "popularity"] = -5
r2 = validate_spotify(malo)
show("Caso 2: 10 % de filas con popularity fuera de rango", r2)

r3 = validate_spotify(df.drop(columns=["tempo"]))
show("Caso 3: falta la columna tempo", r3)

assert r1.passed, "Los datos reales deberían pasar"
assert not r2.passed, "Con 10 % inválido debería fallar"
assert not r3.passed, "Sin la columna tempo debería fallar"
print("\n OK Las 3 pruebas se comportaron como se esperaba")
