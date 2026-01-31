import polars as pl
from pathlib import Path

base_dir = Path(__file__).resolve().parent
año = "2024"
meses = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
archivos_a_leer = [base_dir / "data" / año / f"bitacora_{año}_{mes}.csv" for mes in meses]

archivo_salida = base_dir / "data" / año / f"bitacora_{año}_concentrado.csv"


lista_df = [pl.read_csv(archivo) for archivo in archivos_a_leer]


for df in lista_df:
    print(f"Columnas en el archivo: {df.columns}")
