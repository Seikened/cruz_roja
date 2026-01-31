import polars as pl
from pathlib import Path
from colorstreak import Logger as log

base_dir = Path(__file__).resolve().parent
año = "2024"
meses = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
archivos_a_leer = [base_dir / "data" / año / f"bitacora_{año}_{mes}.csv" for mes in meses]

archivo_salida = base_dir / "data" / año / f"bitacora_{año}_concentrado.csv"


lista_df = [pl.read_csv(archivo,infer_schema_length=0) for archivo in archivos_a_leer]


filas_totales = sum(df.height for df in lista_df)
log.info(f"Cantidad total de filas recuperadas: {filas_totales}")
for df in lista_df:
    
    log.info(f"Columnas: {df.columns}")


df_final = pl.concat(lista_df, how="vertical")
df_final.write_csv(archivo_salida)
cantidad_filas = df_final.height
log.success(f"Cantidad de filas en el DataFrame final: {cantidad_filas}")
log.debug(f"Primeras filas del DataFrame: \n{df_final}")