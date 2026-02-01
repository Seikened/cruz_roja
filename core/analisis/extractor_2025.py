import polars as pl
from pathlib import Path

base_dir = Path(__file__).resolve().parent

archivos_a_leer = [
    base_dir / "data" / "2025" / "BSU-ENE25.csv",
    base_dir / "data" / "2025" / "BSU-FEB25.csv",
    base_dir / "data" / "2025" / "BSU-MAR25.csv",
    base_dir / "data" / "2025" / "BSU-ABR25.csv",
    base_dir / "data" / "2025" / "BSU-MAY25.csv",
    base_dir / "data" / "2025" / "BSU-JUN25.csv",
    base_dir / "data" / "2025" / "BSU-JUL25.csv",
    base_dir / "data" / "2025" / "BSU-AGO25.csv",
    base_dir / "data" / "2025" / "BSU-SEP25.csv",
    base_dir / "data" / "2025" / "BSU-OCT25.csv",
    base_dir / "data" / "2025" / "BSU-NOV25.csv",
    base_dir / "data" / "2025" / "BSU-DIC25.csv"
]

archivo_salida = base_dir / "data" / "2025" / "bitacora_2025_concentrado.csv"


mapa_correcciones = {
    "TIPO DE SALIDA": "TIPO DE SERVICIO",
    "HORA DE TÉRMINO DE SERVICIO": "HORA EN BASE",
    "KM CIERRE": "KM REGRESO"
}

lista_limpia = []

for archivo in archivos_a_leer:
    df = pl.read_csv(
        archivo,
        infer_schema_length=10000,
        schema_overrides={"FRAP_duplicated_0": pl.Float64},  
    )

    df = df.rename({c: c.strip() for c in df.columns})

    columnas_a_cambiar = {old: new for old, new in mapa_correcciones.items() if old in df.columns}
    if columnas_a_cambiar:
        print(f"{archivo.name}: renombrando {columnas_a_cambiar}")
        df = df.rename(columnas_a_cambiar)

    df = df.with_columns(pl.lit(archivo.name).alias("FUENTE"))

    # Evitar Int vs Float
    numeric_cols = [
        c for c, dt in df.schema.items()
        if dt in (pl.Int64, pl.Int32, pl.Float64, pl.Float32)
    ]
    if numeric_cols:
        df = df.with_columns([pl.col(c).cast(pl.Float64) for c in numeric_cols])

    lista_limpia.append(df)

# Concatenar por columnas (rellena faltantes con null)
df_final = pl.concat(lista_limpia, how="diagonal")

print(f"Cantidad de filas recuperadas: {df_final.height}")
print(df_final.head())

df_final.write_csv(archivo_salida)
print(f"Archivo guardado en: {archivo_salida}")