import polars as pl
from pathlib import Path

mapa_correcciones = {
    "TIPO DE SALIDA": "TIPO DE SERVICIO",
    "HORA DE TÉRMINO DE SERVICIO": "HORA EN BASE",
    "KM CIERRE": "KM REGRESO",
}

def leer_bitacora_csv(archivo: Path) -> pl.DataFrame:
    df = pl.read_csv(
        archivo,
        infer_schema_length=10000,
        schema_overrides={"FRAP_duplicated_0": pl.Float64},  # si existe
    )

    # Normalizar nombres
    df = df.rename({c: c.strip() for c in df.columns})

    # Renombrar columnas inconsistentes
    columnas_a_cambiar = {
        old: new for old, new in mapa_correcciones.items() if old in df.columns
    }
    if columnas_a_cambiar:
        print(f"{archivo.name}: renombrando {columnas_a_cambiar}")
        df = df.rename(columnas_a_cambiar)

    # Normalizar tipos numéricos
    numeric_cols = [
        c for c, dt in df.schema.items()
        if dt in (pl.Int64, pl.Int32, pl.Float64, pl.Float32)
    ]
    if numeric_cols:
        df = df.with_columns([pl.col(c).cast(pl.Float64) for c in numeric_cols])

    # Fuente (muy útil)
    df = df.with_columns(pl.lit(archivo.name).alias("FUENTE"))

    return df


base_dir = Path(__file__).resolve().parent

archivo_ene_26 = base_dir / "data" / "2026" / "BSU-ENE26.csv"

df_ene_26 = leer_bitacora_csv(archivo_ene_26)

print(df_ene_26.shape)
print(df_ene_26.head())
