import polars as pl
from pathlib import Path

# --- 1. CONFIGURACIÓN ---
base_dir = Path(__file__).resolve().parent
# Ajusta la ruta a tu archivo real
raw_path = base_dir / "data" / "2022" / "bitacora_2022-2023.csv"
clean_path = base_dir / "data" / "datos_limpios.csv"

# --- 2. LÓGICA (REGEX) ---
RX_FECHA = r"\d{2}[./]\d{2}[./]\d{2,4}"  # dd.mm.yy
RX_HORA  = r"^\d{1,2}[:.]\d{2}"          # 10:50 o 9.48


df = (
    pl.read_csv(raw_path, has_header=False, infer_schema_length=0)
    .with_columns(pl.all().str.strip_chars())
    
    # A) DETECCIÓN INTELIGENTE DE CONTEXTO
    .with_columns([
        # FECHA: Solo si col_1 parece fecha
        pl.when(pl.col("column_1").str.contains(RX_FECHA))
          .then(pl.col("column_1"))
          .otherwise(None)
          .alias("CTX_FECHA"),
          
        # TURNO: Aquí está el truco. Buscamos explícitamente palabras clave de turno
        # o asumimos que si hay FECHA en col_1, lo que sigue es info de turno.
        # Concatenamos cols 2, 3 y 4 para asegurar capturar "TURNO", "HORA" y "NOMBRE"
        pl.when(
            pl.col("column_1").str.contains(RX_FECHA) | 
            pl.col("column_2").str.to_uppercase().str.contains("TURNO") |
            pl.col("column_3").str.to_uppercase().str.contains("TURNO")
        )
          .then(
              pl.concat_str([
                  pl.col("column_2").fill_null(""), 
                  pl.col("column_3").fill_null(""),
                  pl.col("column_4").fill_null("")
              ], separator=" ")
          )
          .otherwise(None)
          # Limpieza extra: quitamos comas repetidas o nulos textuales si aparecen
          .str.replace_all(r"nan|null", "") 
          .str.strip_chars()
          .alias("CTX_TURNO")
    ])
    
    # B) PROPAGACIÓN (Forward Fill)
    .with_columns([
        pl.col("CTX_FECHA").forward_fill(),
        pl.col("CTX_TURNO").forward_fill()
    ])
    
    # C) FILTRADO DE FILAS DE DATOS
    # Condición estricta: Col 1 parece hora Y Col 2 tiene texto (no es vacío)
    # Y IMPORTANTE: No es una fila de encabezado de fecha (evita duplicados)
    .filter(
        pl.col("column_1").str.contains(RX_HORA) & 
        ~pl.col("column_1").str.contains(RX_FECHA) &
        pl.col("column_2").is_not_null()
    )
    
    # D) SELECCIÓN FINAL (Mapeo 1-16)
    .select([
        pl.col("CTX_FECHA").alias("FECHA"),
        pl.col("CTX_TURNO").alias("TURNO"),
        pl.col("column_1").alias("HORA"),
        pl.col("column_2").alias("TIPO"),
        pl.col("column_3").alias("CALLE"),
        pl.col("column_4").alias("CRUCE"),
        pl.col("column_5").alias("COLONIA"),
        pl.col("column_6").alias("REPORTANTE"),
        pl.col("column_7").alias("UNIDAD"),
        pl.col("column_8").alias("OPERADOR"),
        pl.col("column_10").alias("H_SALIDA"),
        pl.col("column_11").alias("H_LLEGADA"),
        pl.col("column_13").alias("KM_SALIDA"),
        pl.col("column_15").alias("RESULTADO"),
        pl.col("column_16").alias("FRAP")
    ])
)

# --- 4. EXPORTAR ---
df.write_csv(clean_path)
print(df.head(5))