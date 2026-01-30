import polars as pl
from pathlib import Path
import re

base_dir = Path(__file__).resolve().parent
archivo_entrada = base_dir / "data" / "2023" / "bitacora_2023_sep.csv"
archivo_salida = base_dir / "data" / "2023" / "datos_limpios_2023_sep.csv"

"""
ARCHIVO DE EXTRACCIÓN ESPECIAL PARA SEPTIEMBRE 2023
"""

# Regex para capturar fechas (dd/mm/yyyy, mm/dd/yy, etc)
RX_FECHA = r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b"
# Regex para identificar filas de datos (empiezan con hora tipo 10:50 o 9.45)
RX_HORA  = r"^\d{1,2}[:.]\d{2}"

def parse_fecha_inteligente(fecha_str):
    """
    Resuelve el conflicto 1/9 (MX) vs 9/7 (US).
    Asume que si un número es > 12, ese es el día.
    Si es ambiguo (9/7), asume el formato 'MX' por defecto, 
    PERO si el mes resultante no es Septiembre (9) y el otro valor sí es 9, invierte.
    (Ajustado específicamente para rescatar tu bitácora de Septiembre)
    """
    if not fecha_str: 
        return None
    
    # Normalizar separadores
    partes = re.split(r"[./-]", str(fecha_str).strip())
    if len(partes) != 3: 
        return None
    
    a, b, y = int(partes[0]), int(partes[1]), int(partes[2])
    
    # Normalizar año (23 -> 2023)
    if y < 100: 
        y += 2000
    
    # Lógica de desempate
    # Caso 1: Formato obvio (13/9) -> 13 es día
    if a > 12: 
        return f"{y}-{b:02d}-{a:02d}" # a=Día, b=Mes
    if b > 12: 
        return f"{y}-{a:02d}-{b:02d}" # a=Mes, b=Día
    
    # Caso 2: Ambiguo (9/7). ¿Es 9 de Julio o 7 de Sept?
    if a == 9 and b != 9:
        return f"{y}-{a:02d}-{b:02d}" # Asumimos a=Mes (Septiembre)
        
    # Default: Asumir formato MX (Día/Mes)
    return f"{y}-{b:02d}-{a:02d}"

df = (
    pl.read_csv(archivo_entrada, has_header=False, infer_schema_length=0, truncate_ragged_lines=True)
    .with_columns(pl.all().str.strip_chars())
    

    .with_columns(
        pl.coalesce([
            pl.when(pl.col(c).str.contains(RX_FECHA)).then(pl.col(c)).otherwise(None)
            for c in ["column_1", "column_2", "column_3", "column_4", "column_5"]
        ]).alias("CTX_RAW_DATE")
    )
    
    # 2. SATURACIÓN (Rellenar huecos hacia abajo)
    .with_columns(
        pl.col("CTX_RAW_DATE").str.extract(RX_FECHA, 0).forward_fill().alias("FECHA_BASE")
    )
    
    # 3. FILTRADO (Quedarnos solo con registros operativos)
    .filter(
        (
            pl.col("column_1").str.contains(RX_FECHA) |      # Formato Nuevo
            pl.col("column_1").str.contains(RX_HORA) |       # Formato Viejo
            pl.col("column_11").str.contains(RX_HORA)        # Formato Nuevo desplazado
        ) &
        ~pl.col("column_3").str.to_uppercase().str.contains("TURNO") # Eliminar headers
    )
    
    # 4. NORMALIZACIÓN DE COLUMNAS (Alinear el Tetris)
    .select([
        # Aplicamos el parser inteligente a la fecha detectada
        pl.when(pl.col("column_1").str.contains(RX_FECHA))
          .then(pl.col("column_1").str.extract(RX_FECHA, 0))
          .otherwise(pl.col("FECHA_BASE"))
          .map_elements(parse_fecha_inteligente, return_dtype=pl.Utf8) # Aplicar corrección Python
          .str.to_date("%Y-%m-%d") # Convertir a objeto fecha real
          .alias("FECHA"),

        # Alinear HORA (Col 1 o Col 11)
        pl.when(pl.col("column_1").str.contains(RX_HORA))
          .then(pl.col("column_1"))
          .otherwise(pl.col("column_11"))
          .alias("HORA"),
          
        
        pl.when(pl.col("column_1").str.contains(RX_FECHA))
          .then(pl.col("column_2")) 
          .otherwise(pl.col("column_2"))
          .alias("TIPO_SERVICIO"),
          
        pl.when(pl.col("column_1").str.contains(RX_FECHA))
          .then(pl.col("column_3"))
          .otherwise(pl.col("column_3"))
          .alias("EMERGENCIA"),
          
        pl.when(pl.col("column_1").str.contains(RX_FECHA))
          .then(pl.col("column_4"))
          .otherwise(pl.col("column_3")) 
          .alias("UBICACION"),
          
        pl.when(pl.col("column_1").str.contains(RX_FECHA))
          .then(pl.col("column_8"))
          .otherwise(pl.col("column_7"))
          .alias("UNIDAD"),
          
        # Rescatamos resultados y folios
        pl.col("column_16").alias("RESULTADO"),
        pl.col("column_17").alias("FRAP")
    ])
)

# Guardar resultado
df.write_csv(archivo_salida)

# Verificación visual
print(f"Filas recuperadas: {df.height}")
print(df.head(10))