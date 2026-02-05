import polars as pl
from pathlib import Path
from colorstreak import Logger as log

FILE = Path(__file__).parent / "data" / "bitacora_todos_concentrado.csv"




# ========================================
# Función de auditoría de errores
# ========================================
def auditar_errores(lf, candidate_col: str, target_name: str, evidence_col: str):
    lf_audited = (
        lf
        .with_columns(
            # Generamos la columna de Error (Temporalmente)
            ERROR_TEMP = pl.when(pl.col(candidate_col).is_null()) 
                        .then(pl.struct(
                            Original=pl.col(target_name),
                            Intento=pl.col(evidence_col)
                        ))
                        .otherwise(None)
        )
        .with_columns(
            pl.col(candidate_col).alias(target_name)
        )
    )

    df_total = lf_audited.collect()

    # Filtros
    df_errores = df_total.filter(pl.col("ERROR_TEMP").is_not_null())
    
    # Reporte
    log.info(f"[{target_name}] Registros Procesados: {df_total.height:,}")
    
    if df_errores.height > 0:
        log.warning(f"[{target_name}] Errores: {df_errores.height:,}")
        # Desempaquetamos para ver detalle
        log.error(df_errores.select("ERROR_TEMP").unnest("ERROR_TEMP"))
    
    # 5. RETORNO LIMPIO
    return (
        df_total
        .filter(pl.col("ERROR_TEMP").is_null())
        # Eliminamos columnas temporales
        .drop([candidate_col, evidence_col, "ERROR_TEMP"])
        .lazy()
    )

# ========================================
# Helpers de limpieza
# ========================================
def texto(col_obj: str):
    return (
        pl.col(col_obj)
        .str.to_lowercase()                      # Estandarizar: Minúsculas
        .str.replace_all(r"[^a-záéíóúüñ\s]", "") # Limpieza: Solo letras y espacios
        .str.replace_all(r"\s+", " ")            # Limpieza: Unificar espacios múltiples
        .str.strip_chars()                       # Limpieza: Bordes vacíos
    )

# ========================================
# Tratamientos
# ========================================
def tratar_cecom(lf, col_obj: str):
    clean_cecom_expr = (
        texto(col_obj)
    )
    
    cecom = (
        lf
        .with_columns(clean_cecom = clean_cecom_expr)
        .with_columns(
            final_cecom = pl.when(pl.col("clean_cecom") == "") # Si quedó vacío tras limpiar
                          .then(pl.lit("se desconoce"))              # Poner "se desconoce"
                          .otherwise(pl.col("clean_cecom"))    # Si no, es el nombre
                          .fill_null(pl.lit("se desconoce"))        # Si es NULL, poner "se desconoce"
        )
    )
    return cecom

def tratar_fecha(lf, col_obj: str):
    # REGLAS DE LIMPIEZA
    clean_fecha_expr = (
        pl.col(col_obj)
        .str.replace_all(r"[./\s:]", "-")              # Estandarizar separadores
        .str.replace_all(r"-+", "-")                   # Unificar separadores múltiples
        .str.strip_chars("-")                          # Eliminar separadores al inicio y final
        .str.replace(r"^(\d)(\d{2})-", "$2-")          # DÍA: Añadir cero inicial si falta
        .str.replace(r"-(\d{1,2})(20\d{2})", "-$1-$2") # FECHA: Reordenar si es necesario
        .str.replace(r"-(20\d{2})\d+$", "-$1")         # AÑO: Corregir formato de año si es necesario
        .str.replace(r"-(\d{2})$", "-20$1")            # AÑO: Añadir prefijo '20' si es necesario
    )
    
    # Transformación final de la fecha
    fecha = (
        lf
        .with_columns(clean_date = clean_fecha_expr)
        .with_columns(
            final_date = pl.coalesce([ 
                pl.col("clean_date").str.to_date("%d-%m-%Y", strict=False),
                pl.col("clean_date").str.to_date("%m-%d-%Y", strict=False)
            ])
        )
    )
    return fecha


def tratar_hora(lf, col_obj: str):
    # REGLAS DE LIMPIEZA
    clean_hora_expr = (
        pl.col(col_obj)
        .str.to_lowercase()                                    # Estandarizar: Minúsculas
        .str.replace(r"n/a", "9:00")                           # Imputación: N/A -> 9:00
        .str.replace(r"^24:", "00:")                           # Corrección: 24:00 -> 00:00
        .str.replace(r"(\d{2})\.(\d{2})\.(\d{2})", "$1:$2:$3") # Formato: HH.MM.SS -> HH:MM:SS
        .str.replace(r"(\d)\.(\d)", "$1:$2")                   # Formato: HH.MM -> HH:MM
        .str.replace_all(r":+", ":")                           # Limpieza: Unificar ::
        .str.replace_all(r"\.", "")                            # Limpieza: Eliminar puntos (a.m.)
        .str.replace_all(r"[|]", "")                           # Limpieza: Eliminar pipes
        .str.strip_chars()                                     # Limpieza: Bordes vacíos
        .str.replace(r"(\d)(am|pm)", "$1 $2")                  # Formato: Espacio antes de AM/PM
        .str.replace(r"^(\d{1,2}:\d{2}:\d{2}):\d+$", "$1")     # Corrección: Eliminar 4to segmento
    )
    
    # Transformación final de la hora
    hora = (
        lf
        .with_columns(clean_time = clean_hora_expr)
        .with_columns(
            final_time = pl.coalesce([
                pl.col("clean_time").str.to_time("%H:%M:%S", strict=False),
                pl.col("clean_time").str.to_time("%H:%M", strict=False),
                pl.col("clean_time").str.to_time("%I:%M:%S %p", strict=False),
                pl.col("clean_time").str.to_time("%I:%M %p", strict=False),
                pl.col("clean_time").str.to_time("%H:%M:%S %p", strict=False),
            ])
            .fill_null(pl.lit("09:00:00").str.to_time("%H:%M:%S"))
        )
    )
    return hora

def tratar_tipo_de_servicio(lf, col_obj: str):
    clean_tipo_expr = (
        texto(col_obj)
    )
    
    tipo_servicio = (
        lf
        .with_columns(clean_tipo_de_servicio = clean_tipo_expr)
        .with_columns(
            final_tipo_de_servicio = pl.when(pl.col("clean_tipo_de_servicio") == "") # Si quedó vacío tras limpiar
                                      .then(pl.lit("se desconoce"))                     # Poner "se desconoce"
                                      .otherwise(pl.col("clean_tipo_de_servicio")) # Si no, es el nombre
                                      .fill_null(pl.lit("se desconoce"))                 # Si es NULL, poner "se desconoce"
        )
    )
    return tipo_servicio

def causa(lf, col_obj: str):
    clean_causa_expr = (
        texto(col_obj)
        .str.replace("na", "se desconoce")
    )
    
    causa = (
        lf
        .with_columns(clean_causa = clean_causa_expr)
        .with_columns(
            final_causa = pl.when(pl.col("clean_causa") == "") # Si quedó vacío tras limpiar
                          .then(pl.lit("se desconoce"))              # Poner "se desconoce"
                          .otherwise(pl.col("clean_causa"))    # Si no, es el nombre
                          .fill_null(pl.lit("se desconoce"))        # Si es NULL, poner "se desconoce"
        )
    )
    return causa

# ========================================
# Pipeline Principal
# ========================================
lf = (
    pl.scan_csv(FILE)
    .pipe(tratar_cecom, "CECOM")
    .pipe(tratar_fecha, "FECHA")
    .pipe(tratar_hora, "HORA_DE_LLAMADA")
    .pipe(tratar_tipo_de_servicio, "TIPO_DE_SERVICIO")
    .pipe(causa, "CAUSA")
)

lf_cecom = auditar_errores(
    lf,
    candidate_col="final_cecom", 
    target_name="CECOM",
    evidence_col="clean_cecom"
)

lf_fecha = auditar_errores(
    lf_cecom, 
    candidate_col="final_date", 
    target_name="FECHA",
    evidence_col="clean_date"
)


lf_hora = auditar_errores(
    lf_fecha,
    candidate_col="final_time", 
    target_name="HORA_DE_LLAMADA", 
    evidence_col="clean_time"
)


lf_tipo_de_servicio = auditar_errores(
    lf_hora,
    candidate_col="final_tipo_de_servicio", 
    target_name="TIPO_DE_SERVICIO",
    evidence_col="clean_tipo_de_servicio"
)


lf_causa = auditar_errores(
    lf_tipo_de_servicio,
    candidate_col="final_causa", 
    target_name="CAUSA",
    evidence_col="clean_causa"
)


log.debug(lf_causa.select("CAUSA").unique().collect())