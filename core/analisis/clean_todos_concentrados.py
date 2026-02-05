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
# Tratamientamientos
# ========================================
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
        .str.to_lowercase()          # Convertir a minúsculas
        .str.replace_all(r"\.", "")  # Eliminar puntos
        .str.replace_all(r"[|]", "") # Eliminamos el pipe 
        
        
    )
    
    # Transformación final de la hora
    hora = (
        lf
        .with_columns(clean_time = clean_hora_expr)
        .with_columns(
            final_time = pl.col("clean_time").str.to_time("%H:%M", strict=False)
        )
    )
    return hora


# ========================================
# Pipeline Principal
# ========================================
lf = (
    pl.scan_csv(FILE)
    .pipe(tratar_fecha, "FECHA")
    .pipe(tratar_hora, "HORA_DE_LLAMADA")
)




df_limpio = auditar_errores(
    lf, 
    candidate_col="final_time", 
    target_name="HORA_DE_LLAMADA", 
    evidence_col="clean_time"
)