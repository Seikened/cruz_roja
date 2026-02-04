import polars as pl
from pathlib import Path
from colorstreak import Logger as log

base_dir = Path(__file__).resolve().parent
años = ["2022", "2023", "2024"]

cols_objetivo = [
    'CECOM', 'FECHA', 'HORA DE LLAMADA', 'TIPO DE SERVICIO', 'CALLE PRINCIPAL', 
    'CRUCE', 'COLONIA O COMUNIDAD', 'REPORTANTE', 'UNIDAD ASIGNADA', 'OPERADOR', 
    'JEFE DE SERVICIO', 'HORA DE SALIDA', 'HORA DE LLEGADA/  CANCELACIÓN', 
    'HORA EN BASE', 'RESULTADO', 'ORIGEN/CAUSA', 'FRAP', 'KM SALIDA', 
    'KM REGRESO', 'TIEMPO DE SALIDA', 'TIEMPO DE RESPUESTA', 
    'DURACIÓN DEL SERVICIO', 'KM RECORRIDO'
]

mapa_renombre_2022 = {
    "TURNO": "CECOM",
    "HORA": "HORA DE LLAMADA",
    "TIPO": "TIPO DE SERVICIO",
    "CALLE": "CALLE PRINCIPAL",
    "COLONIA": "COLONIA O COMUNIDAD",
    "UNIDAD": "UNIDAD ASIGNADA",
    "H_SALIDA": "HORA DE SALIDA",
    "H_LLEGADA": "HORA DE LLEGADA/  CANCELACIÓN",
    "KM_SALIDA": "KM SALIDA"
}

archivo_salida = base_dir / "data" / "bitacora_todos_concentrado.csv"
lista_df = []

for año in años:
    archivo = base_dir / "data" / año / f"bitacora_{año}_concentrado.csv"
    

    df = pl.read_csv(archivo, infer_schema_length=0) # el infer_schema_length es para leer como string
    
    if año == "2022":
        log.info(f"Estandarizando columnas del año {año}...")
        
        df = df.rename(mapa_renombre_2022)

        columnas_faltantes = [col for col in cols_objetivo if col not in df.columns]
        
        if columnas_faltantes:
            df = df.with_columns([
                pl.lit(None).cast(pl.String).alias(col) for col in columnas_faltantes
            ])
            
        # 3. Ordenar
        df = df.select(cols_objetivo)
        
    lista_df.append(df)
    log.info(f"Dataset {año} cargado - Columnas: {len(df.columns)}")


try:
    df_final = pl.concat(lista_df, how="vertical")
    df_final.write_csv(archivo_salida)
    
    df_direcciones = (
        df_final
        .select([
            pl.col("CECOM").alias("CENTRO_DE_COMANDO"),
            pl.col("CALLE PRINCIPAL").alias("CALLE"),
            pl.col("COLONIA O COMUNIDAD").alias("COLONIA")
        ])
        .unique()
    )
    df_direcciones.write_csv(base_dir / "data" / "direcciones_unicas.csv")
    
    filas_totales = df_direcciones.height
    log.success(f"Cantiddad de {filas_totales} filas.")
    log.debug(f"Dataset:\n{df_direcciones}")
except Exception as e:
    log.error(f"Error al concatenar: {e}")