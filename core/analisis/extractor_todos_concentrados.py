import polars as pl
from pathlib import Path
from colorstreak import Logger as log

base_dir = Path(__file__).resolve().parent

años = ["2022", "2023", "2024", "2025", "2026"]


columnas_finales = [
    "CECOM", "FECHA", "HORA_DE_LLAMADA", "TIPO_DE_SERVICIO", "CALLE", 
    "CRUCE", "COLONIA", "REPORTANTE", "UNIDAD_ASIGNADA", "OPERADOR", 
    "JEFE_DE_SERVICIO", "HORA_DE_SALIDA", "HORA_DE_LLEGADA", "HORA_EN_BASE", 
    "RESULTADO", "CAUSA", "FRAP", "FOLIO_911", "KM_SALIDA", 
    "KM_REGRESO", "TIEMPO_DE_SALIDA", "TIEMPO_DE_RESPUESTA", 
    "DURACION_DEL_SERVICIO", "KM_RECORRIDO"
]

mapeo_por_columna = {
    "CECOM": { "2022":"TURNO", "2023":"CECOM", "2024":"CECOM", "2025":"CECOM", "2026":"CECOM" },
    "FECHA": { "2022":"FECHA", "2023":"FECHA", "2024":"FECHA", "2025":"FECHA", "2026":"FECHA" },
    "HORA_DE_LLAMADA": { "2022":"HORA", "2023":"HORA DE LLAMADA", "2024":"HORA DE LLAMADA", "2025":"HORA DE LLAMADA", "2026":"HORA DE LLAMADA" },
    "TIPO_DE_SERVICIO": { "2022":"TIPO", "2023":"TIPO DE SERVICIO", "2024":"TIPO DE SERVICIO", "2025":"TIPO DE SERVICIO", "2026":"TIPO DE SERVICIO" },
    "CALLE" : { "2022":"CALLE", "2023":"CALLE PRINCIPAL", "2024":"CALLE PRINCIPAL", "2025":"CALLE PRINCIPAL", "2026":"CALLE PRINCIPAL" },
    "CRUCE" : { "2022":"CRUCE", "2023":"CRUCE", "2024":"CRUCE", "2025":"CRUCE", "2026":"CRUCE" },
    "COLONIA" : { "2022":"COLONIA", "2023":"COLONIA O COMUNIDAD", "2024":"COLONIA O COMUNIDAD", "2025":"COLONIA O COMUNIDAD", "2026":"COLONIA O COMUNIDAD" },
    "REPORTANTE" : { "2022":"REPORTANTE", "2023":"REPORTANTE", "2024":"REPORTANTE", "2025":"REPORTANTE", "2026":"REPORTANTE" },
    "UNIDAD_ASIGNADA" : { "2022":"UNIDAD", "2023":"UNIDAD ASIGNADA", "2024":"UNIDAD ASIGNADA", "2025":"UNIDAD ASIGNADA", "2026":"UNIDAD ASIGNADA" },
    "OPERADOR" : { "2022":"OPERADOR", "2023":"OPERADOR", "2024":"OPERADOR", "2025":"OPERADOR", "2026":"OPERADOR" },
    "JEFE_DE_SERVICIO" : { "2022":"VACIO", "2023":"JEFE DE SERVICIO", "2024":"JEFE DE SERVICIO", "2025":"JEFE DE SERVICIO", "2026":"JEFE DE SERVICIO" },
    "HORA_DE_SALIDA" : { "2022":"H_SALIDA", "2023":"HORA DE SALIDA", "2024":"HORA DE SALIDA", "2025":"HORA DE SALIDA", "2026":"HORA DE SALIDA" },
    "HORA_DE_LLEGADA" : { "2022":"H_LLEGADA", "2023":"HORA DE LLEGADA/  CANCELACIÓN", "2024":"HORA DE LLEGADA/  CANCELACIÓN", "2025":"HORA DE LLEGADA/  CANCELACIÓN", "2026":"HORA DE LLEGADA/  CANCELACIÓN" }, # Ojo con los espacios en LLEGADA
    "HORA_EN_BASE" : { "2022":"VACIO", "2023":"HORA EN BASE", "2024":"HORA EN BASE", "2025":"HORA EN BASE", "2026":"HORA EN BASE" },
    "RESULTADO" : { "2022":"RESULTADO", "2023":"RESULTADO", "2024":"RESULTADO", "2025":"RESULTADO", "2026":"RESULTADO" },
    "CAUSA" : { "2022":"TIPO", "2023":"ORIGEN/CAUSA", "2024":"ORIGEN/CAUSA", "2025":"ORIGEN/CAUSA", "2026":"ORIGEN/CAUSA" },
    "FRAP" : { "2022":"FRAP", "2023":"FRAP", "2024":"FRAP", "2025":"FRAP", "2026":"FRAP" },
    "FOLIO_911" : { "2022":"VACIO", "2023":"VACIO", "2024":"VACIO", "2025":"FOLIO 9-1-1", "2026":"FOLIO 9-1-1" },
    "KM_SALIDA" : { "2022":"KM_SALIDA", "2023":"KM SALIDA", "2024":"KM SALIDA", "2025":"KM SALIDA", "2026":"KM SALIDA" },
    "KM_REGRESO" : { "2022":"VACIO", "2023":"KM REGRESO", "2024":"KM REGRESO", "2025":"KM REGRESO", "2026":"KM REGRESO" },
    "TIEMPO_DE_SALIDA" : { "2022":"VACIO", "2023":"TIEMPO DE SALIDA", "2024":"TIEMPO DE SALIDA", "2025":"TIEMPO DE SALIDA", "2026":"TIEMPO DE SALIDA" },
    "TIEMPO_DE_RESPUESTA" : { "2022":"VACIO", "2023":"TIEMPO DE RESPUESTA", "2024":"TIEMPO DE RESPUESTA", "2025":"TIEMPO DE RESPUESTA", "2026":"TIEMPO DE RESPUESTA" },
    "DURACION_DEL_SERVICIO" : { "2022":"VACIO", "2023":"DURACIÓN DEL SERVICIO", "2024":"DURACIÓN DEL SERVICIO", "2025":"DURACIÓN DEL SERVICIO", "2026":"DURACIÓN DEL SERVICIO" },
    "KM_RECORRIDO" : { "2022":"VACIO", "2023":"KM RECORRIDO", "2024":"KM RECORRIDO", "2025":"KM RECORRIDO", "2026":"KM RECORRIDO" },
}

archivo_salida = base_dir / "data" / "bitacora_todos_concentrado.csv"
lista_df = []

for año in años:
    archivo = base_dir / "data" / año / f"bitacora_{año}_concentrado.csv"
    
    df = pl.read_csv(archivo, infer_schema_length=0)
    
    expresiones_seleccion = []

    for col_final in columnas_finales:
        nombre_origen = mapeo_por_columna[col_final][año]
        
        if nombre_origen == "VACIO":
            expr = pl.lit(None).cast(pl.String).alias(col_final)
        else:
            expr = pl.col(nombre_origen).alias(col_final)
        
        expresiones_seleccion.append(expr)
    df_procesado = df.select(expresiones_seleccion)
    
    lista_df.append(df_procesado)
    log.info(f"Dataset {año} procesado | Filas: {df_procesado.height} | Mapeo exitoso.")

try:
    df_final = pl.concat(lista_df, how="vertical")
    df_final.write_csv(archivo_salida)
    
    df_direcciones = (
        df_final
        .select([
            pl.col("CECOM").alias("CENTRO_DE_COMANDO"),
            pl.col("CALLE"),
            pl.col("COLONIA")
        ])
        #.unique()
    )
    df_direcciones.write_csv(base_dir / "data" / "direcciones_unicas.csv")
    
    log.success(f"Archivo generado con {df_final.height} registros.")
    log.debug(f"Columnas finales: {df_final.columns}")
except Exception as e:
    log.error(f"Error en el proceso final: {e}")