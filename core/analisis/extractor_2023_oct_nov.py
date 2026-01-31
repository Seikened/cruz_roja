import polars as pl
from pathlib import Path

base_dir = Path(__file__).resolve().parent

archivos_a_leer = [
    base_dir / "data" / "2023" / "bitacora_2023_oct.csv",
    base_dir / "data" / "2023" / "bitacora_2023_nov.csv",
    base_dir / "data" / "2023" / "bitacora_2023_dic.csv"
]

archivo_salida = base_dir / "data" / "2023" / "bitacora_2023_concentrado.csv"

lista_df = [pl.read_csv(archivo) for archivo in archivos_a_leer]


mapa_correcciones = {
    "TIPO DE SALIDA": "TIPO DE SERVICIO",
    "HORA DE TÉRMINO DE SERVICIO": "HORA EN BASE",
    "KM CIERRE": "KM REGRESO"
}

lista_limpia = []


for df in lista_df:
    columnas_a_cambiar = {old: new for old, new in mapa_correcciones.items() if old in df.columns}
    
    if columnas_a_cambiar:
        print(f"Corrigiendo columnas en un archivo: {columnas_a_cambiar}")
        df = df.rename(columnas_a_cambiar)
    
    lista_limpia.append(df)



df_final = pl.concat(lista_limpia, how="vertical")

cantidad_filas = df_final.height
print(f"Cantidad de filas recuperadas: {cantidad_filas}")
print(df_final.head())

df_final.write_csv(archivo_salida)



