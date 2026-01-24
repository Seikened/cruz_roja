import polars as pl
from pathlib import Path
from colorstreak import Logger as log
from pydantic import BaseModel
from typing import Optional
import folium
from geopy.geocoders import Nominatim

# ==================== CONFIG ====================
geolocator = Nominatim(user_agent="cruz_roja_leon_bot_v2")



def dataset() -> pl.DataFrame:
    DATA_DIR = Path(__file__).resolve().parent / "data"
    file = DATA_DIR / "dataset_feb_cruz_roja.csv"
    
    return (
        pl.scan_csv(file)
        .drop_nulls(subset=["CECOM"])
        .drop(["OBSERVACIONES"])
        .select(pl.all().name.to_lowercase().name.replace(" ", "_"))
        .collect()
    )

class Expediente(BaseModel):
    cecom: str
    calle_principal: Optional[str] = None
    colonia_o_comunidad: Optional[str] = None
    cruce: Optional[str] = None
    
    # Coordenadas
    latitud: Optional[float] = None
    longitud: Optional[float] = None

    @property
    def direccion_visual(self) -> str:
        c = self.calle_principal or "S/N"
        col = self.colonia_o_comunidad or "S/C"
        return f"🛣️ {c}<br>🏘️ {col}"

    @property
    def query_busqueda(self) -> str:
        calle = (self.calle_principal or "").strip()
        colonia = (self.colonia_o_comunidad or "").strip()
        
        return f"{calle}, {colonia}, León, Guanajuato, Mexico"

    def geocodificar(self):

        if not self.calle_principal:
            return

        try:
            log.debug(f"🔍 Buscando: '{self.query_busqueda}'") 
            
            location = geolocator.geocode(self.query_busqueda, timeout=10)
            
            if location:
                self.latitud = location.latitude
                self.longitud = location.longitude
                log.info(f"✅ Encontrado: {self.calle_principal}")
            else:

                fallback = f"{self.colonia_o_comunidad}, León, Guanajuato, Mexico"
                log.warning(f"⚠️ Calle no exacta. Intentando solo colonia: {fallback}")
                
                location = geolocator.geocode(fallback, timeout=10)
                if location:
                    self.latitud = location.latitude
                    self.longitud = location.longitude
                    log.info(f"📍 Aprox por Colonia: {self.colonia_o_comunidad}")

        except Exception as e:
            log.error(f"Error API: {e}")

def generador_expediente(df: pl.DataFrame):
    for row in df.iter_rows(named=True):
        yield Expediente(**row)



# ==================== Ejecución ====================
mapa = folium.Map(location=[21.1221, -101.6826], zoom_start=12)
log.info("Iniciando geocodificación...")


for expediente in generador_expediente(dataset()):
    expediente.geocodificar()
    
    if expediente.latitud and expediente.longitud:
        
        # puntos en el mapa
        folium.Marker(
            [expediente.latitud, expediente.longitud],
            popup=f"<b>{expediente.cecom}</b><br>{expediente.direccion_visual}",
            icon=folium.Icon(color="red", icon="plus")
        ).add_to(mapa)



# Generar mapa
output_file = "mapa_cruz_roja_v2.html"
mapa.save(output_file)
log.note(f"Mapa guardado en: {output_file}")