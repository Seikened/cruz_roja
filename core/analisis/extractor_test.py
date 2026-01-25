import polars as pl
from pathlib import Path
from colorstreak import Logger as log
from pydantic import BaseModel
from typing import Optional
import folium
from geopy.geocoders import Nominatim

# ==================== CONFIG ====================
geolocator = Nominatim(user_agent="cruz_roja_leon_bot_v2")
mapa_leon = folium.Map(location=[21.1221, -101.6826], zoom_start=12)

DATA_DIR = Path(__file__).resolve().parent / "data"





    
    


class Expediente(BaseModel):
    cecom: str # esto es: Centro de Comando
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
        return f" {c}<br> {col}"

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
            
            location = geolocator.geocode(self.query_busqueda, timeout=10) #type: ignore
            
            if location:
                self.latitud = location.latitude #type: ignore
                self.longitud = location.longitude #type: ignore
                log.info(f"✅ Encontrado: {self.calle_principal}")
            else:

                fallback = f"{self.colonia_o_comunidad}, León, Guanajuato, Mexico"
                log.warning(f"⚠️ Calle no exacta. Intentando solo colonia: {fallback}")
                
                location = geolocator.geocode(fallback, timeout=10) #type: ignore
                if location:
                    self.latitud = location.latitude #type: ignore
                    self.longitud = location.longitude #type: ignore
                    log.info(f"📍 Aprox por Colonia: {self.colonia_o_comunidad}")

        except Exception as e:
            log.error(f"Error API: {e}")


class GeoCache:
    cache_file = DATA_DIR / "cache_adress.json"
    
    @classmethod
    def ensure_exist(cls) -> None:
        cls.cache_file.parent.mkdir(parents=True, exist_ok=True)
        cls.cache_file.touch(exist_ok=True)
    
    def make_cache(self, expediente: Expediente):
        self.ensure_exist()
        payload = {
        "calle" : expediente.calle_principal,
        "colonia" : expediente.colonia_o_comunidad,
        "latitud" : expediente.latitud,
        "longitud" : expediente.longitud
        }
        
        with open(self.cache_file, "a", encoding="utf-8") as f:
            f.write(f"{payload}\n")
        
    @staticmethod
    def aready_cached(expediente: Expediente) -> bool:
        GeoCache.ensure_exist()
        if GeoCache.cache_file.stat().st_size == 0:
            return False
        with open(GeoCache.cache_file, "r", encoding="utf-8") as f:
            #aun no se 
            pass
        return True





# ==================== Fnciones ====================

def dataset() -> pl.DataFrame:
    file = DATA_DIR / "dataset_feb_cruz_roja.csv"
    return (
        pl.scan_csv(file)
        .drop_nulls(subset=["CECOM"])
        .drop(["OBSERVACIONES"])
        .select(pl.all().name.to_lowercase().name.replace(" ", "_"))
        .collect()
    )

def generador_expediente(df: pl.DataFrame):
    global expedientes_fallidos
    for row in df.iter_rows(named=True):
        try:
            yield Expediente.model_validate(row)
        except Exception as e:
            log.error(f"Expediente no se pudo cargar error: {e}")
            expedientes_fallidos.append(row)

def añadir_marcador(mapa, lat, lon, popup_html):
    folium.Marker(
        [lat, lon],
        popup=popup_html,
        icon=folium.Icon(color="red", icon="plus")
    ).add_to(mapa)






expedientes_fallidos = []
c = 0
for expediente in generador_expediente(dataset()):
    if c >= 10:
        break
    expediente.geocodificar()
    
    
    if expediente.latitud and expediente.longitud:
        
        # puntos en el mapa
        añadir_marcador(
            mapa_leon,
            expediente.latitud,
            expediente.longitud,
            f"<b>{expediente.cecom}</b><br>{expediente.direccion_visual}"
        )
    c += 1 


# Generar mapa
output_file = DATA_DIR / "mapa_cruz_roja.html"
mapa_leon.save(output_file)
log.note(f"Mapa guardado en: {output_file}")