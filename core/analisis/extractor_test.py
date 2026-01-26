import json
import time
from pathlib import Path
from typing import Optional

import folium
import polars as pl
from colorstreak import Logger as log
from geopy.geocoders import Nominatim
from pydantic import BaseModel

# ==================== CONFIG ====================
geolocator = Nominatim(user_agent="cruz_roja_leon_bot_v2")
mapa_leon = folium.Map(location=[21.1221, -101.6826], zoom_start=12)
DATA_DIR = Path(__file__).resolve().parent / "data"
CACHE_FILE = DATA_DIR / "geocache.json"




class GeoCache:
    def __init__(self, path: Path) -> None:
        self.file = path
        self.memory = {}
        self.load()
        
    
    def load(self) -> None:
        if self.file.exists():
            try:
                self.memory = json.loads(self.file.read_text(encoding="utf-8"))
                log.note(f"💾 Caché cargada: {len(self.memory)} direcciones")
            except json.JSONDecodeError:
                self.memory = {}
                
    def get(self, query: str) -> Optional[tuple[float,float]]:
        
        coordenadas = self.memory.get(query)
        if coordenadas:
            latitud,longitud = coordenadas
            return latitud,longitud
        return None
    
    def save(self, query: str, lat: float, lon: float):
        self.memory[query] = [lat, lon]
        objeto_memoria = json.dumps(self.memory, indent=2, ensure_ascii=False)
        self.file.write_text(objeto_memoria, encoding="utf-8")
            
            


BASURA_KEYWORDS = {"n/p", "s/n", "s/d", "sin dato", "no proporcionado", "no", "x", "."}
cache_coord = GeoCache(CACHE_FILE)

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

    @property
    def es_valida(self) -> bool:
        """Filtro Anti-Basura"""
        calle = (self.calle_principal or "").strip().lower()
        colonia = (self.colonia_o_comunidad or "").strip().lower()

        # 1. Si no hay nada de texto
        if not calle and not colonia:
            return False

        # 2. Si la calle es explícitamente basura (ej: "N/P")
        if calle in BASURA_KEYWORDS:
            return False
            
        # 3. Si la calle es muy corta (ej: "A") suele ser error
        if len(calle) < 2:
            return False

        return True

    def geocodificar(self):

        if not self.es_valida:
            log.warning(f"🗑️ Dirección basura ignorada: {self.calle_principal}")
            return
        
        cached_coords = cache_coord.get(self.query_busqueda)
        if cached_coords:
            self.latitud, self.longitud = cached_coords
            log.info(f"Dirección cacheada: {self.calle_principal} -> {self.latitud}, {self.longitud}")
            return

        try:
            log.debug(f"🔍 Buscando en API: {self.query_busqueda}")
            time.sleep(1)
            
            ubi = geolocator.geocode(self.query_busqueda, timeout=10) # type: ignore
            
            # Busqueda por colonia 
            if not ubi and self.colonia_o_comunidad:
                fallback = f"{self.colonia_o_comunidad}, León, Guanajuato, Mexico"
                
                cached_fallback = cache_coord.get(fallback)
                
                if cached_fallback:
                    self.latitud, self.longitud = cached_fallback
                    ubi = type('obj', (object,), {'latitude': self.latitud, 'longitude': self.longitud})
                else:
                    log.warning(f"⚠️ Intentando busqueda por fallback: {fallback}")
                    time.sleep(1)
                    
                    ubi = geolocator.geocode(fallback, timeout=10) # type: ignore
                    
                    if ubi:
                        cache_coord.save(fallback, ubi.latitude, ubi.longitude) # type : ignore
            
            if ubi:
                self.latitud = ubi.latitude # type: ignore
                self.longitud = ubi.longitude # type: ignore
                log.info(f"✅ API Encontró: {self.calle_principal}")
                
                cache_coord.save(self.query_busqueda, self.latitud, self.longitud) # type : ignore
            else:
                log.warning(f"❌ No se encontraron coordenadas para: {self.calle_principal}")
            
        except Exception as e:
            log.error(f"Error API: {e}")




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



# ==================== Lógica ====================
expedientes_fallidos = []

for expediente in generador_expediente(dataset()):

    expediente.geocodificar()
    
    
    if expediente.latitud and expediente.longitud:
        
        # puntos en el mapa
        añadir_marcador(
            mapa_leon,
            expediente.latitud,
            expediente.longitud,
            f"<b>{expediente.cecom}</b><br>{expediente.direccion_visual}"
        )



# Generar mapa
output_file = DATA_DIR / "mapa_cruz_roja.html"
mapa_leon.save(output_file)
log.note(f"Mapa guardado en: {output_file}")