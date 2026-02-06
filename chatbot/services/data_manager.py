import json
from pathlib import Path



# RUTAS
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "db_simulada.json"


# FUNCIONES INTERNAS
def _cargar_db() -> dict:
    """
    Carga el archivo JSON que simula la base de datos.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError("No se encontró el archivo db_simulada.json")

    with open(DB_PATH, "r", encoding="utf-8") as file:
        return json.load(file)



# API PÚBLICA
def get_paciente_info(id_paciente: str) -> dict | None:
    """
    Devuelve la información de un paciente por su ID.
    Retorna None si el paciente no existe.
    """
    db = _cargar_db()

    for paciente in db.get("pacientes", []):
        if paciente.get("id") == id_paciente:
            return paciente

    return None
