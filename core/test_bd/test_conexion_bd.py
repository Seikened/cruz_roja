#Aqui llevaremos la base de datos creada con mongodb
from pymongo import MongoClient
import json
from colorstreak import Logger as log
from bson import json_util
url = "mongodb://mongo:zpwk4iayanz4yryl@89.116.212.100:27020"

cliente = MongoClient(url)

#Probamos conexion
db = cliente["test_bd"]
#print(db.list_collection_names())

coleccion = db["usuarios"]
#Esto es una prueba

doc = {
    "nombre": "Josué",
    "edad": 24,
    "rol": "admin"
}
coleccion.insert_one(doc)

cursor = coleccion.find()
response = json.dumps(list(cursor), indent=4, default=json_util.default)

log.debug(response)
