from config import settings
from pymongo import MongoClient

client = MongoClient(settings.MONGODB_URL)
# test the connection
db = client["test_bd"]
print(db.list_collection_names())