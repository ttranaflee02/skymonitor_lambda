from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class MongoHelper:
    def __init__(self, mongo_uri: str, db_name: str):
        try:
            self.client = MongoClient(mongo_uri)
            self.db = self.client[db_name]
            self.db.command('ping')
        except ConnectionFailure as e:
            raise Exception(f"MongoDB connection failed: {e}")
    
    def get_collection(self, collection_name: str):
        return self.db[collection_name]
    
    def create_index(self, collection_name: str, keys: list):
        collection = self.db[collection_name]
        collection.create_index(keys)