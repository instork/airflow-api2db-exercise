import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv('/tmp/.env')

def _get_mongo_client():
    user = os.getenv("MONGODB_USER") 
    pwd = os.getenv("MONGODB_PWD") 
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")
    client = MongoClient(f'mongodb://{user}:{pwd}@{host}:{port}')
    return client


