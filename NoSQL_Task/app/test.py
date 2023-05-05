import pymongo

client = pymongo.MongoClient("mongodb://root:rootpassword@localhost:27017/")

db = client["mydatabase"]

collection = db["mycollection"]

data = {"name": "John", "age": 30}
collection.insert_one(data)
