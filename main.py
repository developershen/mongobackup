from os import path, makedirs, listdir
import pymongo
from bson.json_util import dumps, loads
import settings
import json


def backup_db(backup_db_dir = '.'):
    client = pymongo.MongoClient(host= settings.DATABASE_HOST, port=settings.DATABASE_PORT)
    database = client[settings.DATABASE_NAME]
    if settings.AUTHENTICATE is not False:
        authenticated = database.authenticate(settings.DATABASE_USERNAME,settings.DATABASE_PASSWORD)
        assert authenticated, "Could not authenticate to database!"
    collections = database.collection_names()
    for i, collection_name in enumerate(collections):
        col = getattr(database,collections[i])
        collection = col.find()
        print(collection_name)
        print(collection)
        jsonpath = collection_name + ".json"

        if not path.exists(backup_db_dir):
            makedirs(backup_db_dir)
        jsonpath = path.join(backup_db_dir, jsonpath)
        print (jsonpath)
        with open(jsonpath, 'w') as jsonfile:
            dump = dumps(collection)
            print(dump)
            jsonfile.write(dump)

def add_collections_to_db(backup_db_dir):
    client = pymongo.MongoClient(host= settings.MIGRATE_TO_DATABASE_HOST, port=settings.MIGRATE_TO_DATABASE_PORT)
    print (client)
    database = client[settings.MIGRATE_TO_DATABASE_NAME]

    if settings.MIGRATE_TO_AUTHENTICATE is not False:
        authenticated = database.authenticate(settings.MIGRATE_TO_DATABASE_USERNAME,settings.MIGRATE_TO_DATABASE_PASSWORD)
        assert authenticated, "Could not authenticate to database!"
    collections = database.collection_names()
    for filename in listdir(backup_db_dir):
        if filename.endswith(".json"):
                page = open(path.join(backup_db_dir,filename), 'r')
                print(filename)
                parsed = loads(page.read())
                collection = database[path.splitext(filename)[0]]
                for item in parsed:
                    print(item)
                    collection.insert(item)
        else:
            continue


backup_db('dumps')
add_collections_to_db('dumps')