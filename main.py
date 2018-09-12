from os import path, makedirs
import pymongo
from bson.json_util import dumps
import settings

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


backup_db('dumps')