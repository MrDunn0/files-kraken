from abc import ABC, abstractmethod
from tinydb import TinyDB, Query, where
from tinydb.storages import JSONStorage
from tinydb_serialization import SerializationMiddleware
from tinydb_serialization.serializers import DateTimeSerializer

# I think this serialization shoould be moved in separate file
serialization = SerializationMiddleware(JSONStorage)
serialization.register_serializer(DateTimeSerializer(), 'TinyDate')

# FilesKraken modules

class Database(ABC):
    @abstractmethod
    def add_blueprint(self, blueprint):
        pass

    @abstractmethod
    def get_blueprint(self, name, id):
        pass

    @abstractmethod
    def update_blueprint(self, name, id, updates):
        pass


class JsonDatabse(Database, TinyDB):
    def __init__(self, file, *args, **kwargs):
        super().__init__(file, *args, **kwargs)
        self.blueprints = self.table('blueprints')

    def add_blueprint(self, blueprint):
        self.blueprints.insert(blueprint)

    def get_blueprint(self, name, id):
        query = Query()
        return self.blueprints.search(
            (query.blueprint == name) and (query.id == id))

    def update_blueprint(self, name, id, updates):
        self.blueprints.update(updates, ((where('blueprint') == name) & (where('id') == id)))

    def remove_blueprint(self, name, id):
        query = Query()
        self.blueprints.remove((query.blueprint == name) and (query.id == id))

    def all(self):
        return self.blueprints.all()


class DatabaseManager:
    def __init__(self, db: Database, kraken=None):
        self.db = db
        # self.kraken = kraken

    def add_blueprint(self, entry):
        self.db.add_blueprint(entry)

    def get_blueprint(self, name, id):
        query = self.db.get_blueprint(name, id)
        if query: 
            return query[0]

    def update_blueprint(self, name, id, updates):
        self.db.update_blueprint(name, id, updates)

    def remove_blueprint(self, name, id):
        self.db.remove_blueprint(name, id)

    def get_all(self):
        return self.db.all()

    def listen(self, info):
        pass
        # BlueprintCreatedInfo
        # BlueprintChangedInfo
        # DeleteBlueprintInfo


if __name__ == '__main__':
    from datetime import datetime
    db = JsonDatabse('backups/db.json', storage=serialization)
    db_manager = DatabaseManager(db)
    db_manager.add_blueprint({'blueprint':'SampleInfoBlueprint', 'id': 'other_100__123456'})
    db_manager.update_blueprint('SampleInfoBlueprint', 'other_100__123456', {'aboba': 'cringe', 'updated': datetime.now()})
    # print(db_manager.get_blueprint('SampleInfoBlueprint', 'other_100__123456'))
    # db_manager.remove_blueprint('SampleInfoBlueprint', 'other_120__111111')
    print(db_manager.get_all())
    t1 = db_manager.get_blueprint('SampleInfoBlueprint', 'other_100__123456')
    print(t1)
    print(t1['updated'])
    

