import pathlib
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from json.decoder import JSONDecodeError
from tinydb import TinyDB, Query, where

# FilesKraken modules
from blueprints import SampleInfoBlueprint
from info import *


class Database(ABC):
    @abstractmethod
    def add_blueprint(self, blueprint):
        pass

    @abstractmethod
    def get_blueprint(self, name, id):
        pass

    @abstractmethod
    def update_blueprint(self, name, id, update):
        pass


class JsonDatabse(Database, TinyDB):
    def __init__(self, file):
        super().__init__(file)
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
    db = JsonDatabse('backups/db.json')
    db_manager = DatabaseManager(db)
    # db_manager.update_blueprint('SampleInfoBlueprint', 'other_100__123456', {'aboba': 'cringe'})
    # print(db_manager.get_blueprint('SampleInfoBlueprint', 'other_100__123456'))
    # db_manager.remove_blueprint('SampleInfoBlueprint', 'other_120__111111')
    print(db_manager.get_all())
    
