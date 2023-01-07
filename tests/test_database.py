import pytest
import pyfakefs
import os
from src.database import JsonDatabse, DatabaseManager, serialization

@pytest.fixture(scope='class')
def db(fs_module):
    os.mkdir('/fs')
    return JsonDatabse('/fs/db.json')

@pytest.fixture()
def blueprint():
    bp = {
        'blueprint': 'TestBlueprint',
        'value': 77,
        'description': 'A blueprint for tests',
        'coverage': 55.5,
        'id': 'test_blueprint'
        }
    return bp


class TestJsonDatabase:
    def test_add_blueprint(self, db: JsonDatabse, blueprint: dict[str, int | str | float]):
        db.add_blueprint(blueprint)

    def test_get_blueprint(self, db: JsonDatabse, blueprint: dict[str, int | str | float]):
        assert db.get_blueprint(name='TestBlueprint', id='test_blueprint')[0] == blueprint

    def test_all(self, db: JsonDatabse, blueprint: dict[str, int | str | float]):
        second_bp = {
        'blueprint': 'TestBlueprint',
        'value': 99,
        'description': 'Second blueprint for tests',
        'coverage': 2,
        'id': 'second_blueprint'
        }
        db.add_blueprint(second_bp)
        all = db.all()
        assert all == [blueprint, second_bp]

    def test_update_blueprint(self, db: JsonDatabse, blueprint: dict):
        updates = {'description': 'Blueprint has been updated'}
        db.update_blueprint('TestBlueprint', 'test_blueprint', updates)
        bp = db.get_blueprint(name='TestBlueprint', id='test_blueprint')[0]
        assert bp['description'] == 'Blueprint has been updated'

    def test_remove_blueprint(self, db: JsonDatabse, blueprint: dict):
        assert db.get_blueprint(name='TestBlueprint', id='test_blueprint')
        db.remove_blueprint(name='TestBlueprint', id='test_blueprint')
        assert db.get_blueprint(name='TestBlueprint', id='test_blueprint') == []

