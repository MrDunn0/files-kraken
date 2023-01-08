import pytest
import pathlib
from typing import List

from src.files_kraken.fields import FieldsTransformer, NoUpdate


@pytest.fixture
def str_test_file():
    return pathlib.Path('tests/Tests.str_test_new.txt')


@pytest.fixture
def str_test_match():
    return 'str_test_new'


@pytest.fixture
def str_old_value():
    return 'str_test_old'


@pytest.fixture
def pathlib_new_file():
    return pathlib.Path('test/Tests.pathlib_test_new.txt').absolute()


@pytest.fixture
def pathlib_old_file():
    return pathlib.Path('test/Tests.pathlib_test_old.txt').absolute()


STRLIST_NEW_VALUE_1 = 'new_value_1'
STRLIST_NEW_VALUE_2 = 'new_value_2'
STRLIST_NEW_VALUE_3 = 'new_value_3'
STRLIST_NEW_VALUE_4 = 'new_value_4'

STRLIST_OLD_VALUE_1 = 'old_value_1'
STRLIST_OLD_VALUE_2 = 'old_value_2'
STRLIST_OLD_VALUE_3 = 'old_value_3'

strlist_new_1 = [STRLIST_NEW_VALUE_1, STRLIST_NEW_VALUE_2, STRLIST_NEW_VALUE_3]
strlist_new_2 = [
    STRLIST_NEW_VALUE_1, STRLIST_NEW_VALUE_2,
    STRLIST_NEW_VALUE_3, STRLIST_NEW_VALUE_4]
strlist_new_3 = [STRLIST_OLD_VALUE_1, STRLIST_OLD_VALUE_2]

strlist_old_1 = [STRLIST_OLD_VALUE_1, STRLIST_OLD_VALUE_2, STRLIST_OLD_VALUE_3]
strlist_old_2 = [
    STRLIST_OLD_VALUE_1, STRLIST_OLD_VALUE_2,
    STRLIST_OLD_VALUE_3, STRLIST_NEW_VALUE_1]
strlist_new_2_old_2_created = [
    STRLIST_OLD_VALUE_1, STRLIST_OLD_VALUE_2,
    STRLIST_OLD_VALUE_3, STRLIST_NEW_VALUE_1,
    STRLIST_NEW_VALUE_2, STRLIST_NEW_VALUE_3, STRLIST_NEW_VALUE_4]
strlist_new_3_old_1_deleted = [STRLIST_OLD_VALUE_3]


class TestFieldsTransformer:
    def test_str_after_match(self, str_test_file: str, str_test_match: str):
        assert FieldsTransformer.after_match(
            str, str_test_file, str_test_match) == str_test_match

    def test_str_update(
            self, str_test_file: str, str_test_match: str, str_old_value: str):

        # Created
        with pytest.raises(ValueError):
            FieldsTransformer.update(
                str, str_test_match, str_old_value, mode='created')

        # Deleted
        # Non equal new_value and old_value when old_value is set
        for value in [None, str_test_match]:
            with pytest.raises(ValueError):
                FieldsTransformer.update(
                    str, value, str_old_value, mode='deleted')
        # Non equal new_value and old_value when old value is None
        with pytest.raises(ValueError):
            FieldsTransformer.update(str, str_test_match, None, 'deleted')

        # Equal values when old_value is set
        assert FieldsTransformer.update(
            str, str_old_value, str_old_value, mode='deleted') is None
        # Equal values when both None
        assert FieldsTransformer.update(
            str, None, None, mode='deleted') is None

    def test_str_to_from_db(str_test_match: str):
        for value in [None, str_test_match]:
            assert FieldsTransformer.to_db(str, value) == value
            assert FieldsTransformer.from_db(str, value) == value

    def test_path_after_match(self, pathlib_new_file: pathlib.Path):
        assert FieldsTransformer.after_match(
            pathlib.Path, pathlib_new_file, 'pathlib_test_new') == pathlib_new_file

    def test_path_update(
            self,
            pathlib_new_file: pathlib.Path,
            pathlib_old_file: pathlib.Path):

        # Created
        with pytest.raises(ValueError):
            FieldsTransformer.update(
                pathlib.Path, pathlib_old_file, pathlib_new_file, mode='created')

        # Deleted
        # Non equal new_value and old_value when old value is None
        for value in [None, pathlib_new_file]:
            with pytest.raises(ValueError):
                FieldsTransformer.update(
                    pathlib.Path, pathlib_old_file, value, mode='deleted')
        # Equal values when old_value is set
        assert FieldsTransformer.update(
            pathlib.Path, pathlib_old_file, pathlib_old_file, mode='deleted') is None
        # Equal values when both None
        assert FieldsTransformer.update(
            pathlib.Path, None, None, mode='deleted') is None

    def test_path_to_db(self, pathlib_new_file: pathlib.Path):
        assert FieldsTransformer.to_db(pathlib.Path, None) is None
        assert FieldsTransformer.to_db(
            pathlib.Path, pathlib_new_file) == str(pathlib_new_file.absolute())

    def test_pathlib_from_db(self, pathlib_new_file: pathlib.Path):
        assert FieldsTransformer.from_db(pathlib.Path, None) is None
        assert FieldsTransformer.from_db(
            pathlib.Path, str(pathlib_new_file.absolute())) == pathlib_new_file

    def test_str_list_after_match(self, pathlib_new_file: pathlib.Path):
        assert FieldsTransformer.after_match(
            List[str], pathlib_new_file, STRLIST_NEW_VALUE_1) == [STRLIST_NEW_VALUE_1]

    def test_str_list_update(self):
        # Created
        assert FieldsTransformer.update(
            List[str], strlist_old_1, None, 'created') == NoUpdate
        assert FieldsTransformer.update(
            List[str], None, strlist_new_1, 'created') == strlist_new_1
        assert FieldsTransformer.update(
            List[str], None, None, 'created') == NoUpdate
        assert FieldsTransformer.update(
            List[str], strlist_old_1, strlist_new_1, 'created') == [
                *strlist_old_1, *strlist_new_1]
        # Only unique elements in combined list
        assert FieldsTransformer.update(
            List[str], strlist_old_2,
            strlist_new_2, 'created') == strlist_new_2_old_2_created

        # Deleted
        assert FieldsTransformer.update(
            List[str], strlist_old_1,
            strlist_new_3, 'deleted') == strlist_new_3_old_1_deleted
        assert FieldsTransformer.update(
            List[str], strlist_new_1, strlist_new_1, 'deleted') is None

    def test_str_list_to_db(self):
        assert FieldsTransformer.to_db(
            List[str], strlist_new_1) == strlist_new_1
        assert FieldsTransformer.to_db(List[str], None) is None

    def test_str_list_from_db(self):
        assert FieldsTransformer.from_db(
            List[str], strlist_new_1) == strlist_new_1
        assert FieldsTransformer.from_db(List[str], None) is None
