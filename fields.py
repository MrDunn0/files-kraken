from multiprocessing.sharedctypes import Value
import pathlib

from dataclasses import dataclass
from typing import List, Any, Optional, Pattern, Union

# FilesKraken modules
from parsers import DataParser


class NoUpdate:
    pass

@dataclass
class ParserField:
    name: str
    parser: DataParser
    value: Any = None   # Any, but it must be serializable for chosen DB
    pattern: Pattern = None
    dependent_fields: list = None

    def __post_init__(self):
        if self.pattern and self.dependent_fields:
            raise ValueError(
                'You can set either [pattern] or [dependent_fields] argument, but not both.')
        elif not self.pattern and not self.dependent_fields:
            raise ValueError(
                'One of [pattern] or [dependent_fields] must be specified.'
            )

    def __bool__(self):
        return not self.value is None

    def __eq__(self, other):
        return self.value == other.value

    def parse_value(self, *args, **kwargs):
        self.value = self.parser.parse(*args, **kwargs)

class FieldBehavior:
    field_type = None

    @staticmethod
    def after_match(file: pathlib.Path, match_value: str, **kwargs):
        raise NotImplementedError

    @staticmethod
    def update(old_value: Any, new_value: Any, mode):
        raise NotImplementedError

    @staticmethod
    def to_db(value: Any):
        raise NotImplementedError

    @staticmethod
    def from_db(value):
        raise NotImplementedError


class StrFieldBehavior(FieldBehavior):
    field_type = str

    @staticmethod
    def after_match(file: pathlib.Path, match_value:str, **kwargs):
        return match_value

    @staticmethod
    def update(old_value, new_value, mode):
        if mode == 'created':
            if not new_value or (not old_value and not new_value):
                return NoUpdate
            elif not old_value:
                return new_value
            elif new_value == old_value:
                return NoUpdate
            elif new_value != old_value:
                raise ValueError('Blueprint from DB has a set field value while new value ' +
                                'for that field has been reported by monitoring module.\n' +
                                f'\tOld value: {old_value}\n\tNew value: {new_value}\n\tMode: {mode}')
        elif mode =='deleted':
            if new_value == old_value:
                return None
            else:
                raise ValueError('Deleted file field value is not equal to already set value\n' +
                                f'\tOld value: {old_value}\n\tNew value: {new_value}\n\tMode: {mode}')

    @staticmethod
    def to_db(value: Optional[str]) -> Optional[str]:
        return value

    @staticmethod
    def from_db(value: Optional[str]) -> Optional[str]:
        return value


class PathlibFieldBehavior(StrFieldBehavior):
    field_type = pathlib.Path

    @staticmethod
    def after_match(file: pathlib.Path, match_value: str, **kwargs) -> pathlib.Path.absolute:
        return file.absolute()

    @staticmethod
    def to_db(value: Optional[pathlib.Path]) -> Optional[str]:
        return str(value.absolute()) if value else None

    @staticmethod
    def from_db(value: Optional[str]) -> Optional[pathlib.Path]:
        return pathlib.Path(value) if value else None


class StrListFieldBehavior(FieldBehavior):
    field_type = List[str]

    @staticmethod
    def after_match(file: pathlib.Path, match_value: str, **kwargs):
        return [match_value]

    @staticmethod
    def update(old_value: Any, new_value: Any, mode):
        if mode == 'created':
            if not new_value or (not old_value and not new_value):
                return NoUpdate
            elif not old_value:
                return new_value
            elif new_value == old_value:
                return NoUpdate
            elif new_value != old_value:
                updated_list = [val for val in old_value.copy()]
                updated_list.extend(val for val in new_value if val not in updated_list)
                return updated_list

        elif mode =='deleted':
            if new_value == old_value:
                return None
            else:
                return [el for el in old_value if el not in new_value]

    @staticmethod
    def to_db(value: Optional[List[str]]) -> Optional[List[str]]:
        return value

    @staticmethod
    def from_db(value: Optional[List[str]]):
        return value

class PathlibListFieldBehavior(StrListFieldBehavior):
    field_type = List[pathlib.Path]

    @staticmethod
    def after_match(file: pathlib.Path, match_value: str, **kwargs):
        return [file]

    @staticmethod
    def to_db(value: List[pathlib.Path]) -> Optional[List[str]]:
        return [str(file.absolute()) for file in value] if value else None
    
    @staticmethod
    def from_db(value: Optional[List[str]]) -> Optional[List[pathlib.Path]]:
        return [pathlib.Path(file) for file in value] if value else None

class ParserFieldBehavior(FieldBehavior):
    field_type = ParserField

    @staticmethod
    def after_match(file: pathlib.Path, match_value: str, **kwargs):
        # Parser field could be matched only if it has set pattern
        # It means there is no dependent fields and parser requires only matched file
        pf = kwargs.get('field_default')
        return pf.parser.parse(file)

    @staticmethod
    def update(old_value: Any, new_value: Any, mode):
        if mode == 'created':
            if not new_value or (not old_value and not new_value):
                return NoUpdate
            elif not old_value:
                return new_value
            elif new_value == old_value:
                return NoUpdate
            elif new_value != old_value:
                name = new_value.name
                print(f'WARNING: new value in ParserField {name} while old value is set' +
                    f'\tOld value: {old_value}\n\tNew value: {new_value}\n\tMode: {mode}')
                return new_value

        elif mode =='deleted':
            # I don't want to change parsed values when file is deleted
            if new_value == old_value:
                return NoUpdate
            else:
                return print('Comparison of nonequal ParsedField values is not implemented')

    @staticmethod
    def to_db(value: Any):
        return value

    @staticmethod
    def from_db(value: Optional[Any]) -> Optional[Any]:
        return value


def get_all_subclasses(cls):
    all_subclasses = []

    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))

    return all_subclasses


class FieldsTransformer:

    type_behavior_mapping = {
        cls.field_type: cls
        for cls in get_all_subclasses(FieldBehavior)
    }

    @staticmethod
    def after_match(field_type,
                    file: pathlib.Path,
                    match_value: str, **kwargs) -> Optional[Union[pathlib.Path, str]]:
        field_behavior = FieldsTransformer.type_behavior_mapping[field_type]
        return field_behavior.after_match(file, match_value, **kwargs)

    @staticmethod
    def update(field_type, old_value: Any, new_value: Any, mode) -> Any:
        field_behavior = FieldsTransformer.type_behavior_mapping[field_type]
        return field_behavior.update(old_value, new_value, mode)

    @staticmethod
    def to_db(field_type, value: Any) -> Any:
        field_behavior = FieldsTransformer.type_behavior_mapping[field_type]
        return field_behavior.to_db(value)

    @staticmethod
    def from_db(field_type, value: Any) -> Any:
        field_behavior = FieldsTransformer.type_behavior_mapping[field_type]
        return field_behavior.from_db(value)




if __name__ == '__main__':
    ft = FieldsTransformer

    # TESTING STR
    str_test_file = pathlib.Path('tests/Tests.str_test_new.txt')
    str_test_match = 'str_test_new'
    str_test_old_value = 'str_test_old'
    
    # 1. After match
    assert FieldsTransformer.after_match(str, str_test_file, str_test_match) == str_test_match
    # 2. Updates
    # 2.1 Created
    try:
        FieldsTransformer.update(str, str_test_match, str_test_old_value, mode='created')
    except Exception as e:
        assert isinstance(e, ValueError)
    else:
        raise AssertionError('No ValueError raised in str update test')
    
    # 2.2 Deleted
    # Non equal new_value and old_value when old_value is set
    for value in [None, str_test_match]:   
        try:
            FieldsTransformer.update(str, value, str_test_old_value, mode='deleted')
        except Exception as e:
            assert isinstance(e, ValueError)
        else:
            raise AssertionError('No ValueError raised in str update test')

    # Non equal new_value and old_value when old value is None
    try:
        FieldsTransformer.update(str, str_test_match, None, 'deleted')
    except Exception as e:
        assert isinstance(e, ValueError)
    else:
        raise AssertionError('No ValueError raised in str update test')

    # Equal values when old_value is set
    assert FieldsTransformer.update(str, str_test_old_value, str_test_old_value, mode='deleted') is None
    # Equal values when both None
    assert FieldsTransformer.update(str, None, None, mode='deleted') is None

    # 3. To DB 
    # 4. From DB
    for value in [None, str_test_match]:
        assert FieldsTransformer.to_db(str, value) == value
        assert FieldsTransformer.from_db(str, value) == value


    # TESTING pathlib.Path
    pathlib_new_file = pathlib.Path('test/Tests.pathlib_test_new.txt').absolute()
    pathlib_old_file = pathlib.Path('test/Tests.pathlib_test_old.txt').absolute()
    # 1. After match
    assert FieldsTransformer.after_match(pathlib.Path, pathlib_new_file, 'pathlib_test_new') == pathlib_new_file
    # 2. Update
    # 2.1 Created
    try:
        FieldsTransformer.update(pathlib.Path,pathlib_old_file ,pathlib_new_file , mode='created')
    except Exception as e:
        assert isinstance(e, ValueError)
    else:
        raise AssertionError('No ValueError raised in pathlb.Path update test')
    
    # 2.2 Deleted
    # Non equal new_value and old_value when old_value is set
    for value in [None, pathlib_new_file]:
        try:
            FieldsTransformer.update(pathlib.Path, pathlib_old_file, value, mode='deleted')
        except Exception as e:
            assert isinstance(e, ValueError)
        else:
            raise AssertionError('No ValueError raised in pathlib.Path update test')

    # Non equal new_value and old_value when old value is None
    try:
        FieldsTransformer.update(pathlib.Path, None, pathlib_new_file, 'deleted')
    except Exception as e:
        assert isinstance(e, ValueError)
    else:
        raise AssertionError('No ValueError raised in str update test')

    # Equal values when old_value is set
    assert FieldsTransformer.update(pathlib.Path, pathlib_old_file, pathlib_old_file, mode='deleted') is None
    # Equal values when both None
    assert FieldsTransformer.update(pathlib.Path, None, None, mode='deleted') is None


    # 3. To DB
    assert FieldsTransformer.to_db(pathlib.Path, None) is None
    assert FieldsTransformer.to_db(pathlib.Path, pathlib_new_file) == str(pathlib_new_file.absolute())
    
    # 4. From DB
    assert FieldsTransformer.from_db(pathlib.Path, None) is None
    assert FieldsTransformer.from_db(pathlib.Path, str(pathlib_new_file.absolute())) == pathlib_new_file


    # StrList TESTING
    STRLIST_NEW_VALUE_1 = 'new_value_1'
    STRLIST_NEW_VALUE_2 = 'new_value_2'
    STRLIST_NEW_VALUE_3 = 'new_value_3'
    STRLIST_NEW_VALUE_4 = 'new_value_4'

    STRLIST_OLD_VALUE_1 = 'old_value_1'
    STRLIST_OLD_VALUE_2 = 'old_value_2'
    STRLIST_OLD_VALUE_3 = 'old_value_3'

    strlist_new_1 = [STRLIST_NEW_VALUE_1, STRLIST_NEW_VALUE_2, STRLIST_NEW_VALUE_3]
    strlist_new_2 = [STRLIST_NEW_VALUE_1, STRLIST_NEW_VALUE_2, STRLIST_NEW_VALUE_3, STRLIST_NEW_VALUE_4]
    strlist_new_3 = [STRLIST_OLD_VALUE_1, STRLIST_OLD_VALUE_2]

    strlist_old_1 = [STRLIST_OLD_VALUE_1, STRLIST_OLD_VALUE_2, STRLIST_OLD_VALUE_3]
    strlist_old_2 = [STRLIST_OLD_VALUE_1, STRLIST_OLD_VALUE_2, STRLIST_OLD_VALUE_3, STRLIST_NEW_VALUE_1]
    strlist_new_2_old_2_created = [STRLIST_OLD_VALUE_1, STRLIST_OLD_VALUE_2, STRLIST_OLD_VALUE_3, STRLIST_NEW_VALUE_1,
                                    STRLIST_NEW_VALUE_2, STRLIST_NEW_VALUE_3, STRLIST_NEW_VALUE_4]
    strlist_new_3_old_1_deleted = [STRLIST_OLD_VALUE_3]
    # 1. After Match - match is always single str
    assert FieldsTransformer.after_match(List[str], pathlib_new_file, STRLIST_NEW_VALUE_1) == STRLIST_NEW_VALUE_1
    # 2. Update
    # 2.1 Created
    assert FieldsTransformer.update(List[str], strlist_old_1, None, 'created') == strlist_old_1
    assert FieldsTransformer.update(List[str], None, strlist_new_1, 'created') == strlist_new_1
    assert FieldsTransformer.update(List[str], None, None, 'created') is None
    assert FieldsTransformer.update(List[str], strlist_old_1, strlist_new_1, 'created') == [*strlist_old_1, *strlist_new_1]
    # Only unique elements in combined list
    assert FieldsTransformer.update(List[str], strlist_old_2, strlist_new_2, 'created') == strlist_new_2_old_2_created
    # 2.2 Deleted
    assert FieldsTransformer.update(List[str], strlist_old_1, strlist_new_3, 'deleted') == strlist_new_3_old_1_deleted
    assert FieldsTransformer.update(List[str], strlist_new_1, strlist_new_1, 'deleted') is None

    # 3. To DB
    assert FieldsTransformer.to_db(List[str], strlist_new_1) == strlist_new_1
    assert FieldsTransformer.to_db(List[str], None) is None
    # 4. From DB
    assert FieldsTransformer.from_db(List[str], strlist_new_1) == strlist_new_1
    assert FieldsTransformer.from_db(List[str], None) is None


    # PatlibList TESTING
    # 