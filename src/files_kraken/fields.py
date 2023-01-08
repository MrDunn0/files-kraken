import pathlib
from dataclasses import dataclass
from typing import List, Any, Optional, Pattern, Union

# FilesKraken modules
from functions import get_all_subclasses


class DataParser:
    @staticmethod
    def parse(*args, **kwargs):
        pass


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
        return self.value is not None

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
    def after_match(file: pathlib.Path, match_value: str, **kwargs):
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
                raise ValueError(
                    'Blueprint from DB has a set field value while new value ' +
                    'for that field has been reported by monitoring module.\n' +
                    f'\tOld value: {old_value}\n\tNew value: {new_value}\n\tMode: {mode}')
        elif mode == 'deleted':
            if new_value == old_value:
                return None
            else:
                raise ValueError(
                    'Deleted file field value is not equal to already set value\n' +
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
                return new_value  # New value is not empty
            elif new_value == old_value:
                return NoUpdate
            elif new_value != old_value:
                updated_list = [val for val in old_value.copy()]
                updated_list.extend(val for val in new_value if val not in updated_list)
                return updated_list

        elif mode == 'deleted':
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
    field_type = ['ParserField', ParserField]

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
                print(
                    f'WARNING: new value in ParserField {name} while old value is set' +
                    f'\tOld value: {old_value}\n\tNew value: {new_value}\n\tMode: {mode}')
                return new_value

        elif mode == 'deleted':
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


class FieldsTransformer:

    type_behavior_mapping = {}
    for cls in get_all_subclasses(FieldBehavior):
        if isinstance(cls.field_type, list):
            for field_type in cls.field_type:
                type_behavior_mapping[field_type] = cls
        else:
            type_behavior_mapping[cls.field_type] = cls

    @staticmethod
    def after_match(field_type,
                    file: pathlib.Path,
                    match_value: str, **kwargs) -> Optional[Union[pathlib.Path, str]]:
        try:
            field_behavior = FieldsTransformer.type_behavior_mapping[field_type]
        except KeyError:
            field_behavior = FieldsTransformer.type_behavior_mapping[field_type.__name__]
        return field_behavior.after_match(file, match_value, **kwargs)

    @staticmethod
    def update(field_type, old_value: Any, new_value: Any, mode) -> Any:
        try:
            field_behavior = FieldsTransformer.type_behavior_mapping[field_type]
        except KeyError:
            field_behavior = FieldsTransformer.type_behavior_mapping[field_type.__name__]
        return field_behavior.update(old_value, new_value, mode)

    @staticmethod
    def to_db(field_type, value: Any) -> Any:
        try:
            field_behavior = FieldsTransformer.type_behavior_mapping[field_type]
        except KeyError:
            field_behavior = FieldsTransformer.type_behavior_mapping[field_type.__name__]
        return field_behavior.to_db(value)

    @staticmethod
    def from_db(field_type, value: Any) -> Any:
        try:
            field_behavior = FieldsTransformer.type_behavior_mapping[field_type]
        except KeyError:
            field_behavior = FieldsTransformer.type_behavior_mapping[field_type.__name__]
        return field_behavior.from_db(value)
