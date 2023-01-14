from dataclasses import dataclass
from typing import ClassVar
from copy import deepcopy

# FilesKraken modules
from fields import FieldsTransformer
# It's good idea to create dataclass with constants for blueprints to use it here


@dataclass
class DataBlueprint:
    required_fields: ClassVar

    def __post_init__(self):
        '''
        This should be executed after match_scheme declaration in child classes.
        Here patterns are set to match_scheme for each ParserField.
        '''
        self.has_parser_fields = False
        for field, value in self.__dict__.items():
            # isintance(value, ParserField) doesn't work when running
            # in real project. It's because of imports problem
            # This possibly can be fixed with package sys.path modification
            # and changes in module imports to package imports. Now Idk how to realize it
            if value.__class__.__name__ == 'ParserField':
                # Copy ParserField in each instance.
                # I don't know another way to solve the problem, when all instances have
                # the same id of this field. Create method also have deepcopy but it works only
                # when there is value for it in DB, otherwise object will be created
                # with default class vlaue.
                setattr(self, field, deepcopy(value))
                pf = getattr(self, field)
                # There could be set either pattern or dependent_fields in a ParserField, not both
                if pf.pattern:
                    # Each child must have match_scheme declared before super().__post_init__
                    # It's very bad and need to be fixed
                    self.match_scheme[field] = pf.pattern
                # At first I added all patterns for dependent fields to match_scheme,
                # but then I decided to check each dependent field in data_organizer, because of
                # iterative processing of single file, which may not allow
                # to match several fields at once.
                if not self.has_parser_fields:
                    self.has_parser_fields = True

    @classmethod
    def create(cls, **kwargs):
        '''
        Factory method for all DataBlueprint subclasses
        '''
        annotations = cls.__annotations__
        required_fields = cls.required_fields.keys()
        required_args = [kwargs[arg] for arg in required_fields]
        # It realy needs some sort of specification...
        optional_args = {}
        for field, value in kwargs.items():
            if (field not in required_fields) and (field in annotations):
                f_type = annotations[field]
                # Again I need to process ParserField separately...
                if f_type.__name__ == 'ParserField':
                    parser_field = deepcopy(getattr(cls, field))
                    # I'm not sure it's okay to change class ParserField like that
                    parser_field.value = value
                    optional_args[field] = parser_field
                    continue
                optional_args[field] = FieldsTransformer.from_db(f_type, value)
        return cls(*required_args, **optional_args)

    @classmethod
    def get_field_type(cls, field: str):
        return cls.__annotations__[field]

    def fields_are_set(self, *fields):
        return all(getattr(self, f) for f in fields if hasattr(self, f))

    @classmethod
    @property
    def name(cls):
        return cls.__name__


__all__ = ['DataBlueprint']
