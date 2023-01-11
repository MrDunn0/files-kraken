import pathlib
from dataclasses import dataclass, fields
from collections import namedtuple

from typing import Dict, NamedTuple, Any

# FilesKraken modules
from blueprint import DataBlueprint
from fields import FieldsTransformer, NoUpdate
from retools import SchemeMatcher
from krakens_nest import Kraken
from database import DatabaseManager
from info import FileChangesInfo


'''
    Here is a problem with autodetection of data format. Now
there is only one format of namedtuple with .deleted and .created
fields. But with each modification in this format we will need to
rewrite all blueprint stuff.
    Another problem related to the previous one conserns an iterator
of changes, which I want to be something like adapter, which
transforms different changes data formats into uniform signal
to builder or organizer. The problem can be seen in the example of
deleted and created changes format. Iterator gets this namedtuple, but
it can't just yield filenames without description. The information about
deletion or creation of file is key-concept of the app.
'''


class BlueprintsDBUpdater:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def update(self, builder_data) -> None:
        for bp, structures in builder_data.items():
            for id, info in structures.items():
                structure_info = info.structure_info
                if structure_info.is_new:
                    entry = self.entry_from_structure(structure_info.structure, id)
                    self.db_manager.add_blueprint(entry)
                else:
                    if info.updates:
                        formatted_updates = self.old_structure_updates_to_db(
                            structure_info.structure, info.updates
                        )
                        self.db_manager.update_blueprint(bp.name, id, formatted_updates)

    @staticmethod
    def entry_from_structure(structure: DataBlueprint, id) -> Dict[str, Any]:
        entry = {'blueprint': structure.name, 'id': id}
        for field in fields(structure):
            value = getattr(structure, field.name)
            if field.type.__name__ == 'ParserField':
                value = value.value
            entry[field.name] = FieldsTransformer.to_db(field.type, value)
        return entry

    @staticmethod
    def old_structure_updates_to_db(structure: DataBlueprint, updates: dict):
        '''Formats old structure updates for db'''
        formatted_updates = {}
        for field, value in updates.items():
            field_type = structure.get_field_type(field)
            formatted_updates[field] = FieldsTransformer.to_db(
                field_type, value)
        return formatted_updates


@dataclass
class BlueprintInfo:
    blueprint: DataBlueprint

    def __post_init__(self):
        self.scheme_matcher = SchemeMatcher(self.blueprint.required_fields)


@dataclass
class StructureInfo:
    structure: DataBlueprint
    is_new: bool = True
    scheme_matcher = None

    def __post_init__(self):
        if hasattr(self.structure, 'match_scheme'):
            self.scheme_matcher = SchemeMatcher(self.structure.match_scheme)


class BlueprintBuilder:
    _StructureIdInfo = namedtuple('StructureIdInfo', 'structure_info updates')

    def __init__(
            self,
            db_manager: DatabaseManager,
            db_updater: BlueprintsDBUpdater,
            kraken: Kraken = None,
            blueprints: list[DataBlueprint] | None = None):
        self.db_manager = db_manager
        self.db_updater = db_updater
        self.kraken = kraken
        self.blueprints = {bp: BlueprintInfo(bp) for bp in blueprints} if blueprints else {}
        self.structures = {bp: {} for bp in self.blueprints}
        if self.kraken:
            self.kraken.events.append(self.listen)

    # All methods hardcoded for dict_collection ChangesFactory format

    def listen(self, info):
        if isinstance(info, FileChangesInfo):
            print(f'Data organizer has recieved changes: {info.changes}')
            self.build(info.changes)

    def set_kraken(self, kraken: Kraken):
        '''Allows to set Kraken when object is already instantiated'''
        self.kraken = kraken
        self.kraken.events.append(self.listen)

    def register_blueprint(self, blueprint: DataBlueprint):
        self.blueprints[blueprint] = BlueprintInfo(blueprint)
        self.structures[blueprint] = {}

    def build(self, data: NamedTuple):
        for file in data.created:
            self._process_file(file, 'created')
        for file in data.deleted:
            self._process_file(file, 'deleted')

        self.update_parser_fields()
        self.db_updater.update(self.structures)
        # Delete all builded structures
        self.clear_structures()

    # It will be good to write matched blueprints iterator
    def _process_file(self, file: pathlib.Path, mode: str) -> None:
        print(f'BlueprintBuilder processing file {file}')
        file = pathlib.Path(file)
        for bp, bp_info in self.blueprints.items():
            structures = self.structures[bp]
            # At this step scheme_matcher matches only required fields as set in BlueprintInfo
            # Required fields must be of type str
            match = bp_info.scheme_matcher.match(file.name)

            # if match:
            #     print(f'Matched fields: {match}')
            # else:
            #     print('No matched fields')

            if len(match) == len(bp.required_fields):  # All required fields found
                structure_id = '__'.join(match.values())  # Required fields combination
                id_info = structures.get(structure_id)
                structure_info = id_info.structure_info if id_info else None
                if not structure_info:
                    # Check if there is a structure with the same ID in DB
                    db_entry = self.db_manager.get_blueprint(bp.name, structure_id)
                    if db_entry:
                        structure_info = StructureInfo(bp.create(**db_entry), is_new=False)
                        # print('Structure from DB', structure_info)
                    else:
                        # Here we initialize an instance of bp with required args
                        # and further name it a structure. It's not necessary to format
                        # required fields after match because they can only be of str type
                        # and matcher always returns str
                        structure_info = StructureInfo(bp.create(**match))
                        # print('New structure', structure_info)
                structures[structure_id] = self._StructureIdInfo(structure_info, {})
                # We need to check also optional fields on current file
                # But there could be blueprint without optional fields
                if structure_info.scheme_matcher:
                    optional_match = structure_info.scheme_matcher.match(file.name)
                    if optional_match:
                        # After match formatting
                        formatted_fields = self.format_fields(
                            structure_info.structure,
                            file,
                            optional_match)
                        # Compare current field values with new ones
                        updates = self.get_updates(
                            structure_info.structure, formatted_fields, mode)
                        if not structure_info.is_new:
                            # Set updates for current structure_id
                            self.set_updates(bp, structure_id, updates)
                        # Set updated field values for current structure
                        self.set_fields(structure_info.structure, updates)

    @staticmethod
    def format_fields(structure: DataBlueprint, file: pathlib.Path, match: Dict[str, str],) -> None:
        '''
        Formats structure field values based on matched values and type annotations
        using FieldsTransformer
        '''
        field_types = {f: structure.get_field_type(f) for f in match}
        formatted_fields = {}
        for field, matched_value in match.items():
            f_type = field_types[field]
            field_default = getattr(structure, field)
            # ParserField parsers now run in any mode, and it's bad
            formatted_fields[field] = FieldsTransformer.after_match(f_type, file,
                                                                    matched_value,
                                                                    field_default=field_default)
        return formatted_fields

    @staticmethod
    def get_updates(structure: DataBlueprint, matched_fields, mode):
        updates = {}
        for field, new_value in matched_fields.items():
            f_type = structure.get_field_type(field)
            old_value = getattr(structure, field)
            update = FieldsTransformer.update(f_type, old_value, new_value, mode)
            if not update == NoUpdate:
                updates[field] = update
        return updates

    def set_updates(self, bp: DataBlueprint, id: str, updates: dict):
        self.structures[bp][id].updates.update(updates)

    @staticmethod
    def set_fields(structure: DataBlueprint, fields: dict) -> None:
        for field, new_value in fields.items():
            setattr(structure, field, new_value)

    def update_parser_fields(self) -> None:
        '''Updates ParserFields with dependent fields in all structures'''
        for bp, structures in self.structures.items():
            for structure_id, info in structures.items():
                structure = info.structure_info.structure
                updates = {}
                parser_fields = [
                    getattr(structure, field.name)
                    for field in fields(structure)
                    if structure.get_field_type(field.name).__name__ == 'ParserField']
                # Select only pf with dependent_fields and without value set
                parser_fields = [
                    pf for pf in parser_fields
                    if pf.dependent_fields and not pf.value]
                for pf in parser_fields:
                    if structure.fields_are_set(*pf.dependent_fields):
                        args = [getattr(structure, f) for f in pf.dependent_fields]
                        # Parsed values are auto set to pf value attr
                        pf.parse_value(*args)
                        updates[pf.name] = pf.value
                #  This pf processing is the worst place of the module
                self.set_updates(bp, structure_id, updates)

    def clear_structures(self):
        self.structures = {bp: {} for bp in self.blueprints}


__all__ = [
    'BlueprintsDBUpdater',
    'BlueprintInfo',
    'StructureInfo',
    'BlueprintBuilder'
]
