import pathlib
from dataclasses import dataclass, fields
from collections import namedtuple

from typing import List, Dict, NamedTuple, Any
from webbrowser import get

from tinydb import Storage

# FilesKraken modules
from blueprint import DataBlueprint
from fields import FieldsTransformer, ParserField, NoUpdate
from retools import SchemeMatcher
from krakens_nest import Kraken
from database import DatabaseManager, JsonDatabse, serialization
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
                        self.db_manager.update_blueprint(bp.name, id, info.updates)

    @staticmethod
    def entry_from_structure(structure: DataBlueprint, id) -> Dict[str, Any]:
        entry = {'blueprint': structure.name, 'id': id}
        for field in fields(structure):
            value = getattr(structure, field.name)
            if field.type == ParserField:
                value = value.value
            entry[field.name] = FieldsTransformer.to_db(field.type, value)
        return entry


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
    def __init__(self, kraken: Kraken,
                    db_manager: DatabaseManager,
                    db_updater: BlueprintsDBUpdater):
        self.kraken = kraken
        self.db_manager = db_manager
        self.db_updater = db_updater
        self.blueprints = {bp: BlueprintInfo(bp) for bp in DataBlueprint.__subclasses__()}
        self.structures = {bp: {} for bp in self.blueprints}

        self.kraken.events.append(self.listen)

    def listen(self, info):
        if isinstance(info, FileChangesInfo):
            print(f'Data organizer has recieved changes: {info.changes}')
            self.build(info.changes)

    # All methods hardcoded for dict_collection ChangesFactory format

    def build(self, data: NamedTuple):
        for file in data.created:
            self._process_file(file, 'created')
        for file in data.deleted:
            self._process_file(file,'deleted')

        self.update_parser_fields()

        self.db_updater.update(self.structures)
        # Delete all builded structures
        self.clear_structures()

    # It will be good to write matched blueprints iterator
    def _process_file(self, file: pathlib.Path, mode: str) -> None:
        file = pathlib.Path(file)
        for bp, bp_info in self.blueprints.items():
            structures = self.structures[bp]
            # At this step scheme_matcher matches only required fields as set in BlueprintInfo
            # Required fields must be of type str
            match = bp_info.scheme_matcher.match(file.name)
            if len(match) == len(bp.required_fields): # all required fields found
                structure_id = '__'.join(match.values()) # required fields combination
                id_info = structures.get(structure_id)
                structure_info = id_info.structure_info if id_info else None
                if not structure_info:
                    # Check if there is a structure with the same ID in DB
                    db_entry = self.db_manager.get_blueprint(bp.name, structure_id)
                    if db_entry:
                        structure_info = StructureInfo(bp.create(**db_entry), is_new=False)
                    else:
                        # Here we initialize an instance of bp with required args
                        # and further name it a structure. It's not necessary to format
                        # required fields after match because they can only be of str type
                        # and matcher always returns str
                        structure_info = StructureInfo(bp.create(**match))
                structures[structure_id] = self._StructureIdInfo(structure_info, {})
                # We need to check also optional fields on current file
                # But there could be blueprint without optional fields
                if structure_info.scheme_matcher:
                    optional_match = structure_info.scheme_matcher.match(file.name)
                    if optional_match:
                        # After match formatting
                        formatted_fields = self.format_fields(structure_info.structure, file, optional_match)
                        # Compare current field values with new ones
                        updates = self.get_updates(structure_info.structure, formatted_fields, mode)
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
    def get_updates(structure, matched_fields, mode):
        updates = {}
        for field, new_value in matched_fields.items():
            f_type = structure.get_field_type(field)
            old_value = getattr(structure, field)
            update = FieldsTransformer.update(f_type, old_value, new_value, mode)
            if not update == NoUpdate:
                updates[field] = update
        return updates

    def set_updates(self, bp, id, updates):
        self.structures[bp][id].updates.update(updates)

    @staticmethod
    def set_fields(structure: DataBlueprint, fields) -> None:
        for field, new_value in fields.items():
            setattr(structure, field, new_value)

    def update_parser_fields(self) -> None:
        '''
        Updates ParserFields with dependent fields in all structures
        '''
        for bp, structures in self.structures.items():
            for id, info in structures.items():
                structure = info.structure_info.structure
                updates = {}
                parser_fields = [getattr(structure, field.name)
                                    for field in fields(structure)
                                    if structure.get_field_type(field.name) == ParserField]
                # Select only pf with dependent_fields and without value set
                parser_fields = [pf for pf in parser_fields
                                if pf.dependent_fields and not pf.value]
                for pf in parser_fields:
                    if structure.fields_are_set(*pf.dependent_fields):
                        args = [getattr(structure, f) for f in pf.dependent_fields]
                        # Parsed values are auto set to pf value attr
                        pf.parse_value(*args)
                        updates[pf.name] = pf.value
                #  This pf processing is the worst place of the module
                self.set_updates(bp, id, updates)

    def clear_structures(self):
        self.structures = {bp: {} for bp in self.blueprints}


if __name__ == '__main__':
    db = JsonDatabse('/mnt/c/Users/misha/Desktop/materials/Programming/files-kraken/backups/db.json', storage=serialization)
    # blueprints = db.table('blueprints')
    kraken = Kraken()
    db_manager = DatabaseManager(db)
    db_updater = BlueprintsDBUpdater(db_manager)
    bb = BlueprintBuilder(kraken, db_manager, db_updater)

    t1 = 'other_154.sample_NG067.lane_69.R1.fastq.gz'
    t2 = 'wgs_154.sample_NG067.lane_69.R1.fastq.gz'
    t3 = 'wes_154.sample_NG067.lane_69.R1.fastq.gz'
    t4 = 'ces_154.sample_NG067.lane_69.R1.fastq.gz'
    t5 = 'ces_154.sample_NG067.dedup.bam'


    Changes = namedtuple('Changes', 'created deleted')
    changes = Changes([t1, t2, t3, t4, t5], [])
    # processed = bb.build(changes)
    # db.insert(processed)
    # print(db.all())

    changes_2 = Changes([
        'other_100.sample_123456.lane_1.R1.fastq.gz',
        'other_100.sample_123456.lane_1.R2.fastq.gz',
        'other_100.sample_123456.lane_2.R2.fastq.gz',
        'other_100.sample_123456.dedup.bam',
        'other_100.sample_123456.realigned.bam',
        'other_100.sample_123456.recal.bam',
        'other_100.123456.vcf',
        'other_100.sample_123456.csv',

        'other_120.sample_111111.lane_1.R1.fastq.gz',
        'other_120.sample_111111.lane_1.R2.fastq.gz',
        'other_120.sample_111111.lane_2.R2.fastq.gz',
        'other_120.sample_111111.dedup.bam',
        'other_120.sample_111111.realigned.bam',
        'other_120.sample_111111.recal.bam',
        'other_120.111111.vcf',
        'other_120.sample_111111.xslx',
    ], ['other_120.111111.vcf'])
    # bb.build(changes_2)
    
    changes_3 = Changes([
    ], [
        'other_120.sample_111111.lane_1.R1.fastq.gz',
        'other_120.sample_111111.lane_1.R2.fastq.gz',
        'other_120.sample_111111.lane_2.R2.fastq.gz'
    ])

    changes_4 = Changes(['other_120.sample_111111.xlsx'], [])

    changes_5 = Changes([
        'ces_120.SBR1442.csv',
        'ces_120.SBR1442.xlsx',
        'ces_120.Final.vcf',
        'ces_120.SBR1442.vcf'
    ], [])
    print(bb.build(changes_5))