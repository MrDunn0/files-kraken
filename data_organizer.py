from ctypes import Structure
import pathlib
from dataclasses import dataclass, asdict
from collections import namedtuple
from typing import List, Dict, NamedTuple, Any
from copy import copy

# FilesKraken modules
from blueprints import DataBlueprint
from retools import SchemeMatcher
from krakens_nest import Kraken
from database import DatabaseManager, JsonDatabse
from parsers import DataParser
from info import CreatedFilesOrganizedInfo, DeletedFilesOrganizedInfo, FileChangesInfo


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

@dataclass
class BlueprintInfo:
    blueprint: DataBlueprint

    def __post_init__(self):
        self.scheme_matcher = SchemeMatcher(self.blueprint.required_fields)


@dataclass
class StructureInfo:
    structure: DataBlueprint

    def __post_init__(self):
        self.scheme_matcher = SchemeMatcher(self.structure.match_scheme)


class BlueprintBuilder:
    _Structures = namedtuple('Structures', 'created deleted')
    def __init__(self, kraken: Kraken):
        self.kraken = kraken
        self.blueprints = {bp: BlueprintInfo(bp) for bp in DataBlueprint.__subclasses__()}
        self.structures = self._Structures(
            {bp: {} for bp in self.blueprints},
            {bp: {} for bp in self.blueprints})
        self.kraken.events.append(self.listen)

    def listen(self, info):
        if isinstance(info, FileChangesInfo):
            print(f'Data organizer has recieved changes: {info.changes}')
            self.build(info.changes)

    # All methods hardcoded for dict_collection ChangesFactory format

    def build(self, data: NamedTuple):
        for file in data.created:
            self._process_file(file, self.structures.created)
        for file in data.deleted:
            self._process_file(file, self.structures.deleted)
        constructed = {
            bp: structures
            for bp, structures in self.structures.created.items() if structures}
        deconstructed = {
            bp: structures
            for bp, structures in self.structures.deleted.items() if structures}
        if constructed:
            self.kraken.move_tentacle(CreatedFilesOrganizedInfo(constructed))
        if deconstructed:
            self.kraken.move_tentacle(DeletedFilesOrganizedInfo(deconstructed))
        # Delete all builded structures
        self.clear_structures()

    # It will be good to write matched blueprints iterator
    def _process_file(
        self,
        file: pathlib.Path,
        structures_field: Dict[DataBlueprint, Dict[str, StructureInfo]]):

        file = pathlib.Path(file)

        file_name = file.name # Idk if it's usefull at all to have alternative to pathlib.Path
        for bp, bp_info in self.blueprints.items():
            structures = structures_field[bp]
            # At this step shecme_matcher matches only required fields as set in BlueprintInfo
            # Required fields must be of type str
            match = bp_info.scheme_matcher.match(file_name)
            if len(match) == len(bp.required_fields): # all required fields found
                structure_id = '__'.join(match.values()) # required fields combination
                structure_info = structures.get(structure_id)
                if structure_info: # object with structure_id is already being constructed
                    self._check_optional(file, structure_info)
                else:
                    # Here we initialize an instance of bp with required args
                    # and further name it a structure
                    structure_info = StructureInfo(bp(*match.values()))
                    structures[structure_id] = structure_info
                    # Only required fields have been checked, but
                    # the optional fields need to be checked as well
                    self._check_optional(file, structure_info)

    @staticmethod
    def format_fields(bp: DataBlueprint, file: pathlib.Path, match: Dict[str, str]):
        # I think it can be represented as specification in blueprints module
        formatted_fields = {}
        field_types = {f: bp.get_field_type(f) for f in match}
        for field, matched_value in match.items():
            f_type = field_types[field]
            if f_type in {str, List[str]}:
                formatted_fields[field] = matched_value
            elif f_type in {pathlib.Path, List[pathlib.Path]}:
                formatted_fields[field] = file.absolute()
            elif isinstance(f_type, DataParser):
                formatted_fields[field] = 'not_implemented'
        return formatted_fields

    def _check_optional(self, file: pathlib.Path, structure_info: StructureInfo):
        optional_match = structure_info.scheme_matcher.match(file.name)
        if optional_match:  # optional fields match
            formatted_fields = self.format_fields(structure_info.structure, file, optional_match)
            self._set_optional(structure_info.structure, formatted_fields)

    @staticmethod
    def check_field_type(structure: DataBlueprint, field: str, type_to_check: Any):
        return structure.get_field_type(field) == type_to_check

    @staticmethod
    def _set_optional(structure: DataBlueprint, fields):
        list_types = {List[pathlib.Path], List[str]}
        value_types = {str, pathlib.Path}

        '''Here is the same problem which is described in BlueprintsDBUpdater
        compare_blueprints() method. When user defines single value field for
        custom blueprint, a situation may arise where several files match this field.
        Now this method just places new_value instead old_value. So the last iterated will
        remain as a field value.'''
        for field, value in fields.items():
            f_type = structure.get_field_type(field)
            if f_type in value_types:
                setted = getattr(structure, field)
                if setted:
                    print(
                        f'WARNING: Second value for the \'{field}\' single value field was reported:',
                        f'\tPrevious value: {setted}',
                        f'\tCurrent value: {value}',
                        'The last value reported for this field will be setted',
                        'Consider to change the field type to list to save all files')
                setattr(structure, field, value)
            elif f_type in list_types:
                structure_field = getattr(structure, field)
                structure_field.append(value)
            elif isinstance(f_type, DataParser):
                # I think we will run parsers here
                setattr(structure, field, 'not_implemented')

    def clear_structures(self):
        self.structures = self._Structures(
            {bp: {} for bp in self.blueprints},
            {bp: {} for bp in self.blueprints})

class BlueprintsDBUpdater:
    def __init__(self, db_manager: DatabaseManager, kraken: Kraken):
        self.db_manager = db_manager
        self.kraken = kraken
        self.kraken.events.append(self.listen)

    def compare_blueprints(self, new_bp: DataBlueprint, old_bp: DataBlueprint, mode: str) -> Dict[str, Any]:
        '''Compares fields of new_bp and old_bp and returns new fields with values'''
        # Mmmm  spaghetti...
        updates = {}
        for new, old in zip(asdict(new_bp).items(), asdict(old_bp).items()):
            field = new[0]
            field_type = new_bp.get_field_type(field)
            new_value, old_value = new[1], old[1]
            
            if field in new_bp.required_fields:
                continue  # we don't want to reset structure required fields

            # No updates or field still empty
            if (not new_value) or (not new_value and not old_value):
                continue
            
            # Equal values
            if new_value == old_value:
                if mode == 'deleted':
                    updates[field] = None
                else:
                    continue

            elif not old_value and mode == 'created':
                updates[field] = str(new_value)

            # Below are the cases when all fields values present
            elif new_value != old_value:
                '''I'm not shure this is 100% useful statement. When file is deleted, we 
                expect its name to be equal to the one in DB. In the second case file is created
                and reported as new, but we have another already set value for that field. It seems
                to be a bug / nonspecificity of matching patterns for some user field. And I think
                it is better to throw an exception and let him know about that, than just change old
                value. But I think that some users may prefer to see just WARNING. The better way is
                to add specificity check for patterns, but i think it's impossible without making 
                them match full file names. This will result in a loss of usability.'''
                if field_type in {str, pathlib.Path}:
                    raise ValueError(
                            f'Blueprint from DB has a set value in the \'{new[0]}\' field ' +
                            f'while new value for that field has been reported by monitoring module.\n' +
                            f'\tOld value: {old_value}\n\tNew value: {new_value}\n\tMode: {mode}')

                elif field_type in {List[str], List[pathlib.Path]}:
                    if mode == 'created':
                        updated_list = [el for el in old_value.copy()]
                        updated_list.extend(el for el in new_value if el not in updated_list)
                    elif mode == 'deleted':
                        updated_list = [el for el in old_value if el not in new_value]
                    updates[field] = [str(el) for el in updated_list]

                elif isinstance(field_type, DataParser):
                    continue
                else:
                    raise NotImplementedError(
                        f'Comparison for {type(old_value)} type field is not implemented')
        return updates

    def process(self, listener_info):
        builder_data = listener_info.structures
        for bp, structures in builder_data.items():
            bp_name = bp.__name__
            for id, bp_info in structures.items():
                db_entry = self.db_manager.get_blueprint(bp_name, id)
                if not db_entry:
                    if isinstance(listener_info, CreatedFilesOrganizedInfo):
                        entry = self.entry_from_structure(bp_info.structure, id)
                        self.db_manager.add_blueprint(entry)
                    elif isinstance(listener_info, DeletedFilesOrganizedInfo):
                        continue
                else:
                    old_bp = bp.create(**db_entry)
                    new_bp = bp_info.structure
                    try:
                        if isinstance(listener_info, CreatedFilesOrganizedInfo):
                            updates = self.compare_blueprints(new_bp, old_bp, 'created')
                            self.db_manager.update_blueprint(bp_name, id, updates)
                        elif isinstance(listener_info, DeletedFilesOrganizedInfo):
                            updates = self.compare_blueprints(new_bp, old_bp, 'deleted')
                            self.db_manager.update_blueprint(bp_name, id, updates)
                    except TypeError as e:
                        print(new_bp)
                        print(old_bp)
                        print(updates)
                        raise e
    def listen(self, info):
        if isinstance(info, (CreatedFilesOrganizedInfo, DeletedFilesOrganizedInfo)):
            print(f'BlueprintDBUpdater has recieved blueprints: {info.structures}')
            self.process(info)

    def entry_from_structure(self, structure: DataBlueprint, id: str):
        name = structure.__class__.__name__
        entry = {'blueprint': name, 'id': id}
        for field, value in asdict(structure).items():
            f_type = structure.get_field_type(field)
            if value is None:
                value = None
            elif f_type == pathlib.Path:
                value = str(value.absolute())
            elif f_type == List[pathlib.Path]:
                value = [str(el.absolute()) for el in value]
            entry[field] = value
        return entry


if __name__ == '__main__':
    db = JsonDatabse('/home/ushakov/repo/cerbalab/SamplesInfoCollector/backups/db.json')
    # blueprints = db.table('blueprints')
    kraken = Kraken()
    db_manager = DatabaseManager(db)
    db_updater = BlueprintsDBUpdater(db_manager, kraken)
    bb = BlueprintBuilder(kraken)

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