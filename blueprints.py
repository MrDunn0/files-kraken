import re
import pathlib

from dataclasses import dataclass, field, fields
from typing import ClassVar, List, Any, Pattern
from copy import copy, deepcopy
from datetime import datetime
# FilesKraken modules
from parsers import (
    RunModifiedParser,
    ModificationDateParser,
    RunEndParser,
    RunStartParser)
from fields import ParserField, FieldsTransformer
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
            if isinstance(value, ParserField):
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
                if f_type == ParserField:
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


@dataclass
class SampleInfoBlueprint(DataBlueprint):
    run: str
    sample: str
    fastqs: List[pathlib.Path] = field(default_factory=list)
    bams: List[pathlib.Path] = field(default_factory=list)
    vcf: pathlib.Path = None
    csv: List[pathlib.Path] = field(default_factory=list)
    xlsx: List[pathlib.Path] = field(default_factory=list)
    metrics: pathlib.Path = None
    date: ParserField = ParserField('date', ModificationDateParser, dependent_fields=['vcf'])

    required_fields: ClassVar = {'run': (re.compile(r'^(other|ces|wes|wgs)_\d+'), 0),
                                'sample': (
                                    (re.compile(r'sample_S?(\w+)'), 1),
                                    (re.compile(r'^\w+\.S?(?!(?:Final|AF|GF))(\w+)\.(vcf|csv|xlsx)$'), 1))
                                }

    def __post_init__(self):
        self.match_scheme = {
            'fastqs': re.compile(fr'^\w+\.sample_{self.sample}\.lane_\d+\.R[1-2]\.fastq.gz'),
            'bams': re.compile(fr'^\w+\.sample_{self.sample}\.(dedup|recal|realigned)\.bam'),
            'vcf': re.compile(fr'^\w+\.S?{self.sample}\.vcf$'),
            'csv': re.compile(fr'^\w+\.(sample_)?S?{self.sample}\.csv'),
            'xlsx': re.compile(fr'^\w+\.(sample_)?S?{self.sample}\.xlsx'),
            'metrics': re.compile(fr'^\w+\.sample_{self.sample}\.HS\.metrics\.tsv')
        }
        super().__post_init__()

@dataclass
class RunInfoBlueprint(DataBlueprint):
    run: str
    start_time: ParserField = ParserField('start_time', RunStartParser, dependent_fields=['run'])
    end_time: ParserField = ParserField('end_time', RunEndParser, dependent_fields=['run'])
    required_fields: ClassVar = {'run': (re.compile(r'^(other|ces|wes|wgs)_\d+'), 0)}

if __name__ == '__main__':
    # t = SampleInfoBlueprint('other_150', 'NG050')
    # # print(t.__annotations__)
    # from datetime import datetime
    # t1 = {
    #     'run': 'other_120',
    #     'sample': 'mgg1234',
    #     'csv': [pathlib.Path('other_120.mgg1234.csv')],
    #     'fastqs': ['fastq_1.fq.gz', 'fastq_2.fq.gz'],
    #     'vcf': pathlib.Path('other_120.Smgg1234.vcf')
    #     }

    # t2 = t.create(**t1)
    # # print(t2.fields_are_empty('run', 'sample'))
    # # print(t2)
    # # print(t2)
    # t3 = pathlib.Path('other_120.Smgg1234.vcf')
    # # print(getattr(SampleInfoBlueprint, 'date'))
    # # print(getattr(t2, 'csv'))

    t4_fields = {
        'run': 'other_140',
        'start_time': datetime.now(),
        'end_time': datetime.now()
    }

    t5_fields = {
        'run': 'other_130',
        'start_time': datetime.now(),
        'end_time': None

    }
    t4 = RunInfoBlueprint.create(**t4_fields)
    t5 = RunInfoBlueprint.create(**t5_fields)

    print(RunInfoBlueprint.start_time)
    # new_field = RunInfoBlueprint.start_time
    # new_field.value = datetime.now()
    # setattr(t5, 'start_time', new_field)
    # print(t4)
    # print(t5)
    # test_pf = ParserField('end_time', RunEndParser, dependent_fields=['run'], value=datetime.now())
    # print(id(t4.start_time))
    # print(id(t5.start_time))
    # print(id(RunInfoBlueprint.start_time))
    # print(setattr(t5, 'aboba', 10))
