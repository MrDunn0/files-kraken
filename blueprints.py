from email import parser
from email.parser import Parser
import re
import pathlib

from abc import ABC
from dataclasses import dataclass, field, fields
from typing import ClassVar, List, Any, Pattern

# FilesKraken modules
from parsers import *

# It's good idea to create dataclass with constants for blueprints to use it here


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

    def parse_value(self, *args, **kwargs):
        self.value = self.parser.parse(*args, **kwargs)


@dataclass
class DataBlueprint:
    required_fields: ClassVar

    def get_field_type(self, field: str):
        return self.__annotations__[field]

    def fields_are_empty(self, *fields):
        res = {}
        for f in fields:
            if hasattr(self, f):
                value = getattr(self, f)
                res[f] = False if value else True
        return res

    def __post_init__(self):
        '''
        This should be executed after match_scheme declaration in child classes.
        Here patterns are set to match_scheme for each ParserField.
        '''
        self.has_parser_fields = False
        for field, value in self.__dict__.items():
            if isinstance(value, ParserField):
                pf = value
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
        annotations = cls.__annotations__
        required_fields = cls.required_fields.keys()
        required_args = [kwargs[arg] for arg in required_fields]
        # It realy needs some sort of specification...
        optional_args = {}
        for field, value in kwargs.items():
            if (field not in required_fields) and (field in annotations):
                f_type = annotations[field]
                if value is None:
                    optional_args[field] = None
                elif f_type == pathlib.Path:
                    optional_args[field] = pathlib.Path(value)
                elif f_type == List[pathlib.Path]:
                    optional_args[field] = [pathlib.Path(file) for file in value]
                elif f_type == ParserField:
                    parser_field = getattr(cls, field)
                    setattr(parser_field, 'value', value)
        return cls(*required_args, **optional_args)


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

if __name__ == '__main__':
    t = SampleInfoBlueprint('other_150', 'NG050')
    # print(t.__annotations__)
    from datetime import datetime
    t1 = {
        'run': 'other_120',
        'sample': 'mgg1234',
        'csv': [pathlib.Path('other_120.mgg1234.csv')],
        'fastqs': ['fastq_1.fq.gz', 'fastq_2.fq.gz'],
        'vcf': pathlib.Path('other_120.Smgg1234.vcf')
        }

    t2 = t.create(**t1)
    # print(t2.fields_are_empty('run', 'sample'))
    print(t2)


    def update_parser_fields(structure: DataBlueprint, updates=None):
        '''
        Updates ParserFields with dependent_fields in place  based on
        already set values and possible updates.
        '''
        # It's a temporary solution and I hope to clear fields processing in the future.
        parser_fields = [getattr(structure, field.name)
                            for field in fields(structure)
                            if structure.get_field_type(field.name) == ParserField]

        parser_fields = [pf for pf in parser_fields if pf.dependent_fields]
        for pf in parser_fields:
            empty_check = structure.fields_are_empty(*pf.dependent_fields)
            args = []
            for field, empty in empty_check.items():
                if empty:
                    if updates and field in updates:
                        args.append(updates[field])
                else:
                    args.append(getattr(structure, field))
            if len(args) == len(pf.dependent_fields):
                    pf.parse_value(*args)


    update_parser_fields(t2)
    print(t2)
    t3 = pathlib.Path('other_120.Smgg1234.vcf')

