import re
import pathlib

from abc import ABC
from dataclasses import dataclass, field
from typing import ClassVar, List, Dict, Pattern

# FilesKraken modules
from parsers import *

# It's good idea to create dataclass with constants for blueprints to use it here


@dataclass
class DataBlueprint:
    required_fields: ClassVar

    def get_field_type(self, field: str):
        return self.__annotations__[field]


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
    date: RunDateParser = None

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

    @staticmethod
    def create(**kwargs):
        annotations = SampleInfoBlueprint.__annotations__
        required_fields = SampleInfoBlueprint.required_fields.keys()
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
                else:
                    optional_args[field] = value

        return SampleInfoBlueprint(*required_args, **optional_args)


if __name__ == '__main__':
    t = SampleInfoBlueprint('other_150', 'NG050')
    print(t.__annotations__)
    print(t.get_field_type('run', 'sample', 'fastqs'))
    t1 = {
        'run': 'other_120',
        'sample': 'mgg1234',
        'csv': pathlib.Path('/media/EXOMEDATA/exomes/other_120/vcfs/csv_by_sample/other_120.mgg1234.csv'),
        'fastqs': ['fastq_1.fq.gz', 'fastq_2.fq.gz']
        }

    print(t.create(**t1))
    
    