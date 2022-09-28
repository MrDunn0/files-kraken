import re
import pathlib
from dataclasses import dataclass, field
from typing import List, ClassVar


# FilesKraken imports
from src.blueprint import DataBlueprint
from src.fields import ParserField
from parsers import (
    RunModifiedParser,
    ModificationDateParser,
    RunEndParser,
    RunStartParser)


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

    from datetime import datetime
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
