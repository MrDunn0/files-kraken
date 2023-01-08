import pytest
import pathlib
from dataclasses import dataclass, field
from typing import ClassVar, List

from src.blueprint import DataBlueprint
from src.data_organizer import BlueprintsDBUpdater, BlueprintBuilder
from src.database import DatabaseManager
from src.fields import ParserField, DataParser
from src.krakens_nest import Kraken
from src.monitoring import Changes
from test_database import db


class TestMetricsParser(DataParser):
    def parse(*args, **kwargs):
        return 50


@dataclass
class SampleBlueprint(DataBlueprint):
    sample: str
    fastqs: List[pathlib.Path] = field(default_factory=list)
    metrics_file: pathlib.Path = None
    metric: ParserField = ParserField(
        'metric',
        parser=TestMetricsParser,
        dependent_fields=['metrics_file'])

    required_fields: ClassVar = {'sample': ('sample_([^\.]+)', 1)}

    def __post_init__(self):
        self.match_scheme = {
            'fastqs': fr'sample_{self.sample}.lane_\d+.R[1-2].fastq.gz',
            'metrics_file': fr'sample_{self.sample}.metrics.txt'
            }
        super().__post_init__()


@pytest.fixture(scope='module', autouse=True)
def kraken() -> Kraken:
    return Kraken()


@pytest.fixture(scope='class')
def builder(db, kraken) -> BlueprintBuilder:
    db_manager = DatabaseManager(db)
    db_updater = BlueprintsDBUpdater(db_manager)
    builder = BlueprintBuilder(kraken, db_manager, db_updater)
    builder.register_blueprint(SampleBlueprint)
    return builder


class TestBlueprintBuilder:
    def test_build(self, builder: BlueprintBuilder):
        '''Test the whole class'''
        # Some fake files created
        created = ['sample_1.file']
        changes = Changes(created)
        builder.build(changes)
        assert builder.db_manager.get_blueprint(
            name='SampleBlueprint',
            id='1'
        )

        # Here we report the creation of a file, which is
        # dependent field for the metric ParserField.
        # And then we check that metrics value has been parsed

        parser_deps_changes = Changes(['sample_1.metrics.txt'])
        builder.build(parser_deps_changes)
        entry = builder.db_manager.get_blueprint(
            name='SampleBlueprint',
            id='1'
        )
        assert entry['metric'] == 50

        # Check that both files will be added
        # to the corresponding db field
        fastq_paths = [
            '/sample_1.lane_1.R1.fastq.gz',
            '/sample_1.lane_1.R2.fastq.gz']
        path_list_changes = Changes(fastq_paths)
        builder.build(path_list_changes)
        entry = builder.db_manager.get_blueprint(
            name='SampleBlueprint',
            id='1'
        )
        assert entry['fastqs'] == fastq_paths

        # Check that after deletion of the fastq file
        # it will be removed from the list in the db

        deleted_fastq = ['/sample_1.lane_1.R2.fastq.gz']
        builder.build(Changes([], deleted_fastq))
        entry = builder.db_manager.get_blueprint(
            name='SampleBlueprint',
            id='1')
        assert entry['fastqs'] == ['/sample_1.lane_1.R1.fastq.gz']
