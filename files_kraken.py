import pathlib

from tinydb import Storage

# FilesKraken modules
from krakens_nest import Kraken
from retools import ReSorter, GroupSearcher, BoolOutputMultimatcher
from collector import SingleRootCollector
from monitoring import MonitorManager, SingleIterationWatcher, ChangesFactory
from data_organizer import BlueprintBuilder, BlueprintsDBUpdater
from database import DatabaseManager, JsonDatabse, serialization


if __name__ == '__main__':
    TEST_PATH_1 = '/home/ushakov/repo/cerbalab/SamplesInfoCollector/test'
    TEST_PATH_2 = '/media/EXOMEDATA/exomes/'
    TEST_PATH_3 = '/mnt/c/Users/misha/Desktop/materials/Programming/files-kraken/test'
    SCRIPT_DIR = pathlib.Path(__file__).parent.absolute()
    BACKUPS_DIR = SCRIPT_DIR / 'backups'
    CW_BACKUP_FILE = BACKUPS_DIR / 'cw_backups.json'
    
    DATABASE = JsonDatabse('/mnt/c/Users/misha/Desktop/materials/Programming/files-kraken/backups/db.json',
                            storage=serialization)

    kraken = Kraken()
    monitor_manager = MonitorManager(BACKUPS_DIR, kraken)
    db_manager = DatabaseManager(DATABASE)
    db_updater = BlueprintsDBUpdater(db_manager)
    bb = BlueprintBuilder(kraken, db_manager, db_updater)

    upper_src = SingleRootCollector(
        TEST_PATH_3,
        matcher = BoolOutputMultimatcher(
            [
                (r'^ces_\d+', 0),
                (r'^wes_\d+', 0),
                (r'^other_\d+', 0),
                (r'^wgs_\d+', 0)
            ]
            ),
        match_dirs=True,
        max_depth=0)
    lower_src = SingleRootCollector(
        None,
        matcher = BoolOutputMultimatcher([
            (r'fastq\.gz', 0),
            (r'\.vcf$', 0),
            (r'\.csv', 0),
            (r'\.xlsx', 0),
            (r'\.metrics\.tsv', 0),
            (r'\.bam$', 0)],
            exclude=[r'(?:other|ces|wes|wgs|)_\d+\.(AF|GF|S\d[0-1]?)\.vcf$']),
        keep_empty_dirs=False)

    lower_src_2 = SingleRootCollector(
        None,
        matcher = BoolOutputMultimatcher([r'hs_metrics', r'HS_Report\.html']),
        keep_empty_dirs=False)

    sorter = ReSorter(GroupSearcher(r'_(\d+)', 1), int)

    upper_siw = SingleIterationWatcher(upper_src, sorter=sorter,
                                        changes_formatter=ChangesFactory.dict_collection,
                                        keep_empty_dirs=True)
    recursive_siw = SingleIterationWatcher(lower_src, keep_empty_dirs=False,
                                            changes_formatter=ChangesFactory.dict_collection)


    rec_siw_2 = SingleIterationWatcher(lower_src_2, keep_empty_dirs=False,
                                            changes_formatter=ChangesFactory.dict_collection)

    monitor_manager.add_monitor(upper_siw, CW_BACKUP_FILE, timeout =5, reindex_timeout=10)
    monitor_manager.add_coworker(upper_siw, recursive_siw)
    monitor_manager.add_coworker(upper_siw, rec_siw_2)
    monitor_manager.start()
