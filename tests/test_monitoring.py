import pytest
import pyfakefs
import pathlib
import os
import threading
import time
import json

from shutil import rmtree
from dataclasses import dataclass, field
from typing import Hashable, Callable

from tests_data.collector_collections import (
    FS_DEFAULT_MATCH_COLLECTION,
    FS_MODIFIED_COLLECTION
)
from src.collector import DictCollection, SingleRootCollector
from src.monitoring import ChangesWatcher, ChangesFactory, MonitorManager, BackupManager
from copy import deepcopy
from test_collector import create_SRC, create_BOM, test_matcher


@pytest.fixture()
def files_dc():
    yield DictCollection(deepcopy(FS_DEFAULT_MATCH_COLLECTION))


@pytest.fixture()
def fake_filesystem(fs):
    yield fs


@pytest.fixture(autouse=True)
def fs_files_from_dict(fs, files_dc: dict[str, str | None]):
    # There could be some problems with infinite loop according to this thread
    # https://stackoverflow.com/questions/18011902/pass-a-parameter-to-a-fixture-function
    stack = list(files_dc.items())
    while stack:
        file, content = stack.pop()
        if isinstance(content, dict):
            fs.create_dir(file)
            stack.extend(
                {
                    f'{file}{os.sep}{subdir}': items
                    for subdir, items in content.items()
                }.items())
        else:
            fs.create_file(file)


@pytest.fixture()
def collector(collector_args):
    '''Builds SingleRootCollector with hardcoded root and matcher'''
    src = SingleRootCollector(
        root='/fs/tests_data/collector_path/',
        matcher=test_matcher,
        **collector_args)
    yield src


@pytest.fixture()
def watcher(collector, watcher_args):
    watcher = ChangesWatcher(collector=collector, **watcher_args)
    yield watcher


def test_fake_fs_existance(fs, fs_files_from_dict):
    assert os.path.exists('/fs/tests_data/')
    assert not os.path.exists('/fs/tests_data/nonexisting')


class TestChangesWatcher:
    @pytest.mark.parametrize(
        'collector_args', [dict(keep_empty_dirs=False)]
        )
    @pytest.mark.parametrize(
        'watcher_args', [dict(prev_state=DictCollection(FS_DEFAULT_MATCH_COLLECTION))])
    def test_get_changes(
            self, fs, collector: SingleRootCollector, watcher: ChangesWatcher):
        '''Test files creation and deletion handling'''

        fs.create_dir('/fs/tests_data/collector_path/run_4')
        fs.create_dir('/fs/tests_data/collector_path/run_4/bams')
        fs.create_file('/fs/tests_data/collector_path/run_4/bams/run_4.sample_14.bam')
        changes = watcher.get_changes()
        assert changes.created == ['/fs/tests_data/collector_path/run_4/bams/run_4.sample_14.bam']
        assert not changes.deleted

        os.remove('/fs/tests_data/collector_path/run_4/bams/run_4.sample_14.bam')
        os.mkdir('/fs/tests_data/collector_path/run_4/results')
        open('/fs/tests_data/collector_path/run_4/results/run_4.results.txt', 'w').close()

        changes = watcher.get_changes()
        assert changes.created == ['/fs/tests_data/collector_path/run_4/results/run_4.results.txt']
        assert changes.deleted == ['/fs/tests_data/collector_path/run_4/bams/run_4.sample_14.bam']

    @pytest.mark.parametrize('collector_args', [dict()])
    @pytest.mark.parametrize(
        'watcher_args,expected',
        [(dict(name='test_watcher'), 'test_watcher'), (dict(name=None), 'ChangesWatcher_2')])
    def test_name(self, collector: SingleRootCollector, watcher: ChangesWatcher, expected: str):
        '''Test ChangesWatcher class properties'''
        # We have ChangesWatcher_2 here because first instance is created above.
        assert watcher.name == expected

    @pytest.mark.parametrize('collector_args', [dict()])
    @pytest.mark.parametrize('watcher_args', [dict()])
    def test_changes_formatter(self, collector: SingleRootCollector, watcher: ChangesWatcher):
        '''Test changes_formatter property'''

        assert watcher.changes_formatter == ChangesFactory.dict_collection

    @pytest.mark.parametrize('collector_args', [dict()])
    @pytest.mark.parametrize('watcher_args', [dict()])
    def test_collection(self, collector: SingleRootCollector, watcher: ChangesWatcher):
        '''Test that collection is the same as collector.output_format'''

        assert watcher.collection == collector.output_format

    @pytest.mark.parametrize('collector_args', [dict()])
    @pytest.mark.parametrize('watcher_args', [dict()])
    def test_state(self, fs, collector: SingleRootCollector, watcher: ChangesWatcher):
        '''Test set_state and reset_state'''

        watcher.get_changes()
        assert watcher.prev_state == FS_DEFAULT_MATCH_COLLECTION
        watcher.reset_state()
        assert watcher.prev_state == {}
        watcher.set_state(FS_DEFAULT_MATCH_COLLECTION)
        assert watcher.prev_state == FS_DEFAULT_MATCH_COLLECTION

    @pytest.mark.parametrize('collector_args', [dict()])
    @pytest.mark.parametrize('watcher_args', [dict()])
    def test_set_root(self, collector: SingleRootCollector, watcher: ChangesWatcher):
        watcher.set_root('/fs')
        assert collector.root == pathlib.Path('/fs')


# BackupManager Tests


@pytest.fixture(scope='class')
def backup_manager():
    return BackupManager()


@dataclass
class BackupInfo:
    obj: Hashable
    backup_file: str | pathlib.PosixPath
    new_data: field(default_factory=dict)
    modified_data: field(default_factory=dict)
    collection: Callable = DictCollection


@pytest.fixture()
def backup_info():
    return BackupInfo(
        'backup_object',
        '/fs/test_object.json',
        dict(test='dict'),
        dict(test='passed')
    )


class TestBackupManager:
    def test_add(self, fs, backup_manager: BackupManager, backup_info: BackupInfo):
        backup_manager.add(
            backup_info.obj,
            backup_info.backup_file,
            collection=DictCollection
        )
        assert backup_info.obj in backup_manager.backups
        assert os.path.exists(backup_info.backup_file)

    def test_stores(self, backup_manager: BackupManager, backup_info: BackupInfo):
        assert backup_manager.stores(backup_info.obj)

    def test_save(self, backup_manager: BackupManager, backup_info: BackupInfo):
        backup_manager.save(backup_info.obj, data=backup_info.new_data)
        with open(backup_info.backup_file) as f:
            loads = json.load(f)
            assert loads == backup_info.new_data

    def test_update(self, backup_manager: BackupManager, backup_info: BackupInfo):
        backup_manager.save(backup_info.obj, data=backup_info.new_data)
        backup_manager.update(backup_info.obj, backup_info.modified_data)

    def test_load(self, backup_manager: BackupManager, backup_info: BackupInfo):
        backup_manager.save(backup_info.obj, data=backup_info.new_data)
        backup_manager.update(backup_info.obj, backup_info.modified_data)
        assert backup_manager.load(backup_info.obj) == backup_info.modified_data

    def test_clear_backup(self, backup_manager: BackupManager, backup_info: BackupInfo):
        backup_manager.save(backup_info.obj, data=backup_info.new_data)
        backup_manager.clear_backup(backup_info.obj)
        assert backup_manager.load(backup_info.obj) == {}


# MonitorManager tests


def manager_timed_exit(exit_file, sec: int = 0):
    '''Writes string to exit_file after a given time'''
    sec = int(sec)
    waiting_time = 0
    while waiting_time < sec:
        print(f'Exit in {sec - waiting_time}')
        time.sleep(1)
        waiting_time += 1
    with open(exit_file, 'w') as f:
        f.write('exit')


class TestMonitorManager:
    def test_start(self, fs):
        main_bom_patterns = [r'run_[0-9]+']
        coworker_bom_patterns = [r'.+\.fastq.gz', r'.+\.bam', r'.+metrics.txt', r'.+results.txt']

        BACKUP_DIR = '/fs/backups/'
        EXIT_FILE = '/fs/exit.txt'
        main_bom = create_BOM(main_bom_patterns)
        coworker_bom = create_BOM(coworker_bom_patterns)
        main_src = create_SRC(
            root='/fs/tests_data/collector_path/',
            matcher=main_bom,
            match_dirs=True,
            max_depth=0
        )
        coworker_src = create_SRC(root=None, matcher=coworker_bom, keep_empty_dirs=False)
        main_cw = ChangesWatcher(main_src, name='Main Monitor', keep_empty_dirs=True)
        coworker = ChangesWatcher(coworker_src, name='Coworker')
        manager = MonitorManager(BACKUP_DIR, exit_file=EXIT_FILE)
        manager.add_monitor(main_cw, backup_file='main_monitor_backup.json')
        manager.add_coworker(main_cw, coworker)

        assert os.path.exists('/fs/tests_data/collector_path/run_2/results/')
        # Run a parallel thread with function which will stop the monitor manager
        exit_thread = threading.Thread(target=manager_timed_exit, args=(EXIT_FILE, 3))
        exit_thread.start()

        # Make some changes in FS
        os.makedirs('/fs/tests_data/collector_path/run_5/results/')
        open('/fs/tests_data/collector_path/run_5/results/sample_12.results.txt', 'w').close()
        os.remove('/fs/tests_data/collector_path/run_1/bams/sample_1.bam')
        rmtree('/fs/tests_data/collector_path/run_2')

        # Start the monitor manager
        manager.start()
        # Manager stops when function in parallel thread writes to the specified file
        with open('/fs/backups/Coworker.json') as f:
            assert json.load(f) == FS_MODIFIED_COLLECTION
