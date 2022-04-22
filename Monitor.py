
import time
import json
from dataclasses import dataclass, field
from Collector import *
from typing import *
from collections import namedtuple
from datetime import datetime


class FilesWatcher(ABC):
    def __init__(self, collector):
        self.collector = collector
        self.prev_state = None

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def _get_changes(self, cur_state):
        pass


class ConstantWatcher(FilesWatcher):
    def __init__(self, collector, manager=None, backup_file=None, sorter=None, timeout=60):
        super().__init__(collector)
        self.manager = manager
        self.sorter = sorter
        self.timeout = timeout
        if not backup_file:
            script_dir = pathlib.Path(__file__).parent.absolute()
            self.backup_file = script_dir/'rfm_backup.json'
            self.prev_state = DictCollection()
        else:
            self.backup_file = pathlib.Path(backup_file)
            self.prev_state = self._load_state()


    def start(self):
        print(f'[{datetime.now()}] {self} Starting on root {self.collector.root}')
        while True:
            cur_state = DictCollection(self.collector.collect())
            changes = self._get_changes(cur_state)
            if changes:
                if self.sorter:
                    changes = self.sorter.sort(changes)
                # print(
                # f'[{datetime.now()}] {type(self).__name__} Found following changes: \n\t',
                # '\n\t'.join(f for f in changes)
                # )
                self.prev_state = cur_state
                self._save_state()
            else:
                pass
                # print(f'[{datetime.now()}] {type(self).__name__} Nothing found')
            if self.manager:
                    self.manager.report(MonitorInfo(self, changes))
            time.sleep(self.timeout)

    def _get_changes(self, cur_state):
        cur_state = set(cur_state.to_list(keep_empty_dirs=True))
        prev_state = set(self.prev_state.to_list(keep_empty_dirs=True))
        changes = list(cur_state - prev_state)
        return changes

    def __str__(self):
        return type(self).__name__


    # It can be done with pickle
    #https://stackoverflow.com/questions/19201290/how-to-save-a-dictionary-to-a-file
    # Also we can make an option for chosing json or pickle.
    # Goog idea is to make a separate class for backups, because there will be
    # many places in this app where it will be needed.
    def _save_state(self):
        with open(self.backup_file, 'w', encoding='utf-8') as f:
            json.dump(self.prev_state, f)

    def _load_state(self):
        if self.backup_file.exists():
            with open(self.backup_file, 'r', encoding='utf-8') as f:
                return DictCollection(json.load(f))


class SingleIterationWatcher(FilesWatcher):
    def __init__(self, collector, sorter=None, prev_state=None, keep_empty_dirs=True):
        super().__init__(collector)
        self.sorter = sorter
        self.keep_empty_dirs = keep_empty_dirs
        self.prev_state = prev_state if prev_state else DictCollection()

    def start(self):
        cur_state = DictCollection(self.collector.collect())
        changes = self._get_changes(cur_state)
        if changes:
            self.prev_state = cur_state
        return changes

    def _get_changes(self, cur_state):
        Changes = namedtuple('Changes', 'created deleted')
        cur_state = set(cur_state.to_list(keep_empty_dirs=self.keep_empty_dirs))
        prev_state = set(self.prev_state.to_list(keep_empty_dirs=self.keep_empty_dirs))
        deleted = prev_state - cur_state
        created = cur_state - prev_state
        if self.sorter:
            created = self.sorter.sort(created)
            deleted = self.sorter.sort(deleted)
        if created or deleted:
            return Changes(created, deleted)
            # print(f'[{datetime.now()}] {self} Nothing found')
        return None

    def __str__(self):
        return type(self).__name__


class Event(list):
    def __call__(self, *args, **kwargs):
        for item in self:
            item(*args, **kwargs)


# @dataclass
# class MonitorInfo:
#         monitor: FilesWatcher
#         data: list


@dataclass
class BackupManager:

    @dataclass
    class BackupInfo:
        backup_file: pathlib.Path
        collection: Callable
        format: str = 'json'

    backups: Dict[Any, BackupInfo] = field(default_factory=dict)

    def add(self, obj, backup_file, collection=DictCollection):
        backup_file = pathlib.Path(backup_file)
        self.backups[obj] = self.BackupInfo(pathlib.Path(backup_file ), collection)
        if not backup_file.exists():
            self.save(obj, collection())

    def get_path(self, obj):
        return self.backups[obj].backup_file

    def save(self, obj, data):
        obj_info = self.backups[obj]
        match obj_info.format:
            case 'json':
                with open(obj_info.backup_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f)

    def load(self, obj):
        obj_info = self.backups[obj]
        match obj_info.format:
            case 'json':
                with open(obj_info.backup_file, encoding='utf-8') as f:
                    return obj_info.collection(json.load(f))


class MonitorManager:
    @dataclass
    class MonitorInfo:
        # maybe  better to import annotation from __future__ and write just list[Monitor]
        # https://stackoverflow.com/questions/69802491/create-recursive-dataclass-with-self-referential-type-hints
        backup_file: Optional[pathlib.Path]
        timeout: int
        reindex_timeout: Optional[time.time]
        coworkers: List[SingleIterationWatcher] = field(default_factory=list)
        last_reindex: Optional[time.time] = None
        last_run: Optional[time.time] = None

    def __init__(self):
        self.events = Event()
        self.monitors = {}
        self.backup_manager = BackupManager()
        self._exit = False

    def add_monitor(self, monitor: SingleIterationWatcher, backup_file=None,
                    timeout: int = 10,
                    reindex_timeout: int = None) -> None:

        self.monitors[monitor] = self.MonitorInfo(backup_file, timeout, reindex_timeout)
        if backup_file:
            self.backup_manager.add(monitor, backup_file=backup_file)
            monitor.prev_state = self.backup_manager.load(monitor)

    def add_coworker(self, monitor: SingleIterationWatcher, coworker):
        self.monitors[monitor].coworkers.append(coworker)

    def _time_to_rerun(self, info):
        if info.last_run:
            return time.time() - info.last_run > info.timeout
        return True

    def _time_to_reindex(self, info):
        if info.last_reindex:
            return time.time() - info.last_reindex > info.reindex_timeout
        return True

    @staticmethod
    def _print_changes(monitor, changes):
        now = datetime.now().isoformat(' ', 'seconds')
        if changes.deleted:
            print(
                f'[{now}] {monitor}: \n\tDeleted ',
                '\n\tDeleted '.join(f for f in changes.deleted))
        if changes.created:
            print(
                f'[{now}] {monitor}: \n\tCreated ',
                '\n\tCreated '.join(f for f in changes.created))

    def _run_coworkers(self, coworkers, changes):
        for file in changes:
            for coworker in coworkers:
                coworker.collector.root = pathlib.Path(file)
                print(f'Reindexing {file}')
                coworker_changes = coworker.start()
                if coworker_changes:
                    self._print_changes(coworker, coworker_changes)
                coworker.prev_state = DictCollection()

    def start(self):
        while not self._exit:
            time.sleep(1) # it helps not to load full core
            for monitor, info in self.monitors.items():
                if self._time_to_rerun(info):
                    changes = monitor.start()
                    if changes:
                        self._print_changes(monitor, changes)
                        self.backup_manager.save(monitor, monitor.prev_state)
                        if info.coworkers and changes.created:
                            self._run_coworkers(info.coworkers, changes.created)
                    else:
                        print('No data')
                    info.last_run = time.time()
                if info.reindex_timeout:
                    if self._time_to_reindex(info) and info.coworkers:
                        
                        self._run_coworkers(info.coworkers, monitor.prev_state.to_list())

if __name__ == '__main__':
    TEST_PATH_1 = '/home/ushakov/repo/cerbalab/SamplesInfoCollector/test'
    TEST_PATH_2 = '/media/EXOMEDATA/exomes'

    monitor_manager = MonitorManager()
    upper_src = SingleRootCollector(
        TEST_PATH_1,
        matcher = AnyMatcher([r'^ces_\d+', r'^wes_\d+', r'^other_\d+', r'^wgs_\d+']),
        match_dirs=True,
        max_depth=0)
    lower_src = SingleRootCollector(
        None,
        matcher = AnyMatcher([r'fastq\.gz', r'Final.\vcf', r'\.csv', r'\.bam$']),
        keep_empty_dirs=False)
    sorter = ReSorter(GroupSearcher(r'_(\d+)', 1), int)

    upper_siw = SingleIterationWatcher(upper_src, sorter=sorter)
    recursive_siw = SingleIterationWatcher(lower_src, keep_empty_dirs=False)

    SCRIPT_DIR = pathlib.Path(__file__).parent.absolute()
    CW_BACKUP_FILE = SCRIPT_DIR / 'cw_backups.json'

    monitor_manager.add_monitor(upper_siw, CW_BACKUP_FILE, reindex_timeout=20)
    monitor_manager.add_coworker(upper_siw, recursive_siw)
    monitor_manager.start()
