import time
import json
import pathlib
import os

from dataclasses import dataclass, field
from datetime import datetime
from itertools import count
from json.decoder import JSONDecodeError
from typing import (
    Callable, Dict, Any,
    Optional, List)
# FilesKraken modules
from collector import FilesCollector
from info import FileChangesInfo
from krakens_nest import Kraken
from functions import get_module_dir, create_dirs


class Event(list):
    def __call__(self, *args, **kwargs):
        for item in self:
            item(*args, **kwargs)


@dataclass
class Changes:
    created: list = field(default_factory=list)
    deleted: list = field(default_factory=list)

    def extend(self, other):
        self.created.extend(other.created)
        self.deleted.extend(other.deleted)

    def __len__(self):
        return len(self.created) + len(self.deleted)


class ChangesFactory:
    @staticmethod
    def dict_collection(prev_state, cur_state, sorter=None, **dcargs) -> Changes:
        cur_state = set(cur_state.to_list(**dcargs))
        prev_state = set(prev_state.to_list(**dcargs))
        deleted = list(prev_state - cur_state)
        created = list(cur_state - prev_state)
        if sorter:
            created = sorter.sort(created)
            deleted = sorter.sort(deleted)
        if created or deleted:
            return Changes(created, deleted)


class ChangesWatcher:
    _ids = count(0)

    def __init__(
        self,
        collector: FilesCollector,
        changes_formatter: Callable = ChangesFactory.dict_collection,
        prev_state=None,
        name=None,
        **formatter_args
    ):
        self.collector = collector
        self._id = f'ChangesWatcher_{next(self._ids)}'
        self.prev_state = prev_state if prev_state else self.collection()
        self.changes_formatter = changes_formatter
        self._formatter_args = formatter_args
        self._name = name

    def get_changes(self):
        cur_state = self.collector.collect()
        changes = self.changes_formatter(self.prev_state, cur_state, **self._formatter_args)
        if changes:
            self.set_state(cur_state)  # I'm not sure it's good to set state here
        return changes

    @property
    def collection(self):
        return self.collector.output_format

    @property
    def name(self):
        return self._name if self._name else self._id

    def set_state(self, state):
        self.prev_state = state

    def reset_state(self):
        self.prev_state = self.collection()

    def set_root(self, root):
        self.collector.root = pathlib.Path(root)

    def __str__(self):
        return self.name


@dataclass
class BackupManager:

    @dataclass
    class BackupInfo:
        backup_file: pathlib.Path
        collection: Callable
        format: str

    backups: Dict[Any, BackupInfo] = field(default_factory=dict)

    def add(self, obj, backup_file, collection, format='json'):
        backup_file = pathlib.Path(backup_file)
        self.backups[obj] = self.BackupInfo(backup_file, collection, format)
        if not backup_file.exists():
            self.save(obj, collection())

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
                    try:
                        data = obj_info.collection(json.load(f))
                    except JSONDecodeError:
                        data = obj_info.collection()
                    return data

    def update(self, obj, new_data):
        # method requires all obj_info.collection to have .update() method
        old_data = self.load(obj)
        old_data.update(new_data)
        self.save(obj, old_data)

    def stores(self, obj) -> bool:
        return obj in self.backups

    def clear_backup(self, obj):
        obj_info = self.backups[obj]
        del self.backups[obj]
        obj_info.backup_file.unlink(missing_ok=True)
        self.add(obj, obj_info.backup_file, obj_info.collection, obj_info.format)

# maybe  better to import annotation from __future__ and write just list[Monitor] in MonitorInfo
# https://stackoverflow.com/questions/69802491/create-recursive-dataclass-with-self-referential-type-hints


class MonitorManager:
    @dataclass
    class MonitorInfo:
        timeout: int
        reindex_timeout: Optional[time.time]
        backup_file: Optional[pathlib.Path | str]
        coworkers: List[ChangesWatcher] = field(default_factory=list)
        last_reindex: Optional[time.time] = None
        last_run: Optional[time.time] = None

    def __init__(self, backups_dir: pathlib.Path = None, kraken: Kraken = None, exit_file=None):
        self.kraken = kraken
        self.monitors = {}
        self.backup_manager = BackupManager()
        self.backups_dir = pathlib.Path(backups_dir) \
            if backups_dir else get_module_dir().parent / 'backups'
        create_dirs(self.backups_dir)
        self._exit_file = exit_file
        if exit_file:
            open(exit_file, 'w').close()

    def add_monitor(self,
                    monitor: ChangesWatcher,
                    backup_file=None,
                    timeout: int = 10,
                    reindex_timeout: int = None) -> None:
        self.monitors[monitor] = self.MonitorInfo(timeout, reindex_timeout, backup_file)
        if backup_file:
            # prev_state type of monitor should be difined here
            backup_file = self.backups_dir / backup_file
            self.backup_manager.add(monitor, backup_file=backup_file,
                                    collection=type(monitor.prev_state))
            monitor.set_state(self.backup_manager.load(monitor))  # Loading backups to monitor

    def add_coworker(self, monitor: ChangesWatcher, coworker: ChangesWatcher):
        self.monitors[monitor].coworkers.append(coworker)

    def _time_to_rerun(self, info):
        if info.last_run:
            return time.time() - info.last_run > info.timeout
        return True

    def _time_to_reindex(self, info):
        if info.last_reindex:
            return time.time() - info.last_reindex > info.reindex_timeout
        return True

    def _time_to_exit(self):
        if self._exit_file:
            if os.stat(self._exit_file).st_size > 0:
                os.remove(self._exit_file)
                return True
        return False

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
        # It's running only on existing files
        coworkers_changes = Changes()
        for file in changes:
            file = pathlib.Path(file)
            if not file.is_dir():
                continue
            for coworker in coworkers:
                now = datetime.now().isoformat(' ', 'seconds')
                print(f'[{now}] Starting coworker {coworker} at root {file}')
                # JSON format hardcoded here. Fix is needed
                if not self.backup_manager.stores(coworker):
                    self.backup_manager.add(coworker,
                                            self.backups_dir / f'{coworker}.json',
                                            coworker.collection)
                backup = self.backup_manager.load(coworker)
                '''Here I need to get only root file's part from coworker's backup.
                If I write this:
                coworker.prev_state = coworker.collection(file: backup.set_default(file, {}))
                It will destroy the idea of another collections support. So I need a method
                cut_to_key() or something like that in each collection.
                '''
                # JSON backups for coworkers contain files structure for
                # top-level directories reported by the main monitor. This
                # structures nest under the full path of the main directory
                # which corresponds to the str(pathlib.Path.absolute())
                coworker.set_state(backup.cut_to_key(str(file.absolute())))
                coworker.set_root(file)
                coworker_changes = coworker.get_changes()
                if coworker_changes:
                    # Really bad naming, it's two different variables
                    coworkers_changes.extend(coworker_changes)
                    self._print_changes(coworker, coworker_changes)
                    self.backup_manager.update(coworker, coworker.prev_state)
                coworker.reset_state()
        return coworkers_changes

    def report_changes(self, changes):
        if self.kraken:
            self.kraken.release(FileChangesInfo(changes))

    def start(self):
        while not self._time_to_exit():
            time.sleep(1)  # It helps not to load full core
            for monitor, info in self.monitors.items():
                if self._time_to_rerun(info):
                    changes = monitor.get_changes()
                    if changes:
                        self._print_changes(monitor, changes)
                        self.backup_manager.save(monitor, monitor.prev_state)
                        if info.coworkers and changes.created:
                            coworkers_changes = self._run_coworkers(info.coworkers, changes.created)
                            changes.extend(coworkers_changes)
                        self.report_changes(changes)
                    else:
                        now = datetime.now().isoformat(' ', 'seconds')
                        print(f'[{now}] {monitor}: No changes')
                    info.last_run = time.time()
                if info.reindex_timeout:
                    if self._time_to_reindex(info) and info.coworkers:
                        print("Reindexing")
                        changes = self._run_coworkers(
                            info.coworkers, monitor.prev_state.to_list(**monitor._formatter_args))
                        if changes:
                            self.report_changes(changes)
                        info.last_reindex = time.time()
        now = datetime.now().isoformat(' ', 'seconds')
        print(f'[{now}] Finishing monitoring')
