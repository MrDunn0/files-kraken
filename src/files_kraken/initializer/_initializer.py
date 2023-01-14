from dataclasses import dataclass, field
import pathlib

# FilesKraken imports
from blueprint import DataBlueprint
from collector import SingleRootCollector
from database import Database, JsonDatabse, DatabaseManager
from data_organizer import BlueprintBuilder, BlueprintsDBUpdater
from exceptions import InitializationError
from functions import create_dirs
from krakens_nest import Kraken
from monitoring import MonitorManager, ChangesWatcher


@dataclass
class Workflow:
    name: str
    monitor_manager: MonitorManager = None
    wf_dir: str | pathlib.Path = './'
    db_path: str | pathlib.Path = None
    db: Database = None
    schemes: list[DataBlueprint] = field(default_factory=list)
    collector_path: str | pathlib.Path = None
    exit_time: int = None
    db_manager: DatabaseManager = None
    db_updater: BlueprintsDBUpdater = None
    bp_builder: BlueprintBuilder = None
    kraken: Kraken = Kraken()

    def __post_init__(self):
        self.wf_dir = pathlib.Path(self.wf_dir) / 'workflow_data' / self.name

        create_dirs(self.wf_dir)

        # Maybe it's a bad idea to simplify initialization
        # by the cost of readability and class __init__ overloading.

        if not self.monitor_manager:
            self.monitor_manager = MonitorManager(backups_dir=self.wf_dir / 'monitor_backups')
            if not self.collector_path:
                raise InitializationError(
                    'No collector path provided when initializing without MonitorManager specified'
                )
            else:
                collector = SingleRootCollector(self.collector_path)
                monitor = ChangesWatcher(collector, name='DefaultMonitor')
                monitor_backup_file = str(monitor) + '.json'
                self.monitor_manager.add_monitor(monitor, backup_file=monitor_backup_file)

        if not self.monitor_manager.backups_dir or \
                self.monitor_manager.backups_dir.name == 'backups':
            self.monitor_manager.backups_dir = self.wf_dir / 'monitor_backups'

        if not self.db:
            if not self.db_path:
                self.db_path = self.wf_dir / f'{self.name}_db.json'
            self.db = JsonDatabse(self.db_path)

        if not self.db_manager:
            self.db_manager = DatabaseManager(self.db)

        if not self.db_updater:
            self.db_updater = BlueprintsDBUpdater(self.db_manager)

        if not self.bp_builder:
            self.bp_builder = BlueprintBuilder(self.db_manager, self.db_updater)

        # All main components are set
        # Bind files monitor and blueprint builder with kraken
        self.monitor_manager.kraken = self.kraken
        self.bp_builder.set_kraken(self.kraken)

        # Register schemes in blueprint builder
        for scheme in self.schemes:
            self.bp_builder.register_blueprint(scheme)
        # Set exit time if specified
        if self.exit_time:
            self.monitor_manager.exit_time = self.exit_time

    def run(self):
        # Check that all key components are set
        for monitor in self.monitor_manager.monitors:
            if not monitor.collector:
                raise InitializationError('No collector provided for monitor {monitor}')
        if not self.bp_builder.blueprints:
            raise InitializationError('No data schemes provided for workflow}')
        self.monitor_manager.start()


__all__ = [
    'Workflow'
]
