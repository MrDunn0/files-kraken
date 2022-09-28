from dataclasses import dataclass
import pathlib


from krakens_nest import Kraken
from monitoring import MonitorManager
from database import Database, JsonDatabse, serialization, DatabaseManager
from data_organizer import BlueprintBuilder, BlueprintsDBUpdater
from functions import create_dirs


APP_DIR = pathlib.Path(__file__).parent.absolute() # change to parent.parent when moved to modules dir


@dataclass
class Workflow:
    name: str
    monitor_manager: MonitorManager
    db_path: str | pathlib.Path = None
    db: Database = None
    kraken: Kraken = Kraken()
    db_manager: DatabaseManager = None
    db_updater: BlueprintsDBUpdater = None
    bp_builder: BlueprintBuilder = None

    def __post_init__(self):
        self.wf_dir = APP_DIR / 'workflow_data' / self.name

        if not self.monitor_manager.backups_dir:
            self.monitor_manager.backups_dir = self.wf_dir

        if not self.db:
            if not self.db_path:
                self.db_path = self.wf_dir / 'db.json'
            self.db = JsonDatabse(self.db_path, serialization=serialization)
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.db)
        
        if not self.db_updater:
            self.db_updater = BlueprintsDBUpdater(self.db_manager)
        
        if not self.bp_builder:
            self.bp_builder = BlueprintBuilder(self.kraken, self.db_manager, self.db_updater)
        create_dirs(self.wf_dir, self.db_path)
