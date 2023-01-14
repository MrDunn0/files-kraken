import pathlib
import os
from dataclasses import dataclass
from random import randint
from shutil import rmtree

from files_kraken.blueprint import DataBlueprint
from files_kraken.collector import SingleRootCollector
from files_kraken.initializer import Workflow
from files_kraken.fields import ParserField, DataParser
from files_kraken.monitoring import MonitorManager, ChangesWatcher
from files_kraken.retools import BoolOutputMultimatcher


EXAMPLE_DIR = pathlib.Path('advanced_workflow_data')
RAW_DATA_DIR = EXAMPLE_DIR / 'raw_data'
RESULTS_DIR = EXAMPLE_DIR / 'results'


def create_example_data():
    '''
    Create 20 run directories with raw data file for 3 random samples
    and create separate results directory with results for every sample in
    every run.
    '''

    rmtree(EXAMPLE_DIR, ignore_errors=True)

    os.makedirs(RAW_DATA_DIR)
    os.mkdir(RESULTS_DIR)

    for run in range(1, 21):
        run_dir = RAW_DATA_DIR / f'run_{run}'
        os.mkdir(run_dir)
        run_samples = [str(randint(1, 1000)) for _ in range(3)]
        raw_samples_data = [
            run_dir / f'run_{run}.sample_{sample}.raw_data.data'
            for sample in run_samples
        ]
        results = [
            RESULTS_DIR / f'run_{run}.sample_{sample}.results.txt'
            for sample in run_samples
        ]

        for raw_data in raw_samples_data:
            open(raw_data, 'w').close()
        for result in results:
            with open(result, 'w') as f:
                metric = str(randint(1000, 2000))
                f.write(metric)


create_example_data()


# Our metric parser from easy example

class MyMetricParser(DataParser):
    def parse(file):
        with open(file) as f:
            value = float(f.read().strip())
            return value


# Now we have two directories:
# 1. Directory with raw data for 20 runs with 3 samples in each.
#       raw data file format: run_[0-9]+.sample_[0-9]+.raw_data.data
# 2. Directory with results for every samlpe in every run.
#     results file format: run_[0-9]+.sample_[0-9]+.results.txt
# Each results file contains a single metric value


# Let's say you want to collect info about sample metric in each run
# In that case you'll need some scheme like this:


@dataclass
class SampleRunInfo(DataBlueprint):
    sample: str
    run: str
    results_file: pathlib.Path = None
    metric: ParserField = ParserField(
        'metric', MyMetricParser, dependent_fields=['results_file'])
    required_fields = {
        'sample': (r'sample_([0-9]+)', 1),
        'run': (r'run_[0-9]+', 0)
    }

    def __post_init__(self):
        self.match_scheme = {
            'results_file': fr'{self.run}.sample_{self.sample}.results.txt'
        }
        return super().__post_init__()


# Now suppose that after creating the scheme you realize that you want to
# collect informations only on runs starting from 10th. Of course you can
# modify your regular expression, but imagine that you can't :)

# Create raw data collector object

raw_data_collector = SingleRootCollector(
    root=RAW_DATA_DIR,
    match_dirs=False,  # Default
    max_depth=None,  # Default
    keep_empty_dirs=True,  # Default
)

# Excellent, this collector is an exact copy of the collector from easy example
# And it will gather all files in provied directory. To avoid this we can
# create a matcher object and set it to collector's matcher attribute.

# Here we don't need such scheme like DataBlueprint for DB. You need to specify
# just a list of regular expressions.

raw_data_patterns = [(r'run_[1-9][0-9]+', 0)]

runs_matcher = BoolOutputMultimatcher(
    patterns=raw_data_patterns,
)

# Of course you could do it at the collector initialization stage
raw_data_collector.matcher = runs_matcher


# If you run next line you will see that all runs up to the 10th number
# have no files. In this case you can exclude these empty directories by
# two ways:
# 1. Set collector's match_dirse=False
# 2. Set collector's keep_empty_dirs=False

# print(raw_data_collector.collect())

# To collect results files with same runs we need another collector
# because it can have only one root. But both can have the same matcher.

results_collector = SingleRootCollector(
    root=RESULTS_DIR,
    matcher=runs_matcher
)

# print(results_collector.collect())


# Next we need to create watcher objects, that
# will be collecting changes in our directories.

raw_data_watcher = ChangesWatcher(collector=raw_data_collector)
results_watcher = ChangesWatcher(collector=results_collector)

# Then we need to create MonitorManager, which can contain and
# manage any number of ChangesWatcher.

monitor_manager = MonitorManager()

# If you want to keep your monitors state between runs
# you need to specify monitor backup file.

monitor_manager.add_monitor(
    raw_data_watcher,
    backup_file='raw_data_watcher_backup.json')

monitor_manager.add_monitor(
    results_watcher,
    backup_file='results_watcher_backup.json')

# This completes the basic preparations and now we can
# finally create Workflow object
# You can specify a directory where to place "workflow_data"
# directory with DB files and monitor_manager backups
# MonitorManager backups_dir can be specified directly in object


wf = Workflow(
    name='sample_info',
    monitor_manager=monitor_manager,
    schemes=[SampleRunInfo],
    wf_dir='./',  # Default
    exit_time=12
)

# Now let's run our Workflow!

wf.run()
# Collected scheme data is here:
# workflow_data/sample_info/sample_info_db.json
