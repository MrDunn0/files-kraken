# In this easy example you only need:
# 1. Create a data scheme, which you want to store in DB
# 2. Write parser for a metric
# 3. Create simple Workflow object and run it

import pathlib
import os
from dataclasses import dataclass

from files_kraken.blueprint import DataBlueprint
from files_kraken.fields import ParserField, DataParser
from files_kraken.initializer import Workflow


def create_example_data():
    dirs = [
        'easy_example_workflow',
        'easy_example_workflow/project_1',
        'easy_example_workflow/project_2',
        'easy_example_workflow/project_3'
        ]

    metric_files = {
        'easy_example_workflow/project_1/project_1_results.txt': '100',
        'easy_example_workflow/project_2/project_2_results.txt': '200',
        'easy_example_workflow/project_3/project_3_results.txt': '300'
    }

    for dir in dirs:
        os.makedirs(dir, exist_ok=True)
    for file, metric in metric_files.items():
        with open(file, 'w') as f:
            f.write(metric)


class MyMetricParser(DataParser):
    def parse(file):
        with open(file) as f:
            value = float(f.read().strip())
            return value

# Each scheme must be a dataclass inherited from DataBlueprint
# Required fields must have no default value


@dataclass
class MyScheme(DataBlueprint):
    project: str
    results_file: pathlib.Path = None
    metric: ParserField = ParserField(
        name='metric',
        parser=MyMetricParser,
        dependent_fields=['results_file'])

    # required_fields must be specified
    required_fields = {
        'project': (r'project_[0-9]+', 0)
    }

    # You have to create __post_init__ in each of your schemes
    def __post_init__(self):
        #  You need to provide match_scheme if there are some
        # non-required fields in your scheme.
        self.match_scheme = {
            'results_file': fr'{self.project}_results.txt'
        }
        super().__post_init__()  # Also required


wf = Workflow(
    'easy_workflow',  # Workflow name
    collector_path='./easy_example_workflow',  # Path to the target data
    schemes=[MyScheme],  # Your scheme
    exit_time=3,  # Time to exit in seconds
    )
create_example_data()
wf.run()

# Workflow data is stored in {your_current_dir}/workflow_data/easy_workflow
# DB with data schemes file basename is 'kraken_workflow_db.json'
