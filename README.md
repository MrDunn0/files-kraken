# FilesKraken
FilesKraken is a simple app that allows you to  monitor file system changes and automatically  parse files into the database according to user-created schemes.
# Quick Access

[Python/OS](#python-version-and-os)

[Basic Usage](#basic-usage)

[Advanced Usage](#advanced-usage)

[Collected data](#how-to-get-collected-data)

[Contact](#contact)

# Description

FilesKraken is a set of python modules linked by the idea of quickly parsing organized file structures into given database structures. The tool can run in the background and automatically detect changes in the file system. However, some modules may be useful on their own, although they most likely have more advanced counterparts.

**Most suitable use case**: You have many directories with **similarly structured information**, and you want to collect (also in real time) a specific set of data for each directory.

**Limitation**: The key functinality of the tool is file-names pattern matching and "similarly structured information" means that your file names must have some parts to be used as unique identifiers.

It's important to note that this is primarily a training project, so don't expect too much from it.

# Python version and OS

FilesKraken requires Python 3.10+
Tested on Mac 10.14.6 and  Ubuntu 18.04.3

# Basic Usage

## Data Scheme creation

The idea is same as Pydantic or Django models: You create some schemes with named fields of fixed type. Then these schemes will be filled and sent to the database.

Let's look at an example from here.

```python
import pathlib
from dataclasses import dataclass
from files_kraken.blueprint import DataBlueprint
from  files_kraken.fields import ParserField


@dataclass
class MyScheme(DataBlueprint):
    project: str
	results_file: pathlib.Path = None
	metric: ParserField = ParserField(
        name='metric',
		parser=MyMetricParser,
		dependent_fields=['results_file']
	)
	# required_fields must be specified
	required_fields = {
	    'project': (r'project_[0-9]+', 0)
	}
	# You have to create __post_init__ in each of your schemes
	def __post_init__(self):
      # You have to provide match_scheme if there are some
	  # non-required fields in your scheme.
	  self.match_scheme = {
	      'results_file': fr'{self.project}_results.txt'
	  }
	  super().__post_init__()  # Also required
```

As you can see, everything is, to put it mildly, a bit complicated. But let's try to figure this out.

Here we have the scheme with 3 fields: `project`, `results_file` and `metric`.  The `project` field is a required field. Each required field  must be of `str` type and not have default value. `results_file`  has type `pathlib.Path`, which means that corresponding field of intermediate scheme will be of this type  and it will be stored as a full path string in the DB. The last `metric` field is a `ParserField`. This is the only custom field type in the package at the moment. As you can see `results_file` is provided as a dependent_field for `metric`. This means that `metric` will be parsed only after the value for the `results_file`  appears in the scheme. Your parser must have a method `parse()` that accepts arguments in the order specified in `dependent_fields`.

[Regular expression formats](#regular-expression-formats)
[Supported field types](#supported-scheme-field-types)

```python
from files_kraken.fields import DataParser

class MyMetricParser(DataParser):
    def parse(file):
        with open(file) as f:
            value = float(f.read().strip())
            return value
```

In `required_fields`  you specify  required fields and provide regular expressions for them. Other fields regular expressions must be specified in `__post_init__` method in `self.math_scheme` attribute. As you can see there, it's possible to use  required fields as a part of a regular expression. This will ensure that only the necessary files get into the scheme.

## Create and run workflow

This is the most simplest workflow configuration option:

```python
from files_kraken.initializer import Workflow

wf = Workflow(
    name='kraken_workflow',
	collector_path='./my_files_path',
	schemes=[MyScheme],
	exit_time=3
)
wf.run()
```

Here we create `Workflow` object, where we set our target files directory and schemes. `exit_time=3` means that this workflow will exit in 3 seconds. But first it will process all files at `./my_files_path`. If there are a lot of files, it will take longer than 3 seconds.

## Regular expression formats

There are three ways you can specify regular expressions for a field:

1. pattern raw string
2. tuple of str and matching group
3. tuple with combination of previous two

Let's consider the following scheme:

```python
from files_kraken.blueprint import DataBlueprint

@dataclass
class SampleInfo(DataBlueprint):
   run: str
   sample: str
   required_fields = {
       sample: (
	         (r'sample_([^\.]+)', 1),
			 (r'(\w+)_metrics.csv', 1)
	   )
	     run: (r'^(run_[0-9]+)', 1)
   }
```

Here `sample` field has two regular expressions. If the first one didn't match the second one will be checked. An Integer after regex in a tuple  means that we need the corresponding match group as the value of the field. Thus in both these files the sample will be matched as "MYSAMPLE": *run_100.sample_MYSAMPLE.results.txt*, *MYSAMPLE_metrics.tsv*

In case of raw string as a pattern `re.fullmatch`  is used for pattern matching. So it's your choice if you need to match full file basename. No matchings is done on the path to the file.

The path to the file is not matched for any type of regular expression for any field type.

## Supported scheme field types

There are five types supported for `DataBlueprint` fields.

- str
- pathlib.Path
- List[str]
- List[pathlib.Path]
- ParserField

List here is `typing.List` 
All types except `ParserField` are used to work with file names.  `str` type is used to create scheme identifiers. It can contain the full name of the file, or part of it. All fields could be passed to the `ParserField`. You can see the main differences in the section below.

## Fields Behavior

The table below shows the behavior of different field types. Unfortunately, the user can't change any of this in the current version of the package.

|Action/Behavior|str|pathlib.Path|List[str]|List[pathlib.Path]|ParserField|
|-
|File deleted|Set to None||Remove from list||No action|
|DB value|str|Full path str| List of str| List of full path str|Any supported by db|

As you can see, all types except  `ParserField` will be removed from the DB if their respective files are deleted.

# Advanced Usage
## Collector configuration

In the [Basic Usage](#basic-usage) section we didn't create the collector object, but it may be needed for more specific tasks. This option might be useful if you don't want to match every file against your schemes.The collector class has several configuration options that can help with that:

```python
from files_kraken.collector import SingleRootCollector

SingleRootCollector(
	 root,  # Required
	 matcher = None,
	 match_dirs = False,
	 keep_empty_dirs = True,
	 max_depth = None
)
```python

`root`  is path to your data directory, it's the only required argument.
`matcher` - matcher object. Actually it could be any object with `match()`, method, which returns `bool`. You can use the package's built-in class `BoolOutputMultimatcher`. 
`match_dirs` - defines whether to use matcher on directories.
`keep_empty_dirs` - understandable by name, but let me clarify that a directory could become empty during the matching.
`max_depth` - depth of files gathering. Zero depth is the level of your `root` directory.

Let's say you have some directory with `run_[0-9]+` directories with raw data for each run. Detailed workflow explanation can found here. But you want to collect only runs starting from 10th. Then your collector will need a matcher object. It can be created like this:

```python
from files_kraken.retools import BoolOutputMultimatcher
from files_kraken.collector import SingleRootCollector

RAW_DATA_ROOT = './raw_data'

patterns = [
    (r'run_[1-9][0-9]+', 0)
]
raw_data_matcher = BoolOutputMultimatcher(
   patterns=patterns,
   mode="any"  # Default
)
raw_data_collector = SingleRootCollector(
		RAW_DATA_ROOT,
		matcher=raw_data_matcher)
```

There is `mode`  option in matcher class that could have two values: **any** and **cons**. **any** means that `True` returns if any of provided patterns was matched. **cons** returns `True` only if **all** patterns are matched.

## Monitor manager

If you create multiple collectors or you want custom collector, you will have to crete the MonitorManager object for the Workflow yourself.

But first we need to create an object that directly watches for changes in the directory.  This is an object of class `ChangesWatcher`:

```python
from files_kraken.monitoring import ChangesWatcher

raw_data_cw = ChangesWatcher(
    collector=raw_data_collector,
	name=None,  # Default,
	keep_empty_dirs=True  # If you want to keep empty dirs
)
```

Then we need to register our watcher in monitor manager

```python
from files_kraken.monitoring import MonitorManager

monitor_manager = MonitorManager()
monitor_manager.add_monitor(
    raw_data_cw,
	backup_file='raw_data_watcher_backup.json'
)
```

You can specify `MonitorManager.backups_dir`, where the state of watchers will be stored. Default directory is `./backups` or in case of `Workflow` object `./workflow_data/workflow_name/monitor_backups` .

To add your monitor manager to workflow just do this:

```python
from files_kraken.initializer import Workflow

wf = Workflow(
    name='MyWorkflow',
	schemes=[MyScheme],
	wf_dir='./',  # Default
	monitor_manager=monitor_manager
)

```


## Custom Database and serialization

At the moment FilesKraken supports the only one database - [TinyDB](https://github.com/msiemens/tinydb).  If you want to embed your DB in workflow, you need to create respective database class. To do this, you need to inherit from `files_kraken.database.Database` object:

```python
from files_kraken.database import Database, DatabaseManager

class MyCustomDatabase(Database):
    def __init__(self, ...):
	       pass
		def add_blueprint(self, blueprint):
		    pass
		def get_blueprint(self, name, id):
		    pass
		def update_blueprint(self, name, id, updates):
		    pass

my_db = MyCustomDatabase(...)
db_manager = DatabaseManager(my_db)

wf = Workflow(
   name='my_workflow',
   monitor_manager=mm,
   db_manager=db_manager,
   schemes=[Scheme1, Scheme2]
)
```

In case you use TinyDB and your parser fields have some non-default types, you can write serializers for them as described [here](https://pypi.org/project/tinydb-serialization/). To add your serialization to Workflow you need to create database as in example above, and then create database manager and set it to the workflow object.

# How to get collected data

Collected data in JSON format is stored by default in a `{workflow_name}_db.json` file in the workflow directory. To access it you can use TinyDB, or any JSON parser. Also you can use workflow object directly to access database manager API.

```python
wf.db_manager.get_blueprint(
   name=your_scheme_name,
   id=object_id
)
wf.db_manager.update_blueprint(
    name=your_scheme_name,
	id=object_id,
	updates = None  # Dict of object fields
)

wf.db_manager.remove_blueprint(
    name=your_scheme_name,
	id=object_id
)
wf.db_manager.get_all()  # Get full database

```

Scheme object id in FilesKraken is the combination of required fields joined by two underscores in `scheme.required_fields` order.

# Contact

etozhe.musha@gmail.com


