from dataclasses import dataclass
from typing import List, Any, NamedTuple

# FilesKraken modules
from blueprints import DataBlueprint

@dataclass
class KrakenInfo:
    pass

@dataclass
class FileChangesInfo(KrakenInfo):
    changes: NamedTuple

@dataclass
class CreatedFilesOrganizedInfo(KrakenInfo):
    structures: List[DataBlueprint]

@dataclass
class DeletedFilesOrganizedInfo(KrakenInfo):
    structures: List[DataBlueprint]


'''
It's a bad way to add sender to info and I didn't want it.
But I don't want to write different info for the same data
in order to ensure the reciever is uniquely identified.

Now classes that handle info with sender must check it.
'''


