from dataclasses import dataclass
from typing import NamedTuple

# FilesKraken modules


@dataclass
class KrakenInfo:
    pass

@dataclass
class FileChangesInfo(KrakenInfo):
    changes: NamedTuple


'''
It's a bad way to add sender to info and I didn't want it.
But I don't want to write different info for the same data
in order to ensure the reciever is uniquely identified.

Now classes that handle info with sender must check it.
'''
