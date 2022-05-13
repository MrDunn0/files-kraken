from abc import abstractmethod
import pathlib
from datetime import datetime, timedelta, timezone
from typing import Optional

TIMEZONE_UTC_OFFSET = timedelta(hours=3)
TIMEZONE = timezone(TIMEZONE_UTC_OFFSET, name='MCK')

class DataParser:
    @staticmethod
    def parse(*args, **kwargs):
        pass


class ModificationDateParser(DataParser):
    # https://stackoverflow.com/questions/237079/how-do-i-get-file-creation-and-modification-date-times

    @staticmethod
    def parse(file: pathlib.Path) -> Optional[datetime]:
        print(file)
        if file.exists():
            print('File exists')
            mtime = datetime.fromtimestamp(file.stat().st_ctime, TIMEZONE)
            return mtime.replace(microsecond=0, second=0)
        else:
            return None


if __name__ == '__main__':
    mdparser = ModificationDateParser()
    f = pathlib.Path('backups/db.json')
    mtime = mdparser.parse(f)
    print(dir(mtime))
    print(mtime)