import pathlib
import pytz
from abc import abstractmethod
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Optional

# TIMEZONE_UTC_OFFSET = timedelta(hours=3)
# TIMEZONE = timezone(TIMEZONE_UTC_OFFSET, name='MCK')

TIMEZONE = pytz.timezone('Europe/Moscow')

# Config will be somewhere here
RUNS_PATH = pathlib.Path(
            '/mnt/c/Users/misha/Desktop/materials/Programming/files-kraken/test')

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
            mtime = datetime.fromtimestamp(file.stat().st_ctime)

            return mtime.replace(microsecond=0, second=0)
        else:
            return None

class RunModifiedParser(DataParser):
    @staticmethod
    def parse(run: str) -> Optional[datetime]:
        run_path = RUNS_PATH / run
        return ModificationDateParser.parse(run_path)

def utc_to_local(utc_dt):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(TIMEZONE)
    return TIMEZONE.normalize(local_dt)

def snakedate_to_datetime(snakedate: str):
    # Snakedate has fromat like [Thu May  5 19:02:46 2022]
    snakedate = snakedate[1: -1]
    date = datetime.strptime(snakedate, '%a %b  %d %H:%M:%S %Y')
    return utc_to_local(date)

class RunStartParser(DataParser):
    @staticmethod
    def parse(run: str) -> Optional[datetime]:
        run_path = RUNS_PATH / run
        snakelog = run_path / 'nohup.out'
        if snakelog.exists():
            with open(snakelog, 'r') as f:
                lines = []
                for line in f:
                    line = line.strip()
                    if line.startswith('1 of '):
                        start = lines[-2]
                        return snakedate_to_datetime(start)
                    lines.append(line)

class RunEndParser(DataParser):
    @staticmethod
    def parse(run: str) -> Optional[datetime]:
        run_path = RUNS_PATH / run
        snakelog = run_path / 'nohup.out'
        if snakelog.exists():
            with open(snakelog, 'r') as f:
                lines = []
                for line in f:
                    line = line.strip()
                    if line.endswith('(100%) done'):
                        end = lines[-2]
                        return snakedate_to_datetime(end)
                    lines.append(line)

if __name__ == '__main__':
    mdparser = ModificationDateParser()
    f = pathlib.Path('backups/db.json')
    # mtime = mdparser.parse(f)
    # print(dir(mtime))
    # print(mtime)
    print(RunStartParser.parse('other_120'))
    print(RunEndParser.parse('other_120'))