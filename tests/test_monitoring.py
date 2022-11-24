import pytest
import pyfakefs
import os
from tests_data.collector_collections import DEFAULT_MATCH_COLLECTION, FS_DEFAULT_MATCH_COLLECTION
from src.collector import DictCollection
from copy import deepcopy
from test_collector import create_SRC

@pytest.fixture()
def files_dc():
    return DictCollection(deepcopy(FS_DEFAULT_MATCH_COLLECTION))

@pytest.fixture()
def fake_filesystem(fs):
    yield fs

@pytest.fixture()
def fs_files_from_dict(fs, files_dc: dict[str, str | None]):
    # There could be some problems with infinite loop according to this thread
    # https://stackoverflow.com/questions/18011902/pass-a-parameter-to-a-fixture-function
    stack = list(files_dc.items())
    while stack:
        file, content = stack.pop()
        if isinstance(content, dict):
            fs.create_dir(file)
            stack.extend(
                {
                f'{file}{os.sep}{subdir}': items
                for subdir, items in content.items()
                }.items())
        else:
            fs.create_file(file)

# class TestSomething:
#     def test_something(self, files_dc):
#         files_dc['tests/tests_data/collector_path']['run_300'] = 'NOT FOUND'
#         assert 'run_1' in files_dc['tests/tests_data/collector_path']

def test_fs(fs, fs_files_from_dict):
    src = create_SRC(root='/fs/tests_data/collector_path')
    print(src.collect())
    
    # assert os.path.exists('/fs/abo')