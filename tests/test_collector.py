import pathlib
from src.files_kraken.collector._collector import SingleRootCollector
from src.files_kraken.retools import BoolOutputMultimatcher
from tests_data.collector_collections import (
    DEFAULT_MATCH_COLLECTION,
    COLLECTOR_ALL_FILES,
    MATCH_DIRS_COLLECTION,
    ZERO_DEPTH_COLLECTION,
    DATA_DEPTH_COLLECTION,
    MATCH_DIRS_NO_EMPTY_COLLECTION
)

TEST_DATA_ROOT = pathlib.Path('tests/tests_data/collector_path')
TEST_DATA_PATTERNS = [
    r'run_\d+', r'.+\.fastq.gz', r'.+\.bam',
    r'.+metrics.txt', r'.+results.txt', 'bams', 'input', 'results']


def create_BOM(*args, **kwargs):
    '''Creates BoolOutputMultimatcher from provided arguments'''
    return BoolOutputMultimatcher(*args, **kwargs)


test_matcher = create_BOM(TEST_DATA_PATTERNS)


def create_SRC(*args, **kwargs):
    '''Creates SingleRootCollector with provided arguments'''
    return SingleRootCollector(*args, **kwargs)


def test_collect_default():
    '''The default behavior implies file matching. Directories are not matched by default'''
    src = create_SRC(root=TEST_DATA_ROOT, matcher=test_matcher)
    collected = src.collect()
    assert collected == DEFAULT_MATCH_COLLECTION


def test_collect_no_match():
    '''Collects all files at root without matcher specified'''
    src = create_SRC(root=TEST_DATA_ROOT)
    collected = src.collect()
    assert collected == COLLECTOR_ALL_FILES


def test_collect_match_dirs():
    '''First match dirname, and only if matched, collect matched files inside'''
    src = create_SRC(root=TEST_DATA_ROOT, match_dirs=True, matcher=test_matcher)
    collected = src.collect()
    assert collected == MATCH_DIRS_COLLECTION


def test_zero_depth():
    '''Match/collect only files/dirs in the root directory'''
    src = create_SRC(root=TEST_DATA_ROOT, match_dirs=True, matcher=test_matcher, max_depth=0)
    collected = src.collect()
    assert collected == ZERO_DEPTH_COLLECTION


def test_data_depth():
    '''Nothing changes when we set max depth of tests data'''
    src = create_SRC(root=TEST_DATA_ROOT, match_dirs=True, matcher=test_matcher, max_depth=2)
    collected = src.collect()
    assert collected == DATA_DEPTH_COLLECTION


def test_larger_depth():
    '''Nothing changes with larger max_depth'''
    src = create_SRC(root=TEST_DATA_ROOT, max_depth=10)
    collected = src.collect()
    assert collected == COLLECTOR_ALL_FILES


def test_no_empty_dirs():
    '''Empty directories not included even if it matched'''
    src = create_SRC(
        root=TEST_DATA_ROOT, match_dirs=True,
        matcher=test_matcher, keep_empty_dirs=False)
    collected = src.collect()
    assert collected == MATCH_DIRS_NO_EMPTY_COLLECTION
