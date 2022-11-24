from src.retools import ReExecutor, SchemeMatcher, ReSorter, GroupSearcher
from test_collector import  test_matcher

# ReExecutor.fullmatch


class TestReExecutor:
    rex_pattern = r'run_\d+'
    rex_ptrn_with_group = r'run_(\d+)'

    def test_fullmatch_matched(self):
        assert ReExecutor.fullmatch(self.rex_pattern, 'run_1') == 'run_1'

    def test_fullmatch_not_matched(self):
        assert ReExecutor.fullmatch(self.rex_pattern, 'text_run_1') is None

    def test_fullmatch_group(self):
        assert ReExecutor.fullmatch(self.rex_ptrn_with_group, 'run_123', 1) == '123'

    def test_fullmatch_group_not_matched(self):
        assert ReExecutor.fullmatch(self.rex_ptrn_with_group, 'run123', 1) is None

    # ReExecutor.search

    def test_search_matched(self):
        assert ReExecutor.search(self.rex_pattern, 'some_text_run_123') == 'run_123'

    def test_search_not_matched(self):
        assert ReExecutor.search(self.rex_pattern, 'some_text_run123') is None

    def test_search_with_group(self):
        assert ReExecutor.search(self.rex_ptrn_with_group, 'some_test_run_333', 1) == '333'

    def test_search_group_not_matched(self):
        assert ReExecutor.search(self.rex_ptrn_with_group, 'some_test_run333', 1) is None

    def test_findall(self):
        assert ReExecutor.findall(self.rex_pattern, 'run_1_run2_run_3') == ['run_1', 'run_3'] 


# BoolOutputMultimatcher


def test_BOM():
    '''Check that pattern matching works as intended'''
    entries = [
        'run_1', 'sample_1.bam', 'sample_1.fastq.gz',
        'run_1.metrics.txt', 'sample_1.results.txt']
    matches = [test_matcher.match(entry) for entry in entries]
    assert all(matches)
    assert len(matches) == 5


def test_BOM_no_match():
    '''Test no match where it mustn't be'''
    entries = ['run1', 'sample_1.bamm', 'sample_1.fastq', 'run_1.metrics', 'sample_1.results']
    matches = [test_matcher.match(entry) for entry in entries]
    assert not any(matches)
    assert len(matches) == 5


# ReSorter

def test_ReSorter():
    searcher = GroupSearcher(r'run_(\d+)', 1)
    sorter = ReSorter(searcher=searcher, func=int)
    entries = ['run_2', 'run_1', 'run_4', 'run_3', 'run_5']

    assert [1, 2, 3] != [2, 1, 3]
    assert sorter.sort(entries) == ['run_1', 'run_2', 'run_3', 'run_4', 'run_5']


# SchemeMatcher

class TestSchemeMatcher:
    scheme = {
        'run': r'run_\d+',
        'fastq': r'.+\.fastq.gz',
        'bam': r'.+\.bam',
        'sample': (r'.+sample_([^\.]+)', 1)
    }

    scheme_matcher = SchemeMatcher(scheme)

    def test_scheme_matcher(self):

        # Full string match
        assert self.scheme_matcher.match('run_111') == {'run': 'run_111'}
        # Full string + partial match
        assert self.scheme_matcher.match('run_111.sample_BR616.fastq.gz') == {
            'sample': 'BR616', 'fastq': 'run_111.sample_BR616.fastq.gz'}
        # No match
        assert self.scheme_matcher.match('test.sample-BR616.bai') == {}
