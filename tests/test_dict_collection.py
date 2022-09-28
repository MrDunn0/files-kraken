import pytest
import pathlib
from src.collector import DictCollection


class TestDictCollection:

    @pytest.fixture
    def dc(self):
        '''Just simple structure without existing files'''
        return DictCollection({
                'dir1': {},
                'dir2': {
                    'file1': None
                },
                'dir3': {
                    'file2': None,
                    'dir4': {}
                }
            })

    @pytest.fixture
    def real_dc(self):
        '''Fixture with DictCollection of real existing paths'''
        return DictCollection({
                str(pathlib.Path('tests_data/collector_path')): {
                    'run_1': {
                        'input':{
                            'sample_1.fastq.gz': None
                        }},
                    'run_2': {
                        'bams': {
                            'sample_1.bam': None
                        }
                    },
                    'run_3': {}
                    
                }
            }
        )

    def test_extend_one(self, dc: DictCollection):
        # Idk how to manage with modification of dc fixture after calling .extend
        # So the best way I found is splitting asserts to several methods
        assert dc.extend({
            'dir1': {"file3"},
            'dir2': {
                'dir5': {'file4': None}
            },
            'dir4': {
                'file5': None,
                'dir6': {} }
            }) == {
            'dir1': {"file3"},
            'dir2': {
                'file1': None,
                'dir5': {'file4': None}
            },
            'dir3': {
                'file2': None,
                'dir4': {}
            },
            'dir4': {
                'file5': None,
                'dir6': {} }
        }

    def test_extend_two(self, dc: DictCollection):
        '''Testing that extension with empty dict causes no changes'''
        assert dc.extend({}) == {
                'dir1': {},
                'dir2': {
                    'file1': None
                },
                'dir3': {
                    'file2': None,
                    'dir4': {}
                }
            }

    def test_extend_three(self, dc: DictCollection):
        '''Testing no effect of extention with the same object'''
        assert dc.extend({
                'dir1': {},
                'dir2': {
                    'file1': None
                },
                'dir3': {
                    'file2': None,
                    'dir4': {}
                }
            }) == {
                'dir1': {},
                'dir2': {
                    'file1': None
                },
                'dir3': {
                    'file2': None,
                    'dir4': {}
                }
                }

    def test_to_list(self, real_dc: DictCollection):
        '''Test DictCollection.to_list() method'''

        # Relpath strings without empty dirs
        assert real_dc.to_list(
            keep_empty_dirs=False, to_pathlib=False) == [
                'tests_data/collector_path/run_1/input/sample_1.fastq.gz',
                'tests_data/collector_path/run_2/bams/sample_1.bam']

        # Relpathstrings with empty dirs
        assert real_dc.to_list(
            keep_empty_dirs=True, to_pathlib=False) == [
                'tests_data/collector_path/run_1/input/sample_1.fastq.gz',
                'tests_data/collector_path/run_2/bams/sample_1.bam',
                'tests_data/collector_path/run_3']

        # Pathlib PosixPath without empty dirs
        assert real_dc.to_list(
            keep_empty_dirs=False, to_pathlib=True) == [
                pathlib.Path('tests_data/collector_path/run_1/input/sample_1.fastq.gz'),
                pathlib.Path('tests_data/collector_path/run_2/bams/sample_1.bam')
            ]

        # Pathilb PosixPath with empty dirs
        assert real_dc.to_list(
            keep_empty_dirs=True, to_pathlib=True) == [
                pathlib.Path('tests_data/collector_path/run_1/input/sample_1.fastq.gz'),
                pathlib.Path('tests_data/collector_path/run_2/bams/sample_1.bam'),
                pathlib.Path('tests_data/collector_path/run_3')
            ]

        # Empty collection to_list
        assert DictCollection({}).to_list(keep_empty_dirs=False, to_pathlib=False) == []
        assert DictCollection({}).to_list(keep_empty_dirs=True, to_pathlib=False) == []
        assert DictCollection({}).to_list(keep_empty_dirs=False, to_pathlib=True) == []
        assert DictCollection({}).to_list(keep_empty_dirs=True, to_pathlib=True) == []

    def test_cut_to_key(self, real_dc: DictCollection):
        '''Test DictCollection.cut_to_key() method'''

        # Adapt real dc for tests
        real_dc = DictCollection(real_dc['tests_data/collector_path'])

        # Cut to nonempty dir
        assert real_dc.cut_to_key('run_1') == {'run_1': {
                                                    'input':{
                                                        'sample_1.fastq.gz': None
                                                    }}}

        # Cut to empty dir
        assert real_dc.cut_to_key('run_3') == {'run_3': {}}

        # Cut to single file
        assert DictCollection({'file1': None}).cut_to_key('file1') == {'file1': None}

        # Cut to nonexisting key

        assert real_dc.cut_to_key('run_77') == {}