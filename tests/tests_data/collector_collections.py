import pathlib
COLLECTOR_ALL_FILES = {
    str(pathlib.Path('tests/tests_data/collector_path').absolute()): {
        'run1': {
                'copy_of_run_1.txt': None,
                'bams': {'sample_1.bai': None, 'sample_1.bam': None},
                'input': {'sample_1.fastq.gz': None},
                'results': {'run_1.metrics.txt': None, 'sample_1.results.txt': None},
                'useless_dir': {'useless_file.txt': None}
            },
        'run_1': {
            'bams': {'sample_1.bai': None, 'sample_1.bam': None},
            'input': {'sample_1.fastq.gz': None},
            'results': {'run_1.metrics.txt': None, 'sample_1.results.txt': None},
            'useless_dir': {'useless_file.txt': None}
        },
        'run_2': {
            'bams': {'sample_2.bai': None, 'sample_2.bam': None, 'sample_3.bai': None, 'sample_3.bam': None},
            'input': {'sample_2.fastq.gz': None, 'sample_3.fastq.gz': None},
            'results': {'sample_2.metrics.txt': None, 'sample_2.results.txt': None, 'sample_3.metrics.txt': None, 'sample_3.results.txt': None},
            },
        'run_3': {'empty_run.txt': None}
    }
}


DEFAULT_MATCH_COLLECTION = {
    str(pathlib.Path('tests/tests_data/collector_path').absolute()): {
        'run1': {
                'bams': {'sample_1.bam': None},
                'input': {'sample_1.fastq.gz': None},
                'results': {'run_1.metrics.txt': None, 'sample_1.results.txt': None},
                'useless_dir': {}
        },
        'run_1': {
            'bams': {'sample_1.bam': None},
            'input': {'sample_1.fastq.gz': None},
            'results': {'run_1.metrics.txt': None, 'sample_1.results.txt': None},
            'useless_dir': {}
        },
        'run_2': {
            'bams': {'sample_2.bam': None, 'sample_3.bam': None},
            'input': {'sample_2.fastq.gz': None, 'sample_3.fastq.gz': None},
            'results': {'sample_2.metrics.txt': None, 'sample_2.results.txt': None, 'sample_3.metrics.txt': None, 'sample_3.results.txt': None},
        },
        'run_3': {}
        }
}

MATCH_DIRS_COLLECTION = {
    str(pathlib.Path('tests/tests_data/collector_path').absolute()): {
        'run_1': {
            'bams': {'sample_1.bam': None},
            'input': {'sample_1.fastq.gz': None},
            'results': {'run_1.metrics.txt': None, 'sample_1.results.txt': None},
        },
        'run_2': {
            'bams': {'sample_2.bam': None, 'sample_3.bam': None},
            'input': {'sample_2.fastq.gz': None, 'sample_3.fastq.gz': None},
            'results': {'sample_2.metrics.txt': None, 'sample_2.results.txt': None, 'sample_3.metrics.txt': None, 'sample_3.results.txt': None},
        },
        'run_3': {}
        }
}


ZERO_DEPTH_COLLECTION = {
    str(pathlib.Path('tests/tests_data/collector_path').absolute()): {
        'run_1': {}, 'run_2': {}, 'run_3': {}
    }
}


DATA_DEPTH_COLLECTION = MATCH_DIRS_COLLECTION


MATCH_DIRS_NO_EMPTY_COLLECTION = {
    str(pathlib.Path('tests/tests_data/collector_path').absolute()): {
        'run_1': {
            'bams': {'sample_1.bam': None},
            'input': {'sample_1.fastq.gz': None},
            'results': {'run_1.metrics.txt': None, 'sample_1.results.txt': None},
        },
        'run_2': {
            'bams': {'sample_2.bam': None, 'sample_3.bam': None},
            'input': {'sample_2.fastq.gz': None, 'sample_3.fastq.gz': None},
            'results': {'sample_2.metrics.txt': None, 'sample_2.results.txt': None, 'sample_3.metrics.txt': None, 'sample_3.results.txt': None},
        },
        }
}

# Fake Filesystem collections

FS_DEFAULT_MATCH_COLLECTION = {
    '/fs/tests_data/collector_path': {
        'run1': {
                'bams': {'sample_1.bam': None},
                'input': {'sample_1.fastq.gz': None},
                'results': {'run_1.metrics.txt': None, 'sample_1.results.txt': None},
                'useless_dir': {}
        },
        'run_1': {
            'bams': {'sample_1.bam': None},
            'input': {'sample_1.fastq.gz': None},
            'results': {'run_1.metrics.txt': None, 'sample_1.results.txt': None},
            'useless_dir': {}
        },
        'run_2': {
            'bams': {'sample_2.bam': None, 'sample_3.bam': None},
            'input': {'sample_2.fastq.gz': None, 'sample_3.fastq.gz': None},
            'results': {'sample_2.metrics.txt': None, 'sample_2.results.txt': None, 'sample_3.metrics.txt': None, 'sample_3.results.txt': None},
        },
        'run_3': {}
        }
}

FS_MODIFIED_COLLECTION = {
    '/fs/tests_data/collector_path/run_1': {
        'input': {'sample_1.fastq.gz': None},
        'results': {'run_1.metrics.txt': None, 'sample_1.results.txt': None}
    },
    '/fs/tests_data/collector_path/run_5': {'results': {'sample_12.results.txt': None}}
}
