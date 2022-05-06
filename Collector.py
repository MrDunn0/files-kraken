import os
import pathlib
from abc import ABC, abstractmethod


from retools import BoolOutputMultimatcher

class FilesCollection(ABC):
    pass


class DictCollection(FilesCollection, dict):
    # я не знаю, какие методы тут нужны, и что бы хотелось инициализировать
    # пока что никаких дополнительных атрибутов мне не нужно, хочу метод extend()
    def extend(self, other):
        for o, o_items in other.items():
            if o in self and o_items:
                if not self[o]:
                    self[o] = o_items
                else:
                    self[o] = DictCollection.extend(self[o], o_items)
            else:
                self[o] = o_items
        return self

    @staticmethod
    def _to_list(dc, keep_empty_dirs=False, to_pathlib=True):
        list_out = []
        for key, value in dc.items():
            if isinstance(value, dict):
                if keep_empty_dirs and not value:
                    list_out.append(key)
                else:
                    list_out.extend(
                        [f'{key}{os.sep}{el}'
                            for el in DictCollection._to_list(
                                value,
                                keep_empty_dirs=keep_empty_dirs,
                                to_pathlib=to_pathlib)
                        ]
                    )
            elif value is None:
                list_out.append(key)
            else:
                raise ValueError(f'Wrong value type for DictCollection: {type(value)}')
        return [pathlib.Path(el) for el in list_out] if to_pathlib else list_out

    def to_list(self, keep_empty_dirs=False, to_pathlib=False, **kwargs):
        return self._to_list(self, keep_empty_dirs=keep_empty_dirs, to_pathlib=to_pathlib)

    def cut_to_key(self, key):
        if key in self:
            return DictCollection({key: self.get(key)})
        else:
            return DictCollection()


class FilesCollectionBuilder:
    def __init__(self):
        self.collection = FilesCollection()

    def build(self):
        return self.collection


class FilesCollector(ABC):
    @abstractmethod
    def collect(self):
        pass


class SingleRootCollector(FilesCollector):
    def __init__(self, root, matcher=None, output_format=DictCollection,
                match_dirs=None, max_depth=None, keep_empty_dirs=True):

        self.root = pathlib.Path(root) if root else root
        self.matcher = matcher
        self.max_depth = max_depth
        self.match_dirs = match_dirs
        self.keep_empty_dirs = keep_empty_dirs
        self.output_format = output_format

    def collect(self, root=None, cur_depth=0):
        collection = self.output_format()
        # This block is only for keeping root absolute name at the top of collection
        # I guess it cab be reorganized in some better way
        if not root:
            if self.root is None:
                return collection   # prevents errors when method was called without root set
            root = self.root
            collection[str(root)] = self.collect(root=root, cur_depth=0)
            return self.output_format(collection)

        if self.max_depth is not None and cur_depth > self.max_depth:
            return self.output_format()

        for file in root.iterdir():
            if file.is_dir():
                if self.matcher:
                    if self.match_dirs and not self.matcher.match(file.name):
                        continue
                contents = self.collect(root=file, cur_depth=cur_depth + 1)
                if not self.keep_empty_dirs and not contents:
                    continue
                collection[file.name] = contents
            else: # file is not a directory
                if self.matcher and not self.matcher.match(file.name):
                    continue
                collection[file.name] = None
        return collection


# def parse_args():
#     parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
#     parser._action_groups.pop()
#     required = parser.add_argument_group('required arguments')
#     optional = parser.add_argument_group('optional arguments')
#     # add_arguments here

#     args = parser.parse_args()
#     if len(argv) < 2:
#         parser.print_usage()
#         exit(1)

#     if not args.prefix:
#         args.prefix = args.run_dir.absolute().parts[-1]
#     main(args)


if __name__ == '__main__':
    
    # exomes = '/media/EXOMEDATA/exomes'
    # matcher = Multimatcher([('BR1605', 'fastq.gz')])
    # src = SingleRootCollector(exomes, matcher=matcher)
    # collection = src.collect(keep_empty_dirs=False)
    # collection = DictCollection(collection)
    # print(collection.to_list())

    # depth testing

    # path = '/home/ushakov/repo/cerbalab/SamplesInfoCollector/test'
    # matcher = BoolOutputMultimatcher(['^ces', '^wes', '^other', '^wgs'])
    # src = SingleRootCollector(path, matcher=matcher, keep_empty_dirs=True, match_dirs=True)
    # collection = src.collect()
    # print(collection)
    # print(DictCollection(collection).to_list(keep_empty_dirs=True))
    lower_src = SingleRootCollector(
    '/home/ushakov/repo/cerbalab/SamplesInfoCollector/test',
    matcher = BoolOutputMultimatcher(
            [(r'fastq\.gz', 0),
            (r'\.vcf', 0),
            (r'\.csv', 0),
            (r'\.bam$', 0)
            ],
            exclude=[r'(?:other|ces|wes|wgs|)_\d+\.(AF|GF|S\d[0-1]?)\.vcf$']),
    keep_empty_dirs=False)

    print(lower_src.collect())
    # matcher = BoolOutputMultimatcher([r'fastq\.gz', (r'Final\.vcf', 0), r'\.csv', r'\.bam$'], mode='any')

    