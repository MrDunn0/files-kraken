import os
import pathlib
from abc import ABC, abstractmethod
from better_abc import abstract_attribute


class FilesCollection(ABC):
    @abstractmethod
    def extend(self):
        pass

    @abstractmethod
    def to_list(self):
        pass

    @abstractmethod
    def cut_to_key(self):
        pass


class DictCollection(FilesCollection, dict):
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
                                to_pathlib=to_pathlib)]
                    )
            elif value is None:
                list_out.append(key)
            else:
                raise ValueError(f'Wrong value type for DictCollection: {type(value)}. Key: {key}')
        return [pathlib.Path(el) for el in list_out] if to_pathlib else list_out

    def to_list(self, keep_empty_dirs=False, to_pathlib=False, **kwargs):
        return self._to_list(self, keep_empty_dirs=keep_empty_dirs, to_pathlib=to_pathlib)

    def cut_to_key(self, key):
        if key in self:
            return DictCollection({key: self.get(key)})
        else:
            return DictCollection()


class FilesCollector(ABC):
    @abstractmethod
    def collect(self):
        pass

    @abstract_attribute
    def root(self):
        pass

    @abstract_attribute
    def output_format(self):
        pass


class SingleRootCollector(FilesCollector):
    def __init__(
            self, root, matcher=None, output_format=DictCollection,
            match_dirs=None, max_depth=None, keep_empty_dirs=True):

        self.root = pathlib.Path(root).absolute() if root else root
        self.matcher = matcher
        self.max_depth = max_depth
        self.match_dirs = match_dirs
        self.keep_empty_dirs = keep_empty_dirs
        self.output_format = output_format  # Supports only collections inherited from dict

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
            else:  # file is not a directory
                if self.matcher and not self.matcher.match(file.name):
                    continue
                collection[file.name] = None
        return collection


__all__ = [
    'FilesCollection',
    'DictCollection',
    'FilesCollector',
    'SingleRootCollector'
]
