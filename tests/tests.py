# import os
# import pathlib
# from abc import ABC, abstractmethod


# class FilesCollection(ABC):
#     pass


# class DictCollection(FilesCollection, dict):
#     def extend(self, other):
#         for o, o_items in other.items():
#             if o in self and o_items:
#                 if not self[o]:
#                     self[o] = o_items
#                 else:
#                     self[o] = DictCollection.extend(self[o], o_items)
#             else:
#                 self[o] = o_items
#         return self

#     @staticmethod
#     def _to_list(dc, keep_empty_dirs=False, to_pathlib=True):
#         list_out = []
#         for key, value in dc.items():
#             if isinstance(value, dict):
#                 if keep_empty_dirs and not value:
#                     list_out.append(key)
#                 else:
#                     list_out.extend(
#                         [f'{key}{os.sep}{el}'
#                             for el in DictCollection._to_list(
#                                 value,
#                                 keep_empty_dirs=keep_empty_dirs,
#                                 to_pathlib=to_pathlib)
#                         ]
#                     )
#             elif value is None:
#                 list_out.append(key)
#             else:
#                 raise ValueError(f'Wrong value type for DictCollection: {type(value)}. Key: {key}')
#         return [pathlib.Path(el) for el in list_out] if to_pathlib else list_out

#     def to_list(self, keep_empty_dirs=False, to_pathlib=False, **kwargs):
#         return self._to_list(self, keep_empty_dirs=keep_empty_dirs, to_pathlib=to_pathlib)

#     def cut_to_key(self, key):
#         if key in self:
#             return DictCollection({key: self.get(key)})
#         else:
#             return DictCollection()
        
# t = DictCollection(
#     {
#                 str(pathlib.Path('tests_data/collector_path')): {
#                     'run_1': {
#                         'input':{
#                             'sample_1.fastq.gz': None
#                         }},
#                     'run_2': {
#                         'bams': {
#                             'sample_1.bam': None
#                         }
#                     },
#                     'run_3': {}
                    
#                 }
#             }
# )

# # print(t)
# # print(t['tests_data/collector_path']['run_3'])
# # print(t.to_list())
# print(t.to_list(keep_empty_dirs=True)) 
# print(t.to_list(keep_empty_dirs=True, to_pathlib=False) == [
#                 'tests_data/collector_path/run_1/input/sample_1.fastq.gz',
#                 'tests_data/collector_path/run_2/bams/sample_1.bam',
#                 'tests_data/collector_path/run_3'])