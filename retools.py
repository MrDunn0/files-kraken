import re
from abc import ABC, abstractmethod
from typing import Dict, Pattern


class ReExecutor:
    @staticmethod
    def _return_group(regexp_result, group):
        if regexp_result:
            return regexp_result.group(group)
    @staticmethod
    def fullmatch(pattern, text, group=0):
        
        return ReExecutor._return_group(re.fullmatch(pattern, text), group)

    @staticmethod
    def search(pattern, text, group=0):
        return ReExecutor._return_group(re.search(pattern, text), group)

    @staticmethod
    def findall(pattern, text):
        return re.findall(pattern, text)


class GroupSearcher:
    def __init__(self, pattern, group):
        self.pattern = re.compile(pattern)
        self.group = group

    def search(self, text):
        return ReExecutor.search(self.pattern, text, group=self.group)


class Multimatcher(ABC):
    @abstractmethod
    def match(self, text):
        pass


class MultimatchExecutor(Multimatcher):
    def __init__(self, patterns, exclude=None):
        self.patterns = patterns

    @staticmethod
    def multimatch(patterns, text):
        matches = []
        for entry in patterns:
            match entry:
                case str():  # use re.match on entry
                    pattern = entry
                    matches.append(ReExecutor.fullmatch(pattern, text))
                case tuple(): # two cases
                    match entry[1]:
                        case int(): # 1.Entry is a tuple of single pattern and group
                            pattern = entry[0]
                            group = entry[1]
                            matches.append(ReExecutor.search(pattern, text, group=group))
                        case _: # 2.Entry is a tuple of several patterns
                            sub_result = []
                            for sub_pattern in entry:
                                match sub_pattern:
                                    case tuple():
                                        pattern = sub_pattern[0]
                                        group = sub_pattern[1]
                                        sub_result.append(
                                            ReExecutor.search(pattern, text, group=group))
                                    case str():
                                        sub_result.append(ReExecutor.fullmatch(sub_pattern, text))
                            matches.append(sub_result)
        return matches

    def match(self, text):
        return self.multimatch(self.patterns, text)


class BoolOutputMultimatcher(MultimatchExecutor): # I have big problems with naming...
    def __init__(self, patterns, mode='any', exclude=None):
        super().__init__(patterns)
        self.mode = mode
        self.exclude = exclude

    def any_match(self, text: str) -> bool:
        matches = self.multimatch(self.patterns, text)
        if self.exclude:
            exclude_matches = self.multimatch(self.exclude, text)
            if any(exclude_matches):
                return False
        bools = []
        for match in matches:
            if isinstance(match, list):
                bools.append(all(match))
            else: bools.append(match)
        if self.mode == 'any':
            return any(bools)
        elif self.mode == 'cons':
            return all(bools)

    def match(self, text: str) -> bool:
        return self.any_match(text)


class SchemeMatcher(Multimatcher):
    def __init__(self, matching_scheme: Dict[str, Pattern]):
        self.matching_scheme = matching_scheme

    # I need to rewrite MultimatchExecutor and this to reuse same logic...
    def match_scheme(self, text: str) -> Dict[str, str]:
        result = dict()
        for key, value in self.matching_scheme.items():
            match = None
            match value:
                case tuple():
                    match value[1]:
                        case int():
                            pattern = value[0]
                            group = value[1]
                            match = ReExecutor.search(pattern, text, group=group)
                        case _:
                            for sub_pattern in value:
                                match sub_pattern:
                                    case tuple():
                                        pattern = sub_pattern[0]
                                        group = sub_pattern[1]
                                        match = ReExecutor.search(pattern, text, group=group)
                                    case str():
                                        match = ReExecutor.fullmatch(sub_pattern, text)
                                if match:
                                    result[key] = match\
                                    # Now there is no need to have two matches of text for the same field
                                    break
                case _:
                    pattern = value
                    match = ReExecutor.fullmatch(pattern, text)
            if match:
                result[key] = match
        return result

    def match(self, text: str):
        return self.match_scheme(text)


class ReSorter:
    def __init__(self, searcher, func=None):
        self.searcher = searcher
        self.func = func if func else self._return_self

    def sort(self, items):
        return sorted([i for i in items], 
                    key=lambda i: self.func(self.searcher.search(i)))

    @staticmethod
    def _return_self(value):
        return value


if __name__ == '__main__':
    # t1 = 'wgs_1'
    t2 = '123_ABC_lalala'
    # am = AnyMatcher(['^ces_\d+', '^wes_\d+', '^other_\d+', '^wgs_\d+'])
    # cm = ConseqMatcher(['\d+', 'ABC', 'lala'])
    
    # print(am.match(t1))
    # print(cm.match(t2))
    
    # gs = GroupSearcher(r'_(\d+)', 1)
    # rs = ReSorter(gs, int)
    # print(rs.sort(['wes_123', 'wes_10', 'wes_1', 'wes_3', 'wes_2']))

    me = MultimatchExecutor([('\d+', ('ABC', 0), ('lala', 0))])
    print(me.match(t2))

    t3 = '_ABC_balbalbal'
    am = BoolOutputMultimatcher([('\d+', ('ABC', 0), ('lala', 0)), ('ABC', 0)])
    print(am.match(t3))
    
    required_fields = {'run': (r'^other|ces|wes|wgs_\d+', 0),
                                'sample': (r'sample_S?(\w+)', 1)}

    # sm = SchemeMatcher(required_fields)
    # print(sm.match('other_153.sample_123456.fastq.gz'))
    optional_fields = {
        'fastqs': re.compile('^\\w+\\.sample_NG067\\.lane_\\d+\\.R[1-2].fastq.gz'),
        'bams': re.compile('^\\w+\\.sample_NG067\\.(dedup|recal|realigned)\\.bam'),
        'vcf': re.compile('^\\w+\\.SNG067\\.vcf'),
        'csv': re.compile('^\\w+\\.sample_SNG067\\.csv'),
        'xlsx': re.compile('^\\w+\\.sample_SNG067\\.xlsx'),
        'metrics': re.compile('^\\w+\\.sample_NG067\\.HS\\.metrics\\.tsv')}
    sm = SchemeMatcher(optional_fields)
    print(sm.match('ces_154.sample_NG067.dedup.bam'))
    print(ReExecutor.fullmatch(r'(?:other|ces|wes|wgs|)_\d+\.(AF|GF|S\d[0-1]?)\.vcf$', 'ces_3000.GF.vcf'))
    print(re.fullmatch(r'(?:other|ces|wes|wgs|)_\d+\.(AF|GF|S\d[0-1]?)\.vcf$', 'ces_3000.GF.vcf'))