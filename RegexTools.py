import re
from abc import ABC, abstractmethod


class ReExecutor:
    @staticmethod
    def _return_group(regexp_result, group):
        if regexp_result:
            return regexp_result.group(group)
    @staticmethod
    def match(pattern, text, group=0):
        return ReExecutor._return_group(re.match(pattern, text ), group)
    
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
    def __init__(self, patterns):
        self.patterns = patterns
    
    @abstractmethod
    def match():
        pass


class AnyMatcher(Multimatcher):
    def match(self, text):
        for p in self.patterns:
            if isinstance(p, (list, tuple)):
                if all(ReExecutor.search(sub_p, text) for sub_p in p):
                    return True
            elif ReExecutor.search(p, text):
                    return True
        return False


class ConseqMatcher(Multimatcher):
    def match(self, text):
        for p in self.patterns:
            if isinstance(p, (list, tuple)):
                if not all(ReExecutor.search(sub_p, text) for sub_p in p):
                    return False
            elif not ReExecutor.search(p, text):
                return False
        return True


class MultimatcherOld:
    def __init__(self, patterns, mode='any'):
        self.patterns = patterns
        self.mode = mode

    @staticmethod
    def any_match(text, patterns):
        for p in patterns:
            if isinstance(p, (list, tuple)):
                if all(ReExecutor.search(sub_p, text) for sub_p in p):
                    return True
            elif ReExecutor.search(p, text):
                    return True
        return False

    @staticmethod
    def conseq_match(text, patterns):
        for p in patterns:
            if isinstance(p, (list, tuple)):
                if not all(ReExecutor.search(sub_p, text) for sub_p in p):
                    return False
            elif not ReExecutor.search(p, text):
                return False
        return True
                
    def match(self, text):
        modes_methods = {
            'any': self.any_match,
            'cons': self.conseq_match
        }
        match = modes_methods[self.mode]
        return match(text, self.patterns)


class ReSorter:
    def __init__(self, searcher, func=None):
        self.searcher = searcher
        self.func = func if func else self._return_self

    def sort(self, items):
        return sorted([i for i in items], 
                      key=lambda i: self.func(self.searcher.search(i)))
    
    def _return_self(value):
        return value


if __name__ == '__main__':
    t1 = 'wgs_1'
    t2 = '123_ABC_lalala'
    am = AnyMatcher(['^ces_\d+', '^wes_\d+', '^other_\d+', '^wgs_\d+'])
    cm = ConseqMatcher(['\d+', 'ABC', 'lala'])
    
    print(am.match(t1))
    print(cm.match(t2))
    
    gs = GroupSearcher(r'_(\d+)', 1)
    rs = ReSorter(gs, int)
    print(rs.sort(['wes_123', 'wes_10', 'wes_1', 'wes_3', 'wes_2']))