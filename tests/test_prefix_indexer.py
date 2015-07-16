from nose.tools import eq_, assert_in, assert_not_in
import redis

from redis_gadgets import prefix_indexer

# TODO: delete by key prefix in setup


class CheckCompoundScore(object):
    def __init__(self, msg):
        self.description = msg
        self.db = redis.StrictRedis(db=15)

    def __call__(self, element_list):
        self.db.delete("test:compound_score")
        for element in element_list:
            key = element[0]
            score_list = element[1:]
            score = prefix_indexer.compute_compound_scores(score_list)
            self.db.zadd("test:compound_score", score, key)

        rez = self.db.zrange('test:compound_score', 0, -1)
        eq_([u'first', u'second', u'third'], rez)

cases = {
    'asc, desc, desc': (
        ('first', (3, 'asc'), (4, 'desc'), (0, 'desc')),
        ('third', (7, 'asc'), (5, 'desc'), (5, 'desc')),
        ('second', (7, 'asc'), (5, 'desc'), (50, 'desc'))
    ),
    'asc, desc': (
        ('first', (3, 'asc'), (4, 'desc')),
        ('third', (7, 'asc'), (5, 'desc')),
        ('second', (7, 'asc'), (10, 'desc'))
    ),
    'desc, desc': (
        ('third', (3, 'desc'), (4, 'desc')),
        ('second', (7, 'desc'), (5, 'desc')),
        ('first', (7, 'desc'), (10, 'desc'))
    ),
    'desc, asc': (
        ('third', (3, 'desc'), (4, 'asc')),
        ('first', (7, 'desc'), (5, 'asc')),
        ('second', (7, 'desc'), (10, 'asc'))
    ),
    'no tie in primary ordering, desc, asc': (
        ('third', (4, 'desc'), (3, 'asc')),
        ('second', (5, 'desc'), (7, 'asc')),
        ('first', (10, 'desc'), (7, 'asc'))
    ),
    'no tie in primary ordering, asc, asc': (
        ('first', (4, 'asc'), (3, 'asc')),
        ('second', (5, 'asc'), (7, 'asc')),
        ('third', (10, 'asc'), (7, 'asc'))
    ),
    'no tie in primary ordering, desc, desc': (
        ('third', (4, 'desc'), (3, 'desc')),
        ('second', (5, 'desc'), (7, 'desc')),
        ('first', (10, 'desc'), (7, 'desc'))
    ),
    'no tie in priamry ordering, asc, desc': (
        ('first', (4, 'asc'), (3, 'desc')),
        ('second', (5, 'asc'), (7, 'desc')),
        ('third', (10, 'asc'), (7, 'desc'))
    ),
}


def test_compound_scores():
    for desc, data in cases.items():
        checker = CheckCompoundScore(desc)
        yield checker, data


class TestPrefixIndexing(object):
    """Test the prefix indexer
    """
    @classmethod
    def setup_class(cls):
        cls.con = redis.StrictRedis(db=15)  # use high db for testing
        cls.index_name = 'test_index'

    def setup(self):
        self.con.flushdb()

    def test_match_string(self):
        """Can match a string by prefix
        """
        prefix_indexer.build_prefix_index(self.con, self.index_name, 'alchemy',
                                          'alpha')
        actual = prefix_indexer.get_matches('alc', db=self.con,
                                            index_name=self.index_name,
                                            start=0, end=-1)
        eq_(len(actual), 1)
        eq_(actual[0], 'alpha')

    def test_prefix_multi_match(self):
        """get_matches returns all strings matching the prefix
        """
        prefix_indexer.build_prefix_index(self.con, self.index_name, 'dafydd',
                                          'alpha')
        prefix_indexer.build_prefix_index(self.con, self.index_name, 'dagon',
                                          'beta')
        actual = prefix_indexer.get_matches('da', db=self.con,
                                            index_name=self.index_name,
                                            start=0, end=-1)
        assert_in('alpha', actual)
        assert_in('beta', actual)

    def test_spaces_in_search_string(self):
        """Can use spaces to seperate prefix search terms

        This would be used, for example, to partial match on first and last
        name
        """
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'ebenezer', 'alpha')
        prefix_indexer.build_prefix_index(self.con, self.index_name, 'safari',
                                          'alpha')

        prefix_indexer.build_prefix_index(self.con, self.index_name, 'ebby',
                                          'beta')
        prefix_indexer.build_prefix_index(self.con, self.index_name, 'dakota',
                                          'beta')

        prefix_indexer.build_prefix_index(self.con, self.index_name, 'saffron',
                                          'gamma')
        prefix_indexer.build_prefix_index(self.con, self.index_name, 'cadmus',
                                          'gamma')
        eb_res = prefix_indexer.get_matches('eb', db=self.con,
                                            index_name=self.index_name,
                                            start=0, end=-1)
        saf_res = prefix_indexer.get_matches('saf', db=self.con,
                                             index_name=self.index_name,
                                             start=0, end=-1)
        eb_saf_res = prefix_indexer.get_matches('eb saf', db=self.con,
                                                index_name=self.index_name,
                                                start=0, end=-1)

        assert_in('alpha', eb_res)
        assert_in('beta', eb_res)
        assert_not_in('gamma', eb_res)

        assert_in('alpha', saf_res)
        assert_not_in('beta', saf_res)
        assert_in('gamma', saf_res)

        assert_in('alpha', eb_saf_res)
        assert_not_in('beta', eb_saf_res)
        assert_not_in('gamma', eb_saf_res)

    def test_case_insensitive(self):
        """Prefix matching is case insensitive
        """
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'February', 'alpha')
        assert_in('alpha',
                  prefix_indexer.get_matches('feb', db=self.con,
                                             index_name=self.index_name,
                                             start=0, end=-1))
        assert_in('alpha',
                  prefix_indexer.get_matches('FEB', db=self.con,
                                             index_name=self.index_name,
                                             start=0, end=-1))

    def test_ordering(self):
        """get_matches returns exact matches first
        """
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'gustav', 'alpha')
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'gus', 'beta')
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'gustavo', 'gamma')
        actual = prefix_indexer.get_matches('gus', db=self.con,
                                            index_name=self.index_name,
                                            start=0, end=-1)

        eq_(actual, ['beta', 'alpha', 'gamma'])

    def test_remove(self):
        """Can remove a string from the prefix index
        """
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'gustav', 'alpha')
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'gus', 'beta')
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'gustavo', 'gamma')
        actual = prefix_indexer.get_matches('gus', db=self.con,
                                            index_name=self.index_name,
                                            start=0, end=-1)
        eq_(actual, ['beta', 'alpha', 'gamma'], "GUARD")
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'gustav', 'alpha', operator='rem')
        actual = prefix_indexer.get_matches('gus', db=self.con,
                                            index_name=self.index_name,
                                            start=0, end=-1)
        eq_(actual, ['beta', 'gamma'], "GUARD")

    def test_secondary_sort_keeps_primary_order(self):
        """Adding secondary scores doesn't break length based ordering
        """
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'Kelsey', 'alpha',
                                          secondary_scores=[(10, 'desc')])
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'Kelsey', 'beta',
                                          secondary_scores=[(5, 'desc')])
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'Kelly', 'gamma',
                                          secondary_scores=[(5, 'desc')])

        actual = prefix_indexer.get_matches('kel', db=self.con,
                                            index_name=self.index_name,
                                            start=0, end=-1)
        eq_(actual[0], 'gamma')

    def test_secondary_score_tiebreak(self):
        """Secondary scores are used to break ties in primary ordering
        """
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'Kelsey', 'alpha',
                                          secondary_scores=[(5, 'desc')])
        prefix_indexer.build_prefix_index(self.con, self.index_name,
                                          'Kelsey', 'beta',
                                          secondary_scores=[(10, 'desc')])

        actual = prefix_indexer.get_matches('kel', db=self.con,
                                            index_name=self.index_name,
                                            start=0, end=-1)

        eq_(actual, ['beta', 'alpha'])
