"""
Tests for set_theory cached multi-set query library
"""
import time
import redis
from nose.tools import (raises, eq_, with_setup, assert_in, assert_not_in,
                        assert_not_equal, make_decorator)

# TODO: prefix test keys for keyspace find and delete instead of flushing
# TODO: Add missing test doc strings
# TODO: Change "should style" doc strings to affirmative statements
# TODO: Convert to class-based tests

DB_NUM = 15

from redis_gadgets import set_theory
from redis_gadgets import WeightedKey


def run_with_both(func):
    """Decorator to turn a test into a generator that runs the test with both
    a Redis and a StrictRedis connection.  Decorated tests should take an
    argument for the database connection
    """
    strict = redis.StrictRedis(db=DB_NUM)
    non_strict = redis.Redis(db=DB_NUM)

    def gen_new_tests():
        yield func, strict
        yield func, non_strict

    return make_decorator(func)(gen_new_tests)


def _setup():
    # force these to strict mode - doesn't matter here, because presumably the
    # caller will use the correct call syntax for whichever client they like.
    db = redis.StrictRedis(db=DB_NUM)
    db.flushdb()
    now = time.time()
    yesterday = now - 86400
    for i in range(10):
        db.zadd('SET_A', now + i, i)
    for i in range(5, DB_NUM):  # slight overlap in ids
        db.zadd('SET_B', yesterday + i, i)


def _compound_setup():
    # TEST_1: { 0:50, 1:51, ... 9:59}
    # TEST_2: { 0:50, 1:51, ... 19:69}
    # TEST_3: { 10:70, 11:71, ... 29:79}
    # force these to strict mode - doesn't matter here, because presumably the
    # caller will use the correct call syntax for whichever client they like.
    db = redis.StrictRedis(db=DB_NUM)
    db.flushdb()
    val = 50
    for i in range(30):
        if i < 10:
            db.zadd('TEST_1', val + i, i)
            db.zadd('TEST_2', val + i, i)
        elif i < 20:
            db.zadd('TEST_2', val + i, i)
            db.zadd('TEST_3', val + i, i)
        elif i < 30:
            db.zadd('TEST_3', val + i, i)


def test_thread_key_names():
    import threading
    results = []

    class KeyRecordingThread(threading.Thread):
        def run(self):
            results.append(set_theory._unique_id())

    threads = []
    for i in range(2):
        new_thread = KeyRecordingThread()
        new_thread.start()
        threads.append(new_thread)

    while threads:
        thread = threads.pop()
        thread.join()
        if thread.is_alive():
            threads.append(thread)
    assert_not_equal(results[0], results[1])


@run_with_both
@raises(ValueError)
def test_no_default_start_and_end(db):
    '''Start and end are required for non-count queries
    '''
    st = set_theory.SetTheory(db)
    st.zset_fetch([("fake_key:1",)])


@run_with_both
@raises(ValueError)
def test_no_range_for_count(db):
    '''Start and end are invalid for count queries
    '''
    st = set_theory.SetTheory(db)
    st.zset_fetch([("fake_key:1",)], count=True, start=1, end=1)


@run_with_both
@raises(ValueError)
def test_return_key_raises_without_ttl(db):
    '''You should not be able to return the hash key without a ttl
    because it will be deleted before you can use the return value
    '''
    st = set_theory.SetTheory(db)
    st.zset_fetch([('fake_key',)], return_key=True)


@run_with_both
@raises(ValueError)
def test_invalid_weighted_key(db):
    """SetTheory raises ValueError on invalid bind element
    """
    st = set_theory.SetTheory(db)
    st.zset_fetch([('SET_A', 1.0, 'bogus'), ('SET_B',)],
                  operator="intersect", count=True)


@with_setup(_setup)
@run_with_both
def test_single_weighted_key(db):
    """Can use WeightedKey directly for single key queries
    """
    st = set_theory.SetTheory(db)
    key = WeightedKey('SET_A')
    eq_(10, st.zset_fetch([key], count=True))


@with_setup(_setup)
@run_with_both
def test_multiple_weighted_key(db):
    """Can use WeightedKey directly for multiple key queries
    """
    st = set_theory.SetTheory(db)
    key_a = WeightedKey('SET_A')
    key_b = WeightedKey('SET_B')
    eq_(5, st.zset_fetch([key_a, key_b],
        operator="intersect", count=True,
        thread_local=True))


@with_setup(_setup)
@run_with_both
def test_simple_thread_safe_count(db):
    st = set_theory.SetTheory(db)
    a_count = st.zset_fetch([('SET_A',)], count=True,
                            thread_local=True)
    eq_(10, a_count, "GUARD: initial count for SET_A is %s" % a_count)
    b_count = st.zset_fetch([('SET_B',)], count=True,
                            thread_local=True)
    eq_(10, b_count, "GUARD: initial count for SET_B is %s" % b_count)
    eq_(5, st.zset_fetch([('SET_A',), ('SET_B',)],
        operator="intersect", count=True,
        thread_local=True))
    eq_(15, st.zset_fetch([('SET_A',), ('SET_B',)],
        operator="union", count=True,
        thread_local=True))


@with_setup(_setup)
@run_with_both
def test_simple_count(db):
    st = set_theory.SetTheory(db)
    eq_(10, st.zset_fetch([('SET_A',)], count=True))
    eq_(10, st.zset_fetch([('SET_B',)], count=True))
    eq_(5, st.zset_fetch([('SET_A',), ('SET_B',)],
        operator="intersect", count=True), "intersect count was wrong")
    eq_(15, st.zset_fetch([('SET_A',), ('SET_B',)],
        operator="union", count=True), "union count was wrong")


@with_setup(_setup)
@run_with_both
def test_simple_fetch(db):
    st = set_theory.SetTheory(db)
    results = st.zset_fetch([('SET_A',)], start=0, end=0,
                            reverse=False)
    print results
    assert '0' in results
    assert '1' not in results


@with_setup(_setup)
@run_with_both
def test_weighted_intersect(db):
    # TODO: Shouldn't this test actually look at the scores?
    st = set_theory.SetTheory(db)
    results = st.zset_fetch([('SET_A', 1.0), ('SET_B', 2.0)],
                            operator="intersect", start=0,
                            end=0)
    print results
    assert '9' in results


@with_setup(_setup)
@run_with_both
def test_weighted_union(db):
    # TODO: Shouldn't this test actually look at the scores?
    st = set_theory.SetTheory(db)
    results = st.zset_fetch([('SET_A', 1.0), ('SET_B', 2.0)],
                            operator="union", start=0, end=0)
    print results
    assert '14' in results


@with_setup(_compound_setup)
@run_with_both
def test_intersect_union(db):
    st = set_theory.SetTheory(db)
    # temp hash should be (TEST_2 && TEST_3) (10-19 inclusive)
    temp_hash = st.zset_fetch([('TEST_2',), ('TEST_3',)],
                              return_key=True, ttl=5,
                              operator="intersect")
    # now union TEST_1 onto the earlier set and we should have the entire set
    # again (0-19 inclusive)
    results = st.zset_fetch([(temp_hash,), ('TEST_1',)],
                            start=0, end=-1,
                            operator="union")
    for i in range(30):
        if i < 20:
            assert_in(str(i), results)
        else:
            assert_not_in(str(i), results)


@with_setup(_compound_setup)
@run_with_both
def test_union_intersect(db):
    # first make an unweighted union as a control
    st = set_theory.SetTheory(db)
    temp_hash_control = st.zset_fetch([('TEST_1',), ('TEST_3',)],
                                      return_key=True, ttl=5,
                                      operator="union")

    control_results = st.zset_fetch([(temp_hash_control,),
                                     ('TEST_2',)], start=0,
                                    end=-1,
                                    operator="intersect")
    eq_('19', control_results[0],
        "GUARD: 19 not first element of %s" % control_results)

    # now for the actual weighting experiment
    # now make a weighted union to test TEST_1 trumps TEST_3
    temp_hash_weighted = st.zset_fetch([('TEST_1', 1000), ('TEST_3',)],
                                       return_key=True, ttl=5,
                                       operator="union")
    experiment_results = st.zset_fetch([(temp_hash_weighted,),
                                        ('TEST_2',)], start=0,
                                       end=-1,
                                       operator="intersect")
    eq_('9', experiment_results[0])


@with_setup(_setup)
@run_with_both
def test_timestamp_weighting(db):
    st = set_theory.SetTheory(db)
    strict = redis.StrictRedis(db=DB_NUM)
    now = time.time()
    yesterday = now - 84600
    # force these to strict mode - doesn't matter here, because presumably the
    # caller will use the correct call syntax for whichever client they like.
    strict.zadd("TEST_1", float(now), 'today')
    strict.zadd("TEST_2", float(yesterday), 'yesterday')

    # with no weighting, today should show up first
    results = st.zset_fetch([('TEST_1',), ('TEST_2',)],
                            start=0, end=-1,
                            operator="union")
    eq_('today', results[0])

    results = st.zset_fetch([('TEST_1', 0.0095), ('TEST_2',)],
                            start=0, end=-1,
                            operator="union")
    eq_('yesterday', results[0])

    results = st.zset_fetch([('TEST_1',), ('TEST_2', 1.0005)],
                            start=0, end=-1,
                            operator="union")
    eq_('yesterday', results[0])


def _setup_range():
    """Prime one zset with a range of 20 values
    """
    db = redis.StrictRedis(db=DB_NUM)
    for i in range(20):
        db.zadd('SET_A', i, i)


@with_setup(_setup_range)
@run_with_both
def test_score_range_query(db):
    '''set theory should support score ranges
    '''
    st = set_theory.SetTheory(db)
    # redis range is inclusive by default, python range is not inclusive of
    # both ends, thus 5:16 in python should match 5:15 in redis
    eq_([str(i) for i in range(5, 16)], st.zset_fetch([('SET_A',)],
        min_score=5.0, max_score=15.0, start=0, end=-1,
        reverse=False))


@with_setup(_setup_range)
@run_with_both
def test_score_range_reverse_query(db):
    '''set theory should support reverse score ranges
    '''
    st = set_theory.SetTheory(db)
    # redis range is inclusive by default, python range is not inclusive of
    # both ends, thus 5:16 in python should match 5:15 in redis
    expected = [str(i) for i in range(5, 16)]
    expected.reverse()
    eq_(expected, st.zset_fetch([('SET_A',)], min_score=5.0,
        max_score=15.0, start=0, end=-1, reverse=True))


@with_setup(_setup_range)
@run_with_both
def test_score_range_inf(db):
    '''range queries should support +/-inf
    '''
    st = set_theory.SetTheory(db)
    expected = [str(i) for i in range(0, 11)]
    eq_(expected, st.zset_fetch([("SET_A",)], min_score='-inf',
        max_score=10, start=0, end=-1, reverse=False))

    expected = [str(i) for i in range(10, 20)]
    eq_(expected, st.zset_fetch([("SET_A",)], min_score=10,
        max_score='+inf', start=0, end=-1, reverse=False))


@with_setup(_setup_range)
@run_with_both
def test_score_range_open_interval(db):
    '''range queries should support the redis (score open interval notation
    '''
    st = set_theory.SetTheory(db)
    expected = [str(i) for i in range(6, 15)]
    # that unmatched paren will haunt you all day
    eq_(expected, st.zset_fetch([('SET_A',)], min_score='(5',
        max_score='(15', start=0, end=-1, reverse=False))


@with_setup(_setup_range)
@run_with_both
def test_score_range_limit_offset(db):
    '''range queries should translate start & end to limit + offset notation
    '''
    st = set_theory.SetTheory(db)
    expected = [str(i) for i in range(10, 13)]
    eq_(expected, st.zset_fetch([('SET_A',)], min_score='5',
        max_score='15', start=5, end=7, reverse=False))


@run_with_both
@raises(ValueError)
def test_score_range_negative_offset(db):
    '''range queries don't allow negative end, except 0, -1 special case
    '''
    st = set_theory.SetTheory(db)
    st.zset_fetch([('SET_A',)], min_score='5', max_score='15',
                  start=5, end=-1, reverse=False)


@run_with_both
@raises(ValueError)
def test_score_range_zero_start_neg_offset(db):
    '''for range queries a negative end less than -1 should always be an error
    '''
    st = set_theory.SetTheory(db)
    st.zset_fetch([('SET_A',)], min_score='5', max_score='15',
                  start=0, end=-2, reverse=False)


@with_setup(_compound_setup)
@run_with_both
def test_scored_counts(db):
    """Ensure that counts work with score ranges"""
    st = set_theory.SetTheory(db)
    count = st.zset_count([('TEST_1',), ('TEST_3',)],
                          min_score=58, max_score=79, ttl=5,
                          operator="union")
    eq_(count, 22)


@with_setup(_compound_setup)
@run_with_both
def test_scored_counts_fetch(db):
    """Ensure that counts work with score ranges through zset_fetch"""
    st = set_theory.SetTheory(db)
    count = st.zset_fetch([('TEST_1',), ('TEST_3',)],
                          count=True, min_score=58, max_score=79,
                          ttl=5, operator="union")
    eq_(count, 22)


@with_setup(_compound_setup)
@run_with_both
def test_counts_no_cache(db):
    """Ensure that counts dont cache with ttl=0"""
    st = set_theory.SetTheory(db)
    count = st.zset_count([('TEST_1',), ('TEST_3',)], ttl=0,
                          operator="union")
    assert(count)
    _, cache_created = st.zset_cache([('TEST_1',), ('TEST_3',)],
                                     operator="union")
    assert(cache_created)


@with_setup(_compound_setup)
@run_with_both
def test_range_no_cache(db):
    """Ensure that range dont cache with ttl=0"""
    st = set_theory.SetTheory(db)
    result = st.zset_range([('TEST_1',), ('TEST_3',)], ttl=0,
                           operator="union", start=0, end=-1)
    assert(result)
    _, cache_created = st.zset_cache([('TEST_1',), ('TEST_3',)],
                                     operator="union")
    assert(cache_created)


@with_setup(_compound_setup)
@run_with_both
def test_scored_range(db):
    """Ensure that range with min and max scores works in standalone method"""
    st = set_theory.SetTheory(db)
    results = st.zset_range([('TEST_1',), ('TEST_3',)], ttl=5,
                            operator="union", start=0, end=-1,
                            min_score=59, max_score=78)
    eq_(len(results), 20)
    eq_(results[19], u'9')


@with_setup(_compound_setup)
@run_with_both
def test_scored_range_first_page(db):
    """Ensure that range with min and max scores works with page 0"""
    st = set_theory.SetTheory(db)
    results = st.zset_range([('TEST_1',), ('TEST_3',)], ttl=5,
                            operator="union", start=0, end=5,
                            min_score=59, max_score=78)
    eq_(len(results), 6)
    eq_(results[0], u'28')


@with_setup(_compound_setup)
@run_with_both
def test_scored_range_last_page(db):
    """Ensure that range with min and max scores works with page 0"""
    st = set_theory.SetTheory(db)
    results = st.zset_range([('TEST_1',), ('TEST_3',)], ttl=5,
                            operator="union", start=15, end=19,
                            min_score=59, max_score=78)
    eq_(len(results), 5)
    eq_(results[4], u'9')


@with_setup(_compound_setup)
@run_with_both
def test_range_with_scores(db):
    """Ensure that range with min and max scores works in standalone method"""
    st = set_theory.SetTheory(db)
    results = st.zset_range([('TEST_1',), ('TEST_3',)], ttl=5,
                            operator="union", start=0, end=-1,
                            withscores=True)
    eq_(len(results), 30)
    eq_(results[0], (u'29', 79.0))


@with_setup(_compound_setup)
@run_with_both
def test_scored_range_no_reverse(db):
    """Ensure that range with min and max scores works in standalone method
    without reverse"""
    st = set_theory.SetTheory(db)
    results = st.zset_range([('TEST_1',), ('TEST_3',)], ttl=5,
                            operator="union", start=0, end=-1,
                            min_score=59, max_score=78, reverse=False)
    eq_(len(results), 20)
    eq_(results[0], u'9')


@with_setup(_compound_setup)
@run_with_both
def test_counts_with_expiry(db):
    """Ensure that adding a ttl will expire a cache created by calling count"""
    st = set_theory.SetTheory(db)
    key_hash, _ = st.zset_cache([('TEST_1',), ('TEST_3',)],
                                operator="union")
    eq_(db.ttl(key_hash), set_theory.MAX_CACHE_SECONDS, "GUARD")
    # Delete the key so the count call will create it anew
    db.delete(key_hash)
    st.zset_count([('TEST_1',), ('TEST_3',)], ttl=10,
                  operator="union")
    eq_(db.ttl(key_hash), 10)


@with_setup(_compound_setup)
@run_with_both
def test_range_with_expiry(db):
    """Ensure that adding a ttl will expire a cache created by calling range"""
    st = set_theory.SetTheory(db)
    key_hash, _ = st.zset_cache([('TEST_1',), ('TEST_3',)],
                                operator="union")
    eq_(db.ttl(key_hash), set_theory.MAX_CACHE_SECONDS, "GUARD")
    # Delete the key so the count call will create it anew
    db.delete(key_hash)
    st.zset_range([('TEST_1',), ('TEST_3',)],
                  operator="union", ttl=10, start=0, end=-1)
    eq_(db.ttl(key_hash), 10)


@with_setup(_compound_setup)
@run_with_both
def test_zset_fetch_with_expiry(db):
    """Ensure that adding a ttl will expire a cache created by calling fetch"""
    st = set_theory.SetTheory(db)
    key_hash, _ = st.zset_cache([('TEST_1',), ('TEST_3',)],
                                operator="union")
    eq_(db.ttl(key_hash), set_theory.MAX_CACHE_SECONDS, "GUARD")
    # Delete the key so the count call will create it anew
    db.delete(key_hash)
    st.zset_fetch([('TEST_1',), ('TEST_3',)], operator="union",
                  ttl=10, return_key=True)
    eq_(db.ttl(key_hash), 10)
