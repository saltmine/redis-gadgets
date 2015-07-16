"""
Tests for set_theory cached multi-set query library
"""
import pprint
import time
import redis
from nose.tools import (raises, eq_, with_setup, assert_in, assert_not_in,
    assert_not_equal)
from nose.plugins.attrib import attr

# TODO: prefix test keys for keyspace find and delete instead of flushing
# TODO: Convert to class-based tests
# TODO: Change slow expiration tests to just check that the TTL was set
#       correctly and trust that redis does the right thing.

db = redis.StrictRedis(db=15)

from redis_gadgets import set_theory


def _setup():
    db.flushdb()
    now = time.time()
    yesterday = now - 86400
    for i in range(10):
        db.zadd('SET_A', now + i, i)
    for i in range(5, 15):  # slight overlap in ids
        db.zadd('SET_B', yesterday + i, i)


def _compound_setup():
  # TEST_1: { 0:50, 1:51, ... 9:59}
  # TEST_2: { 0:50, 1:51, ... 19:69}
  # TEST_3: { 10:70, 11:71, ... 29:79}
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


@raises(ValueError)
def test_no_default_start_and_end():
    '''Start and end are required for non-count queries
    '''
    set_theory.zset_fetch([("fake_key:1",)], db=db, callback=lambda(x): x)


@raises(ValueError)
def test_no_range_for_count():
    '''Start and end are invalid for count queries
    '''
    set_theory.zset_fetch([("fake_key:1",)], db=db, count=True, start=1, end=1)


@raises(ValueError)
def test_return_key_raises_without_ttl():
    '''You should not be able to return the hash key without a ttl
    because it will be deleted before you can use the return value
    '''
    set_theory.zset_fetch([('fake_key',)], db=db, return_key=True)


@with_setup(_setup)
def test_simple_thread_safe_count():
    a_count = set_theory.zset_fetch([('SET_A',)], db=db, count=True,
                                    thread_local=True)
    eq_(10, a_count, "GUARD: initial count for SET_A is %s" % a_count)
    b_count = set_theory.zset_fetch([('SET_B',)], db=db, count=True,
                                    thread_local=True)
    eq_(10, b_count, "GUARD: initial count for SET_B is %s" % b_count)
    eq_(5, set_theory.zset_fetch([('SET_A',), ('SET_B',)],
        db=db, operator="intersect", count=True,
        thread_local=True))
    eq_(15, set_theory.zset_fetch([('SET_A',), ('SET_B',)],
        db=db, operator="union", count=True,
        thread_local=True))


@with_setup(_setup)
def test_simple_count():
    eq_(10, set_theory.zset_fetch([('SET_A',)], db=db, count=True))
    eq_(10, set_theory.zset_fetch([('SET_B',)], db=db, count=True))
    eq_(5, set_theory.zset_fetch([('SET_A',), ('SET_B',)],
        db=db, operator="intersect", count=True), "intersect count was wrong")
    eq_(15, set_theory.zset_fetch([('SET_A',), ('SET_B',)],
        db=db, operator="union", count=True), "union count was wrong")


@with_setup(_setup)
def test_simple_fetch():
    results = set_theory.zset_fetch([('SET_A',)], db=db, start=0, end=0,
                                    ids_only=True, reverse=False)
    print results
    assert '0' in results
    assert '1' not in results


@with_setup(_setup)
def test_weighted_intersect():
    results = set_theory.zset_fetch([('SET_A', 1.0), ('SET_B', 2.0)],
                                    db=db, operator="intersect", start=0,
                                    end=0, ids_only=True)
    print results
    assert '9' in results


@with_setup(_setup)
def test_weighted_union():
    results = set_theory.zset_fetch([('SET_A', 1.0), ('SET_B', 2.0)], db=db,
                                    operator="union", start=0, end=0,
                                    ids_only=True)
    print results
    assert '14' in results


@with_setup(_compound_setup)
def test_unweighted_compound_operations():
    # temp hash should be (TEST_2 && TEST_3) (10-19 inclusive)
    temp_hash = set_theory.zset_fetch([('TEST_2',), ('TEST_3',)], db=db,
                                      return_key=True, ttl=5,
                                      operator="intersect")
    # now union TEST_1 onto the earlier set and we should have the entire set
    # again (0-19 inclusive)
    results = set_theory.zset_fetch([(temp_hash,), ('TEST_1',)], db=db,
                                    start=0, end=-1, ids_only=True,
                                    operator="union")
    for i in range(30):
        if i < 20:
            assert_in(str(i), results)
        else:
            assert_not_in(str(i), results)


@with_setup(_compound_setup)
def test_weighted_compound_operations():
    # first make an unweighted union as a control
    temp_hash_control = set_theory.zset_fetch([('TEST_1',), ('TEST_3',)],
                                              db=db, return_key=True, ttl=5,
                                              operator="union")

    control_results = set_theory.zset_fetch([(temp_hash_control,),
                                             ('TEST_2',)], db=db, start=0,
                                            end=-1, ids_only=True,
                                            operator="intersect")
    eq_('19', control_results[0])

    # now for the actual weighting experiment
    # now make a weighted union to test TEST_1 trumps TEST_3
    temp_hash_weighted = set_theory.zset_fetch([('TEST_1', 1000), ('TEST_3',)],
                                               db=db, return_key=True, ttl=5,
                                               operator="union")
    experiment_results = set_theory.zset_fetch([(temp_hash_weighted,),
                                                ('TEST_2',)], db=db, start=0,
                                               end=-1, ids_only=True,
                                               operator="intersect")
    eq_('9', experiment_results[0])


@with_setup(_setup)
def test_timestamp_weighting():
    now = time.time()
    yesterday = now - 84600
    db.zadd("TEST_1", float(now), 'today')
    db.zadd("TEST_2", float(yesterday), 'yesterday')

    # with no weighting, today should show up first
    results = set_theory.zset_fetch([('TEST_1',), ('TEST_2',)], db=db,
                                    start=0, end=-1, ids_only=True,
                                    operator="union")
    eq_('today', results[0])

    results = set_theory.zset_fetch([('TEST_1', 0.0095), ('TEST_2',)], db=db,
                                    start=0, end=-1, ids_only=True,
                                    operator="union")
    eq_('yesterday', results[0])

    results = set_theory.zset_fetch([('TEST_1',), ('TEST_2', 1.0005)], db=db,
                                    start=0, end=-1, ids_only=True,
                                    operator="union")
    eq_('yesterday', results[0])


def _setup_range():
    """Prime one zset with a range of 20 values
    """
    for i in range(20):
        db.zadd('SET_A', i, i)


@with_setup(_setup_range)
def test_score_range_query():
    '''set theory should support score ranges
    '''
    # redis range is inclusive by default, python range is not inclusive of
    # both ends, thus 5:16 in python should match 5:15 in redis
    eq_([str(i) for i in range(5, 16)], set_theory.zset_fetch([('SET_A',)],
        db=db, min_score=5.0, max_score=15.0, start=0, end=-1, ids_only=True,
        reverse=False))


@with_setup(_setup_range)
def test_score_range_reverse_query():
    '''set theory should support reverse score ranges
    '''
    # redis range is inclusive by default, python range is not inclusive of
    # both ends, thus 5:16 in python should match 5:15 in redis
    expected = [str(i) for i in range(5, 16)]
    expected.reverse()
    eq_(expected, set_theory.zset_fetch([('SET_A',)], db=db, min_score=5.0,
        max_score=15.0, start=0, end=-1, ids_only=True, reverse=True))


@with_setup(_setup_range)
def test_score_range_inf():
    '''range queries should support +/-inf
    '''
    expected = [str(i) for i in range(0, 11)]
    eq_(expected, set_theory.zset_fetch([("SET_A",)], db=db, min_score='-inf',
        max_score=10, start=0, end=-1, ids_only=True, reverse=False))

    expected = [str(i) for i in range(10, 20)]
    eq_(expected, set_theory.zset_fetch([("SET_A",)], db=db, min_score=10,
        max_score='+inf', start=0, end=-1, ids_only=True, reverse=False))


@with_setup(_setup_range)
def test_score_range_open_interval():
    '''range queries should support the redis (score open interval notation
    '''
    expected = [str(i) for i in range(6, 15)]
    # that unmatched paren will haunt you all day
    eq_(expected, set_theory.zset_fetch([('SET_A',)], db=db, min_score='(5',
        max_score='(15', start=0, end=-1, ids_only=True, reverse=False))


@with_setup(_setup_range)
def test_score_range_limit_offset():
    '''range queries should translate start & end to limit + offset notation
    '''
    expected = [str(i) for i in range(10, 13)]
    eq_(expected, set_theory.zset_fetch([('SET_A',)], db=db, min_score='5',
        max_score='15', start=5, end=7, ids_only=True, reverse=False))


@raises(ValueError)
def test_score_range_negative_offset():
    '''range queries don't allow negative end, except 0, -1 special case
    '''
    set_theory.zset_fetch([('SET_A',)], db=db, min_score='5', max_score='15',
                          start=5, end=-1, ids_only=True, reverse=False)


@raises(ValueError)
def test_score_range_zero_start_neg_offset():
    '''for range queries a negative end less than -1 should always be an error
    '''
    set_theory.zset_fetch([('SET_A',)], db=db, min_score='5', max_score='15',
                          start=0, end=-2, ids_only=True, reverse=False)


@with_setup(_compound_setup)
def test_scored_counts():
    """Ensure that counts work with score ranges"""
    count = set_theory.zset_count([('TEST_1',), ('TEST_3',)], db=db,
                                  min_score=58, max_score=79, ttl=5,
                                  operator="union")
    eq_(count, 22)


@with_setup(_compound_setup)
def test_scored_counts_fetch():
    """Ensure that counts work with score ranges through zset_fetch"""
    count = set_theory.zset_fetch([('TEST_1',), ('TEST_3',)], db=db,
                                  count=True, min_score=58, max_score=79,
                                  ttl=5, operator="union")
    eq_(count, 22)


@with_setup(_compound_setup)
def test_counts_no_cache():
    """Ensure that counts dont cache with ttl=0"""
    count = set_theory.zset_count([('TEST_1',), ('TEST_3',)], db=db, ttl=0,
                                  operator="union")
    assert(count)
    kh, cache_created = set_theory.zset_cache([('TEST_1',), ('TEST_3',)],
                                              db=db, operator="union")
    assert(cache_created)


@with_setup(_compound_setup)
def test_range_no_cache():
    """Ensure that range dont cache with ttl=0"""
    result = set_theory.zset_range([('TEST_1',), ('TEST_3',)], db=db, ttl=0,
                                   operator="union", start=0, end=-1)
    assert(result)
    kh, cache_created = set_theory.zset_cache([('TEST_1',), ('TEST_3',)],
                                              db=db, operator="union")
    assert(cache_created)


@with_setup(_compound_setup)
def test_scored_range():
    """Ensure that range with min and max scores works in standalone method"""
    results = set_theory.zset_range([('TEST_1',), ('TEST_3',)], db=db, ttl=5,
                                    operator="union", start=0, end=-1,
                                    min_score=59, max_score=78)
    eq_(len(results), 20)
    eq_(results[19], u'9')


@with_setup(_compound_setup)
def test_scored_range_first_page():
    """Ensure that range with min and max scores works with page 0"""
    results = set_theory.zset_range([('TEST_1',), ('TEST_3',)], db=db, ttl=5,
                                    operator="union", start=0, end=5,
                                    min_score=59, max_score=78)
    eq_(len(results), 6)
    eq_(results[0], u'28')


@with_setup(_compound_setup)
def test_scored_range_last_page():
    """Ensure that range with min and max scores works with page 0"""
    results = set_theory.zset_range([('TEST_1',), ('TEST_3',)], db=db, ttl=5,
                                    operator="union", start=15, end=19,
                                    min_score=59, max_score=78)
    eq_(len(results), 5)
    eq_(results[4], u'9')


@with_setup(_compound_setup)
def test_range_with_scores():
    """Ensure that range with min and max scores works in standalone method"""
    results = set_theory.zset_range([('TEST_1',), ('TEST_3',)], db=db, ttl=5,
                                    operator="union", start=0, end=-1,
                                    withscores=True)
    eq_(len(results), 30)
    eq_(results[0], (u'29', 79.0))


@with_setup(_compound_setup)
def test_scored_range_no_reverse():
    """Ensure that range with min and max scores works in standalone method
    without reverse"""
    results = set_theory.zset_range([('TEST_1',), ('TEST_3',)], db=db, ttl=5,
                                    operator="union", start=0, end=-1,
                                    min_score=59, max_score=78, reverse=False)
    eq_(len(results), 20)
    eq_(results[0], u'9')


@attr('slow')
@with_setup(_compound_setup)
def test_counts_with_expiry():
    """Ensure that adding a ttl will expire a cache created by calling count"""
    count = set_theory.zset_count([('TEST_1',), ('TEST_3',)], db=db, ttl=1,
                                  operator="union")
    eq_(count, 30)
    time.sleep(2)
    key_hash, cache_created = set_theory.zset_cache([('TEST_1',), ('TEST_3',)],
                                                    db=db, ttl=1,
                                                    operator="union")
    assert(cache_created)


@attr('slow')
@with_setup(_compound_setup)
def test_range_with_expiry():
    """Ensure that adding a ttl will expire a cache created by calling range"""
    set_theory.zset_range([('TEST_1',), ('TEST_3',)], db=db,
                          operator="union", ttl=1, start=0, end=-1)
    time.sleep(2)
    _, cache_created = set_theory.zset_cache([('TEST_1',), ('TEST_3',)], db=db,
                                             ttl=1, operator="union")
    assert(cache_created)


@attr('slow')
@with_setup(_compound_setup)
def test_zset_fetch_with_expiry():
    """Ensure that adding a ttl will expire a cache created by calling fetch"""
    set_theory.zset_fetch([('TEST_1',), ('TEST_3',)], db=db, operator="union",
                          ttl=1, return_key=True)
    time.sleep(2)
    _, cache_created = set_theory.zset_cache([('TEST_1',), ('TEST_3',)],
                                             db=db, ttl=1,
                                             operator="union")
    assert(cache_created)
