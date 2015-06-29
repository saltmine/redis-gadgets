"""Tests for the unique tracking tool
"""
import datetime
import redis
from nose.tools import eq_

from redis_gadgets import unique_count


class TestUniqueTracking(object):
    """Test the id mapping tools
    """
    @classmethod
    def setup_class(cls):
        cls.con = redis.Redis(db=15)  # use high db for testing
        cls.today = datetime.datetime.now()
        cls.uc = unique_count.RedisUniqueCount(cls.con)

    def setup(self):
        for key in self.con.keys(pattern='redis_gadgets:*'):
            self.con.delete(key)

    def test_offsets_are_sequential(self):
        """Bit offsets generated for a given type are sequential
        """
        # eq_(self.prd2_offset - self.prd1_offset, 1)
        eq_(self.uc.map_id_to_offset('id1'),
            self.uc.map_id_to_offset('id2') - 1)

    def test_back_mapping(self):
        """Can look up the outland_id for a given type, offset pair
        """
        key = 'id1'
        offset = self.uc.map_id_to_offset(key)
        eq_(self.uc.map_offset_to_id(offset), key)

    def test_type_space(self):
        """Different outland ID types have indpendent bit offset sequences
        """
        # this is slightly brittle, but should work
        key_template = 'something_%s'
        for n in range(10):
            key = key_template % n
            eq_(self.uc.map_id_to_offset(key, namespace='first'),
                self.uc.map_id_to_offset(key, namespace='second'))

    def test_stable_offsets(self):
        """Repeated map calls for the same id yield the same offset
        """
        original_offset = self.uc.map_id_to_offset('id1')
        for _ in range(10):
            eq_(original_offset, self.uc.map_id_to_offset('id1'))

    def test_events_are_counted(self):
        """unique event counts reflect track calls
        """
        original_count = self.uc.get_count(self.today, self.today, 'event1',
                                           namespace='users')
        eq_(original_count, 0)
        self.uc.track_event('event1', 'id1', namespace='users')
        new_count = self.uc.get_count(self.today, self.today, 'event1',
                                      namespace='users')
        eq_(new_count, 1)

    def test_events_are_unique(self):
        """unique event tracking is, you know, unique
        """
        eq_(self.uc.get_count(self.today, self.today, 'event2'), 0)
        self.uc.track_event('event2', 'usr1')
        self.uc.track_event('event2', 'usr1')
        self.uc.track_event('event2', 'usr1')
        self.uc.track_event('event2', 'usr1')
        self.uc.track_event('event2', 'usr1')
        eq_(self.uc.get_count(self.today, self.today, 'event2'), 1)
        self.uc.track_event('event2', 'usr2')
        eq_(self.uc.get_count(self.today, self.today, 'event2'), 2)

    def test_same_type_different_event(self):
        """Different events do not change each others count
        """
        eq_(self.uc.get_count(self.today, self.today, 'event3a', 'users'), 0)
        self.uc.track_event('event3b', 'usr1')
        eq_(self.uc.get_count(self.today, self.today, 'event3a', 'users'), 0)

    def test_different_type_same_event(self):
        """Events are namespaced by id type
        """
        eq_(self.uc.get_count(self.today, self.today, 'event4', 'users'), 0)
        self.uc.track_event('event4', 'prd1')
        eq_(self.uc.get_count(self.today, self.today, 'event4', 'users'), 0)

    def test_multi_day_rollup(self):
        """can roll up unique counts for several days
        """
        eq_(self.uc.get_count(self.today, self.today, 'event5'), 0)
        dt = self.today
        for i in xrange(5):
            self.uc.track_event("event5", 'usr1', event_time=dt)
            dt = dt - datetime.timedelta(days=1)
            self.uc.track_event("event5", 'usr2', event_time=dt)
            dt = dt - datetime.timedelta(days=1)
        eq_(self.uc.get_count(self.today, self.today, 'event5'), 1)
        eq_(self.uc.get_count(dt, self.today, 'event5'), 2)

    def test_start_end_order(self):
        """When getting unique counts, start & end date can be in any order
        """
        eq_(self.uc.get_count(self.today, self.today, 'event6'), 0)
        dt = self.today
        for i in xrange(5):
            self.uc.track_event("event6", 'usr1', event_time=dt)
            dt = dt - datetime.timedelta(days=1)
            self.uc.track_event("event6", 'usr2', event_time=dt)
            dt = dt - datetime.timedelta(days=1)
        eq_(self.uc.get_count(self.today, dt, 'event6'), 2)
        eq_(self.uc.get_count(dt, self.today, 'event6'), 2)
