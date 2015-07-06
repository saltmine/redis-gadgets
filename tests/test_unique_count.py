"""Tests for the unique tracking tool
"""
import datetime
import redis
from nose.tools import eq_

from redis_gadgets import unique_count


ITERATIONS = 10


class TestUniqueTracking(object):
    """Test the id mapping tools
    """
    @classmethod
    def setup_class(cls):
        cls.con = redis.Redis(db=15)  # use high db for testing
        cls.today = datetime.datetime.now()
        cls.uc = unique_count.RedisUniqueCount(cls.con)

    def setup(self):
        for key in self.con.keys(pattern=unique_count.BASE_NAMESPACE + '*'):
            self.con.delete(key)

    def test_offsets_are_sequential(self):
        """Bit offsets generated for a given type are sequential
        """
        last_offset = self.uc.map_id_to_offset('start')
        for n in range(ITERATIONS):
            new_offset = self.uc.map_id_to_offset('id%s' % n)
            eq_(new_offset - 1, last_offset)
            last_offset = new_offset

    def test_back_mapping(self):
        """Can look up the id for a given offset, id pair
        """
        offset_data = []
        for n in range(ITERATIONS):
            key = 'id%s' % n
            offset = self.uc.map_id_to_offset(key)
            offset_data.append((offset, key))

        for offset, key in offset_data:
            eq_(self.uc.map_offset_to_id(offset), key)

    def test_type_space(self):
        """Different namespace have independent bit offset sequences,
        if incremented simultaneously, the offset difference should be
        constant.
        """
        key_template = 'something_%s'
        key = key_template % 'original'

        original_diff = (self.uc.map_id_to_offset(key, namespace='first') -
                         self.uc.map_id_to_offset(key, namespace='second'))

        for n in range(ITERATIONS):
            key = key_template % n
            diff = (self.uc.map_id_to_offset(key, namespace='first') -
                    self.uc.map_id_to_offset(key, namespace='second'))
            eq_(diff, original_diff)

    def test_stable_offsets(self):
        """Repeated map calls for the same id yield the same offset
        """
        original_offset = self.uc.map_id_to_offset('id1')
        for _ in range(ITERATIONS):
            eq_(original_offset, self.uc.map_id_to_offset('id1'))

    def test_events_are_counted(self):
        """unique event counts reflect track calls
        """
        event_name = 'event1'
        namespace = 'users'
        id_list = []
        original_count = self.uc.get_count(self.today, self.today, event_name,
                                           namespace=namespace)
        eq_(original_count, 0, "GUARD")

        for n in range(1, ITERATIONS):
            native_id = 'id%s' % n
            id_list.append(native_id)
            self.uc.track_event(
                    event=event_name,
                    native_id=native_id,
                    namespace=namespace
            )
            new_count = self.uc.get_count(self.today, self.today, event_name,
                                          namespace='users')
            eq_(new_count, n)

    def test_events_are_unique(self):
        """unique event tracking is, you know, unique
        """
        eq_(self.uc.get_count(self.today, self.today, 'event2'), 0, 'GUARD')

        # For loop should only increment count by 1
        for _ in range(ITERATIONS):
            self.uc.track_event('event2', 'usr1')

        eq_(self.uc.get_count(self.today, self.today, 'event2'), 1)

        # For loop should only increment count by 1
        for _ in range(ITERATIONS):
            self.uc.track_event('event2', 'usr2')
        eq_(self.uc.get_count(self.today, self.today, 'event2'), 2)

    def test_same_type_different_event(self):
        """Different events do not change each others count
        """
        eq_(self.uc.get_count(self.today, self.today, 'event3a', 'users'), 0,
            "GUARD")
        self.uc.track_event('event3b', 'usr1', namespace='users')
        eq_(self.uc.get_count(self.today, self.today, 'event3a', 'users'), 0)
        eq_(self.uc.get_count(self.today, self.today, 'event3b', 'users'), 1)

    def test_different_namespace_same_event(self):
        """Events are namespaced by id type
        """
        eq_(self.uc.get_count(self.today, self.today, 'event4',
                              namespace='users'), 0, 'GUARD')
        self.uc.track_event('event4', 'prd1', namespace='global')
        eq_(self.uc.get_count(self.today, self.today, 'event4',
                              namespace='users'), 0)

    def test_multi_day_rollup(self):
        """can roll up unique counts for several days
        """
        eq_(self.uc.get_count(self.today, self.today, 'event5'), 0)
        dt = self.today
        for _ in range(ITERATIONS):
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
        for _ in range(ITERATIONS):
            self.uc.track_event("event6", 'usr1', event_time=dt)
            dt = dt - datetime.timedelta(days=1)
            self.uc.track_event("event6", 'usr2', event_time=dt)
            dt = dt - datetime.timedelta(days=1)
        eq_(self.uc.get_count(self.today, dt, 'event6'), 2)
        eq_(self.uc.get_count(dt, self.today, 'event6'), 2)

    def test_no_data_days(self):
        """Days with no data don't impact counts
        """
        yesterday = self.today - datetime.timedelta(days=1)
        tomorrow = self.today + datetime.timedelta(days=1)
        self.uc.track_event('event7', 'a', event_time=self.today)
        self.uc.track_event('event7', 'b', event_time=self.today)
        eq_(self.uc.get_count(self.today, self.today, 'event7'), 2, "GUARD")
        eq_(self.uc.get_count(yesterday, self.today, 'event7'), 2)
        eq_(self.uc.get_count(self.today, tomorrow, 'event7'), 2)
        eq_(self.uc.get_count(yesterday, tomorrow, 'event7'), 2)

    def test_get_current_offset(self):
        for namespace in ('global', 'somethingelse', 'users1'):
            eq_(self.uc.get_current_offset(namespace), 0)
            for n in range(1, ITERATIONS):
                self.uc.track_event('event', native_id=n,
                                    event_time=self.today, namespace=namespace)
                eq_(self.uc.get_current_offset(namespace), n)
