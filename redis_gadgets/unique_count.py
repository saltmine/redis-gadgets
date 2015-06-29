"""
Fast redis backed tool to count unique outland ids per event, using redis bit
sets
"""

import datetime
import logging

log = logging.getLogger(__name__)


TO_OFFSET_TEMPLATE = "redis_gadgets:%s_id_to_offset"
TO_ID_TEMPLATE = "redis_gadgets:offset_to_%s_id"
SEQUENCE_TEMPLATE = "redis_gadgets:%s_current_offset"
DAY_KEY_TEMPLATE = "redis_gadgets:%s:%s:%s"


class RedisUniqueCount(object):

    """Track unique counts using redis bit strings"""

    def __init__(self, redis_conn):
        """Bind a counter to a redis connection

        :param redis_conn: Redis connection to operate on

        """
        self._redis_conn = redis_conn

    def map_id_to_offset(self, native_id, namespace='global'):
        """Return the type-dependent bit offset for the given native_id
        """
        to_offset_key = TO_OFFSET_TEMPLATE % namespace
        to_id_key = TO_ID_TEMPLATE % namespace
        sequence_key = SEQUENCE_TEMPLATE % namespace
        script = self._redis_conn.register_script("""
                local offset
                offset = redis.call('hget', KEYS[1], ARGV[1])
                if not offset then
                    offset = redis.call('incr', KEYS[3])
                    redis.call('hset', KEYS[1], ARGV[1], offset)
                    redis.call('hset', KEYS[2], offset, ARGV[1])
                end
                return offset
                """)
        offset = script(keys=(to_offset_key, to_id_key, sequence_key),
                        args=(native_id,))
        offset = int(offset)
        log.debug("redis returned offset %s for id %s", offset, native_id)
        return offset

    def map_offset_to_id(self, offset, namespace='global'):
        """Get the id for the given offset.  We need namepsace here since
        different object types all have diferent bit sequences, to keep them
        compact.
        """
        to_id_key = TO_ID_TEMPLATE % namespace
        native_id = self._redis_conn.hget(to_id_key, offset)
        return native_id

    def __make_day_key(self, event, event_date, namespace='global'):
        """generate the key name for a given day
        """
        return DAY_KEY_TEMPLATE % (event, namespace, event_date.isoformat())

    def track_event(self, event, native_id, namespace='global',
                    event_time=None):
        """Track that the given event happened to the given id.  By default,
        use day granularity and the current time, but allow backdating data
        (e.g. for batch processing or testing)
        """
        if event_time is None:
            event_time = datetime.date.today()
        if isinstance(event_time, datetime.datetime):
            event_time = event_time.date()
        key = self.__make_day_key(event, event_time, namespace)
        offset = self.map_id_to_offset(native_id)

        self._redis_conn.setbit(key, offset, 1)

    def get_count(self, start_date, end_date, event, namespace='global'):
        """Get the count of uniques for the given event, of the given id type,
        for the given date range
        """
        if isinstance(start_date, datetime.datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime.datetime):
            end_date = end_date.date()

        if start_date == end_date:
            # special case - we can just read from an existing day key here
            log.debug("single key case")
            return self._redis_conn.bitcount(self.__make_day_key(event,
                                                                 start_date,
                                                                 namespace))

        if end_date < start_date:
            # be nice and accept out of order args
            start_date, end_date = end_date, start_date

        date_counter = start_date
        keys = []
        while date_counter < end_date:
            keys.append(self.__make_day_key(event, date_counter, namespace))
            date_counter = date_counter + datetime.timedelta(days=1)

        compound_key = "%s:%s:or:%s_to_%s" % (event, namespace,
                                              start_date.isoformat(),
                                              end_date.isoformat())
        log.debug("ORing keys %s into compound key %s", keys, compound_key)
        if not self._redis_conn.exists(compound_key):
            log.debug("Compound key not found, doing bit op")
            self._redis_conn.bitop('OR', compound_key, *keys)
            # expire stale counts in 10 min
            self._redis_conn.expire(compound_key, 10 * 60)
        return self._redis_conn.bitcount(compound_key)
