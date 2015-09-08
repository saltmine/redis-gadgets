"""
Tests for granularity functions
"""

from datetime import datetime
import pytz
from nose.tools import eq_
from redis_gadgets import granularity as gr


class TestFiveMinute(object):
    def test_arbitrary_time(self):
        """Can truncate a datetime to the nearest 5 miutes
        """
        initial = datetime(2015, 4, 20, 4, 22, 17)
        expected = datetime(2015, 4, 20, 4, 20)
        actual = gr.five_minute(initial)
        eq_(actual, expected)

    def test_no_change_needed(self):
        """Can truncate a date time to itself, at 5 minute granularity
        """
        initial = datetime(2015, 4, 20, 4, 20, 00)
        expected = datetime(2015, 4, 20, 4, 20)
        actual = gr.five_minute(initial)
        eq_(actual, expected)

    def test_time_zone(self):
        """5 minute granularity truncation preserves time zone
        """
        initial = datetime(2015, 4, 20, 4, 22, 17,
                           tzinfo=pytz.timezone("US/Eastern"))
        expected = datetime(2015, 4, 20, 4, 20,
                            tzinfo=pytz.timezone("US/Eastern"))
        actual = gr.five_minute(initial)
        eq_(actual, expected)


class TestHourly(object):
    def test_arbitrary_time(self):
        """Can truncate a datetime to the nearest hour
        """
        initial = datetime(2015, 4, 20, 4, 22, 17)
        expected = datetime(2015, 4, 20, 4)
        actual = gr.hourly(initial)
        eq_(actual, expected)

    def test_no_change_needed(self):
        """Can truncate a date time to itself, at hour
        """
        initial = datetime(2015, 4, 20, 4, 0, 0)
        expected = datetime(2015, 4, 20, 4)
        actual = gr.hourly(initial)
        eq_(actual, expected)

    def test_time_zone(self):
        """Hourly granularity truncation preserves time zone
        """
        initial = datetime(2015, 4, 20, 4, 22, 17,
                           tzinfo=pytz.timezone("US/Eastern"))
        expected = datetime(2015, 4, 20, 4,
                            tzinfo=pytz.timezone("US/Eastern"))
        actual = gr.hourly(initial)
        eq_(actual, expected)


class TestDaily(object):
    def test_arbitrary_time(self):
        """Can truncate a datetime to the nearest day
        """
        initial = datetime(2015, 4, 20, 4, 22, 17)
        expected = datetime(2015, 4, 20)
        actual = gr.daily(initial)
        eq_(actual, expected)

    def test_no_change_needed(self):
        """Can truncate a date time to itself, at day granularity
        """
        initial = datetime(2015, 4, 20, 0, 0, 0)
        expected = datetime(2015, 4, 20)
        actual = gr.daily(initial)
        eq_(actual, expected)

    def test_time_zone(self):
        """day granularity truncation preserves time zone
        """
        initial = datetime(2015, 4, 20, 4, 22, 17,
                           tzinfo=pytz.timezone("US/Eastern"))
        expected = datetime(2015, 4, 20, tzinfo=pytz.timezone("US/Eastern"))
        actual = gr.daily(initial)
        eq_(actual, expected)
