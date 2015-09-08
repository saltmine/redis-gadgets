"""
Collection of callables to transform datetimes into lower precision datetimes,
for a few common use cases.  Also serves as a template for users looking to
implement their own custom granularity
"""


def five_minute(dt):
    """Return a datetime representing dt truncated to a five minute interval

    :param dt: Arbitrary precision date time
    :type dt: datetime.datetime
    :returns: a datetime representing dt truncated to a five minute interval
    """
    interval = dt.minute - (dt.minute % 5)
    return dt.replace(minute=interval, second=0, microsecond=0)


def hourly(dt):
    """Return a datetime representing dt truncated to an hourly interval

    :param dt: Arbitrary precision date time
    :type dt: datetime.datetime
    :returns: a datetime representing dt truncated to an hourly interval
    """
    return dt.replace(minute=0, second=0, microsecond=0)


def daily(dt):
    """Return a datetime representing dt truncated to an hourly interval

    :param dt: Arbitrary precision date time
    :type dt: datetime.datetime
    :returns: a datetime representing dt truncated to an hourly interval
    """
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)
