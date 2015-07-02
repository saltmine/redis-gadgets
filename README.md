# redis-gadgets

This is a collection of helper tools built on top of redis.

[![Build Status](https://travis-ci.org/saltmine/redis-gadgets.svg)](https://travis-ci.org/saltmine/redis-gadgets)

# Note On Testing

Most of the code here is a thin wrapper over a few redis calls.  Testing it
without connecting to redis is pretty useless, as such.  To that end, the tests
make (default) redis connections to pass into the barious tools.  These tests
are destructive.  If you're running the tests, either override the redis
connection or run it on a machine where you don't mind clobbering a redis db.

Pull requests with a better way to do this are welcome.


# Unique tracker

This tool uses redis bit sets to track unique events per day.  The tool maps
between arbitrary string ids and bit offsets, within a namespace (i.e. a given
id-namespace pair maps to a unique bit offset).  This allows for a relatively
compact offset space while still enabling tracking of multiple things, e.g.
unique users viewing pages and unique pages viewed.  There is a default
namespace 'global' if you don't need this feature.

Events are tracked on day granularity, so the same id-event pairing will only
be counted once per day. Counts can then be prodouced for arbitrary date
ranges, using bitwise logical operators.  These rollups are cached with a redis
expiration timer.
