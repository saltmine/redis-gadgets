"""
Manage queries against redis ordered sets, possibly involving unions &
intersections of some complexity
"""
# import hashlib
import logging
import os  # for generating thread-safe key names
import socket  # for generating thread-safe key names
import threading  # for generating thread-safe key names

log = logging.getLogger(__name__)

#If there is a race condition causing us to have to re-run zset_fetch, retry at
#most this number of times
MAX_RETRIES = 2
MAX_CACHE_SECONDS = 60 * 5  # no ZCACHE can live longer than this many seconds

# TODO: Turn this into a class
# TODO: clean up/document kwargs usage
# TODO: get rid of db_context, make db class param
# TODO: get rid of callback & ids only arguments - from the perspective of this
#       tool, returning IDs is the only correct behavior
# TODO: Deal with StrictRedis/Redis order argument difference


def _unique_id():
    """Returns a unique id to be used as a key suffix for thread safety
    """
    return "%s-%s-%s" % (socket.getfqdn(), os.getpid(),
                         threading.current_thread().ident)


def build_keys(bind_elements):
    """Build a dict of fully interpolated keys from a list of bind elts"""
    keys = {}
    for element in bind_elements:
        if len(element) == 1:
            keys[element[0]] = 1.0
        elif len(element) == 2:
            keys[element[0]] = element[1]
        else:
            raise ValueError('each element of bind_elements must be an '
                             'iterable with 1 to 2 elements')
    return keys


def build_key_hash(keys, operator, thread_local):
    """From the dict of query keys/values, generate a hash for cache mapping"""
    key_count = len(keys)
    if key_count == 1:
        key_hash = keys.keys()[0]  # don't make a hash, just use the single key
    elif key_count > 1:
        hash_chunks = []
        for k in sorted(keys.keys()):
            hash_chunks.append('%s*%s' % (k, keys[k]))
        if operator == "union":
            key_hash = " || ".join(hash_chunks)
        else:
            key_hash = " && ".join(hash_chunks)
        key_hash = "ZCACHE:(%s)" % key_hash
        if thread_local:
            key_hash = "%s::%s" % (key_hash, _unique_id())
    else:
        raise ValueError('we cant build a key hash with no keys')
    #log.debug("hash before compression %s", key_hash)
    #key_hash = "ZCACHE:%s" % hashlib.md5(key_hash).hexdigest()
    return key_hash


def zset_cache(bind_elements, db, db_context=None, operator="union",
               aggregate="max", cachebust=False, thread_local=False, **kwargs):
    """Perform the operation described and store the result in redis. If called
    subsequently before the cache is expired then the operation will be
    bypassed.  Returns a tuple containing a key_hash of the result of this
    query, and a boolean representing if the result of the operation was cached
    i.e.
    True : There was no previous cache. One was created and stored at key_hash
    False : Either the cache existed or did not need to be created (one key
    only) Note that it may be your responsibility to expire the cache if it was
    newly created
    """
    #a dict of fully interpolated redis keys and their weights
    keys = build_keys(bind_elements)
    log.debug("key combination %s", keys)
    key_hash = build_key_hash(keys, operator, thread_local)
    log.debug("key hash %s", key_hash)
    cache_created = False
    if len(keys) > 1:
        cache_exists = db.exists(key_hash)
        if cache_exists and not cachebust:
            log.debug("totally in cache, hitting it")
        else:
            # import pdb; pdb.set_trace()
            log.debug("not in cache")
            cache_created = True
            pipe = db.pipeline()
            if operator == "intersect":
                log.debug("Running zinterstore to key %s", key_hash)
                pipe.zinterstore(key_hash, keys, aggregate=aggregate)
            elif operator == "union":
                log.debug("Running zunionstore to key %s", key_hash)
                pipe.zunionstore(key_hash, keys, aggregate=aggregate)
            pipe.expire(key_hash, MAX_CACHE_SECONDS)
            pipe.execute()
    return key_hash, cache_created


def zset_count(bind_elements, db, min_score=None, max_score=None,
               db_context=None, operator="union", ttl=0, aggregate="max",
               thread_local=False, **kwargs):
    """Perform 'operation' on bind_elements (or access its cache) and return
    the size of the set.
    Note that the count operation will be linear if max and min scores are not
    provided
    """
    key_hash, cache_created = zset_cache(bind_elements, db=db,
                                         db_context=db_context,
                                         operator=operator,
                                         aggregate=aggregate,
                                         thread_local=thread_local, **kwargs)
    # if the user just wants a count, but we may still have cache to clean up
    log.debug("getting count on (%s)", key_hash)
    count = 0
    if min_score or max_score:
        log.debug("using zcount")
        count = db.zcount(key_hash, min_score, max_score)
    else:
        log.debug("using zcard")
        count = db.zcard(key_hash)
    if cache_created:
        if not ttl:
            log.debug("no ttl, removing temp store")
            db.delete(key_hash)
        else:
            log.debug("setting ttl on %s to %d seconds", key_hash, ttl)
            db.expire(key_hash, min(ttl, MAX_CACHE_SECONDS))
    return count


def zset_range(bind_elements, db, start=None, end=None, min_score=None,
               max_score=None, reverse=True, withscores=False, db_context=None,
               operator="union", ttl=0, callback=None, aggregate="max",
               retries=MAX_RETRIES, thread_local=False, **kwargs):
    """Perform operation described in bind_elements then cache and return the
    result, subject to all suplied paramaters.
    """
    result = []
    key_hash, cache_created = zset_cache(bind_elements, db=db,
                                         db_context=db_context,
                                         operator=operator,
                                         aggregate=aggregate,
                                         thread_local=thread_local, **kwargs)
    if min_score or max_score:
        limit = None
        offset = None
        if start != 0 or end != -1:
            # 0, -1 is a special case meaning "the whole set"
            offset = start
            # add 1 to make limit work inclusively like start and end
            limit = end - start + 1
        log.debug("fetching scores %s to %s from %s of (%s) "
                  "limit: %s offset: %s reverse: %s",
                  min_score, max_score, operator, key_hash, limit, offset,
                  reverse)
        if reverse:
            # NB: revrange expects max first, range expects min first
            result = db.zrevrangebyscore(key_hash, max_score, min_score,
                                         start=offset, num=limit,
                                         withscores=withscores)
        else:
            result = db.zrangebyscore(key_hash, min_score, max_score,
                                      start=offset, num=limit,
                                      withscores=withscores)
    else:
        log.debug("fetching %s to %s from %s of (%s) reverse: %s", start, end,
                  operator, key_hash, reverse)
        if reverse:
            result = db.zrevrange(key_hash, start, end, withscores=withscores)
        else:
            result = db.zrange(key_hash, start, end, withscores=withscores)
        log.debug("found %d entries", len(result))
    if len(bind_elements) > 1 and not result:
      #at this point we know that the key has expired since we last checked it
        if retries <= 0:
            log.warn('Exceeded maximum number of retries for zset_fetch.')
        else:
            log.info('Caught race condition. Retrying ZSET Fetch...')
            retries -= 1
            log.debug('zset_range vars: %s', vars())
            return zset_range(bind_elements, start=start, end=end,
                              min_score=min_score, max_score=max_score,
                              reverse=reverse, withscores=withscores, db=db,
                              db_context=db_context, operator=operator,
                              ttl=ttl, callback=callback, aggregate=aggregate,
                              retries=retries - 1, thread_local=thread_local,
                              **kwargs)
    if cache_created:
        if not ttl:
            log.debug("no ttl, removing temp store")
            db.delete(key_hash)
        else:
            log.debug("setting ttl on %s to %d seconds", key_hash, ttl)
            db.expire(key_hash, min(ttl, MAX_CACHE_SECONDS))
    if callback:
        return callback(result, **kwargs)
    return result


def zset_fetch(bind_elements, db, start=None, end=None, min_score=None,
               max_score=None, count=False, reverse=True, ids_only=False,
               withscores=False, db_context=None, operator="union", ttl=0,
               callback=None, return_key=False, aggregate="max",
               retries=MAX_RETRIES, thread_local=False,
               **kwargs):
    """General purpose tool for doing zset traversal in redis without abusing
    local memory too much. To call it, you send in iterables consisting of
      * a string representing a zset's key
      * an optional variable to interpolate into the first element of the pair
      * an optional floating point weight multiplier

    If there is more than 1 pair, then the operator kwarg comes into play.
    Currently, union and intersection are supported via the native ZUNIONSTORE
    and ZINTERSTORE redis commands, respectively.


    As an extra precaution against race conditions for cached results,
    zset_fetch can re-call itself up to MAX_RETRIES times to ensure that there
    are valid values in key_hash. If this threshold is exceeded, we simply
    return an empty response. -- jp Mon Nov 26 13:12:47 EST 2012
    """

    if start is None and end is None:
        if not count and not return_key:
            raise ValueError("Non-meta queries must specify start and end")

    if count or return_key:
        if start is not None or end is not None:
            raise ValueError("meta queries may not specify start and end")

    if return_key and (min_score is not None or max_score is not None):
        raise ValueError("return key calls may not specify min_score or "
                         "max_score")

    if count and ids_only:
        raise ValueError("`count` and `ids_only` cannot be set at the same "
                         "time")

    if return_key and not ttl:
        raise ValueError("return_key will return a temporary key and will not "
                         "work with a 0 ttl")

    if min_score or max_score:
        if end < 0 and not (start == 0 and end == -1) and not count:
            # 0, -1 is a special case to indicate the full range, and is
            # handled specifically in the score range code
            raise ValueError("Score range queries do not support negative end "
                             "points")

    if ids_only:
        # ids_only is the default behavior of set theory and should be
        # deprecated
        callback = None

    if count:
        return zset_count(bind_elements, min_score=min_score,
                          max_score=max_score, db=db, db_context=db_context,
                          operator=operator, ttl=ttl, aggregate=aggregate,
                          thread_local=thread_local, **kwargs)
    elif return_key:
        key_hash, cached = zset_cache(bind_elements, db=db,
                                      db_context=db_context, operator=operator,
                                      aggregate=aggregate, **kwargs)
        if cached:
            if not ttl:
                log.debug("no ttl, removing temp store")
                #cant't get here
                #c.delete(key_hash)
            else:
                log.debug("setting ttl on %s to %d seconds", key_hash, ttl)
                db.expire(key_hash, min(ttl, MAX_CACHE_SECONDS))
        return key_hash
    else:
        return zset_range(bind_elements, start=start, end=end,
                          min_score=min_score, max_score=max_score,
                          reverse=reverse, withscores=withscores, db=db,
                          db_context=db_context, operator=operator, ttl=ttl,
                          callback=callback, aggregate=aggregate,
                          retries=retries, thread_local=thread_local, **kwargs)