"""Prefix matching with secondary score ordering
"""
import logging

from .set_theory import zset_fetch

# TODO: Base prefix for keys
# TODO: Document/expand kwargs in get_matches
# TODO: Make this a class
# TODO: Make redis_conn, index_name object attributes
# TODO: Make case sensitivity optional
# TODO: Secondary score type as namedtuple('score', 'direction')

log = logging.getLogger(__name__)


def build_prefix_index(redis_conn, index_name, search_string, some_id,
                       min_prefix_len=1, operator='add', secondary_scores=None,
                       length_score=True):
    '''Indexes some_id by every prefix of search_string longer than
    min_prefix_len.

    :param index_name: The namespace for the collection of prefix indexes this
                       belongs to.  Prepended to the redis key name.
    :param search_string: The string you want prefixes of to find some_id
    :param some_id: The id to find
    :param min_prefix_length: Shortest prefix to index
    :param secondary_scores: Optionally, a list of pairs of the form
           (score, [asc|desc]) for secondary sorting of the results
    :param length_score: set to true to use edit distance as the most
           significant score in a secondary score list.  No effect if
           secondary_scores is None
    :param redis_conn: Redis connection on which to operate.
    '''
    if operator not in ('add', 'rem'):
        raise ValueError("unknown operator: %s" % operator)
    if search_string is None or len(search_string) < min_prefix_len:
        return
    search_string = search_string.lower().strip()
    if secondary_scores:
        secondary_scores = list(secondary_scores)
        if length_score:
            secondary_scores.insert(0, (len(search_string), 'asc'))
        score = compute_compound_scores(secondary_scores, 100000)
    else:
        score = len(search_string)
    # pipe = redis_conn.pipeline()
    for i in xrange(min_prefix_len, len(search_string) + 1):
        key = "%s:%s" % (index_name, search_string[0:i])
        if operator == 'add':
            log.debug("adding id %s to key %s with score %s", some_id, key,
                      score)
            redis_conn.zadd(key, score, some_id)
        elif operator == 'rem':
            redis_conn.zrem(key, some_id)
    # pipe.execute()


def get_matches(search_string, db, index_name, **kwargs):
    """Return an ordered list of matches for the given search string
    """
    return zset_fetch([('%s:%s' % (index_name, term.lower()),)
                       for term in search_string.split()],
                      reverse=False, ids_only=True, operator='intersect',
                      db=db, **kwargs)


def compute_compound_scores(score_list, score_band_width=100):
    '''Computes multi-layered sorting scores.

    :param score_list: Is a list of the subscores to combine.  Each entry
        should be a tuple of the form:
         ((score_1, ['desc'|'asc']), ..., (score_N, ['desc'|'asc']) )
         where each of the score_i's is an unweighted score, in precedence
         order (e.g. score_2 is a higher sort order than score_3) and each
         score_i is paired with a direction, which is either 'desc' for
         decending sort (i.e. higher values first) or 'asc' for ascending sort
         (i.e. lower values first)
    :param score_band_width: how much "space" to allocate for each ordering.
         This should be higher than the highest possible component score.  It
         does not need to be a power of 10, but reading your compund scores
         will be easier if it is.

    :rtype: list of tuples of (key, compound_score)
    '''
    score = 0
    for sub_score, direction in score_list:
        score *= score_band_width
        # note on positive/negative values here:
        # by default, redis sorts zsets in ascending order (i.e. lowest score
        # first), so for ascending sort, we just add the value as expected.
        # for a descending sort, we need to invert that, so we subtract the
        # component score.
        if direction == 'desc':
            score -= sub_score
        elif direction == 'asc':
            score += sub_score
    return score
