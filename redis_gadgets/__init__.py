from collections import namedtuple
WeightedKey = namedtuple('WeightedKey', 'key weight')
WeightedKey.__new__.__defaults__ = (1.0,)  # Default weight
