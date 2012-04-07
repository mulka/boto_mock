from boto.dynamodb.types import dynamize_value


class Condition(object):
    """
    Base class for conditions.  Doesn't do a darn thing but allows
    is to test if something is a Condition instance or not.
    """

    pass


class ConditionNoArgs(Condition):
    """
    Abstract class for Conditions that require no arguments, such
    as NULL or NOT_NULL.
    """

    def __repr__(self):
        return '%s' % self.__class__.__name__

    def to_dict(self):
        return {'ComparisonOperator': self.__class__.__name__}


class ConditionOneArg(Condition):
    """
    Abstract class for Conditions that require a single argument
    such as EQ or NE.
    """

    def __init__(self, v1):
        self.v1 = v1

    def __repr__(self):
        return '%s:%s' % (self.__class__.__name__, self.v1)

    def to_dict(self):
        return {'AttributeValueList': [dynamize_value(self.v1)],
                'ComparisonOperator': self.__class__.__name__}


class ConditionTwoArgs(Condition):
    """
    Abstract class for Conditions that require two arguments.
    The only example of this currently is BETWEEN.
    """

    def __init__(self, v1, v2):
        self.v1 = v1
        self.v2 = v2

    def __repr__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, self.v1, self.v2)

    def to_dict(self):
        values = (self.v1, self.v2)
        return {'AttributeValueList': [dynamize_value(v) for v in values],
                'ComparisonOperator': self.__class__.__name__}

"""
class EQ(ConditionOneArg):

    pass


class NE(ConditionOneArg):

    pass


class LE(ConditionOneArg):

    pass


class LT(ConditionOneArg):

    pass


class GE(ConditionOneArg):

    pass


class GT(ConditionOneArg):

    pass


class NULL(ConditionNoArgs):

    pass


class NOT_NULL(ConditionNoArgs):

    pass


class CONTAINS(ConditionOneArg):

    pass


class NOT_CONTAINS(ConditionOneArg):

    pass


class BEGINS_WITH(ConditionOneArg):

    pass


class IN(ConditionOneArg):

    pass


class BEGINS_WITH(ConditionOneArg):

    pass
"""

class BETWEEN(ConditionTwoArgs):

    pass
