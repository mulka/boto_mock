def is_num(n):
    return isinstance(n, (int, long, float, bool))


def is_str(n):
    return isinstance(n, basestring)


def convert_num(s):
    if '.' in s:
        n = float(s)
    else:
        n = int(s)
    return n


def get_dynamodb_type(val):
    dynamodb_type = None
    if is_num(val):
        dynamodb_type = 'N'
    elif is_str(val):
        dynamodb_type = 'S'
    elif isinstance(val, (set, frozenset)):
        if False not in map(is_num, val):
            dynamodb_type = 'NS'
        elif False not in map(is_str, val):
            dynamodb_type = 'SS'
    if dynamodb_type is None:
        msg = 'Unsupported type "%s" for value "%s"' % (type(val), val)
        raise TypeError(msg)
    return dynamodb_type
