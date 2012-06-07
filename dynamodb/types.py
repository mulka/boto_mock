def is_num(n):
    types = (int, long, float, bool)
    return isinstance(n, types) or n in types


def is_str(n):
    return isinstance(n, basestring) or (isinstance(n, type) and issubclass(n, basestring))


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

def dynamize_value(val):
    def _str(val):
        if isinstance(val, bool):
            return str(int(val))
        return str(val)

    dynamodb_type = get_dynamodb_type(val)
    if dynamodb_type == 'N':
        val = {dynamodb_type: _str(val)}
    elif dynamodb_type == 'S':
        val = {dynamodb_type: val}
    elif dynamodb_type == 'NS':
        val = {dynamodb_type: [str(n) for n in val]}
    elif dynamodb_type == 'SS':
        val = {dynamodb_type: [n for n in val]}
    return val