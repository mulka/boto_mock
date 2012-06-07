class BotoServerError(Exception):
    pass

class DynamoDBResponseError(BotoServerError):
    pass