def connect_dynamodb(filename):
    from boto_mock.dynamodb.layer2 import Layer2
    return Layer2(filename)