def connect_dynamodb(**kwargs):
    from boto_mock.dynamodb.layer2 import Layer2
    return Layer2()