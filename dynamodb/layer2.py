from boto_mock.dynamodb.layer1 import Layer1
from boto_mock.dynamodb.item import Item
from boto_mock.dynamodb.table import Table
from boto_mock.dynamodb.schema import Schema
from boto.dynamodb.batch import BatchWriteList
from boto_mock.dynamodb.types import get_dynamodb_type, dynamize_value
    
class Layer2(object):
    def __init__(self):
        self.layer1 = Layer1()
        
    def get_table(self, name):
        response = self.layer1.describe_table(name)
        return Table(self,  response)
        
    def create_table(self, name, schema, read_units, write_units):
        response = self.layer1.create_table(name, schema.dict)
        return Table(self,  response)

    def delete_table(self, table):
        response = self.layer1.delete_table(table.name)
        table.update_from_response(response)

    def create_schema(self, hash_key_name, hash_key_proto_value,
                    range_key_name=None, range_key_proto_value=None):
        schema = {}
        hash_key = {}
        hash_key['AttributeName'] = hash_key_name
        hash_key_type = get_dynamodb_type(hash_key_proto_value)
        hash_key['AttributeType'] = hash_key_type
        schema['HashKeyElement'] = hash_key
        if range_key_name and range_key_proto_value is not None:
            range_key = {}
            range_key['AttributeName'] = range_key_name
            range_key_type = get_dynamodb_type(range_key_proto_value)
            range_key['AttributeType'] = range_key_type
            schema['RangeKeyElement'] = range_key
        return Schema(schema)

    def put_item(self, item):
        response = self.layer1.put_item(item.table.name,
                                        item)
        return response
    
    def delete_item(self, item):
        return self.layer1.delete_item(item.table.name, item.hash_key, item.range_key)

    def query(self, table, hash_key, range_key_condition=None,
              scan_index_forward=True):
        if range_key_condition and range_key_condition.__class__.__name__ == 'BETWEEN':
            start = range_key_condition.v1
            end = range_key_condition.v2
        else:
            start = None
            end = None
        response = True
        n = 0
        response = self.layer1.query(table.name,
                                     hash_key,
                                     start, end, 
                                     scan_index_forward)
        for item in response['Items']:
            yield Item(table, attrs=item)
        
    def scan(self, table):
        response = self.layer1.scan(table.name)
        if response:
            for item in response['Items']:
                yield Item(table, attrs=item)
    
    def new_batch_write_list(self):
        return BatchWriteList(self)

    def batch_write_item(self, batch_list):
        for batch in batch_list:
            for item in batch.puts:
                self.put_item(item)
        return {'UnprocessedItems': {}}