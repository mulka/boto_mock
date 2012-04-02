from boto_mock.dynamodb.schema import Schema

class Table(object):

    def __init__(self, layer2, response=None):
        self.layer2 = layer2
        self._dict = {}
        self.update_from_response(response)

    def __repr__(self):
        return 'Table(%s)' % self.name

    @property
    def name(self):
        return self._dict['TableName']

    @property
    def create_time(self):
        return self._dict['CreationDateTime']

    @property
    def status(self):
        return self._dict['TableStatus']

    @property
    def item_count(self):
        return self._dict.get('ItemCount', 0)

    @property
    def size_bytes(self):
        return self._dict.get('TableSizeBytes', 0)

    @property
    def schema(self):
        return self._schema

    def update_from_response(self, response):
        if 'Table' in response:
            self._dict.update(response['Table'])
        elif 'TableDescription' in response:
            self._dict.update(response['TableDescription'])
        if 'KeySchema' in self._dict:
            self._schema = Schema(self._dict['KeySchema'])

    def delete(self):
        self.layer2.delete_table(self)
