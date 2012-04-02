import sqlite3

class Layer1(object):
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        
    def describe_table(self, table_name):
        return {'Table': {'TableName': table_name}}
        
    def create_table(self, table_name, schema):
        type_map = {'N': 'NUMERIC', 'S': 'TEXT'}
            
        columns = schema['HashKeyElement']['AttributeName'] + ' ' + type_map[schema['HashKeyElement']['AttributeType']]
        
        if 'RangeKeyElement' in schema:
            columns += ', ' + schema['RangeKeyElement']['AttributeName'] + ' ' + type_map[schema['RangeKeyElement']['AttributeType']]
        
        self.conn.execute('CREATE TABLE ' + table_name + ' (' + columns + ')')
        return {'Table': {'TableName': table_name}}

    def delete_table(self, table_name):
        self.conn.execute('DROP TABLE ' + table_name)
        return {'Table': {'TableName': table_name}}