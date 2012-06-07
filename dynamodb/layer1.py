from boto_mock.exception import DynamoDBResponseError
import sqlite3
import json

class Layer1(object):
    type_to_sqlite = {'N': 'NUMERIC', 'S': 'TEXT'}
    type_from_sqlite = {'NUMERIC': 'N', 'TEXT': 'S'}
    
    def __init__(self):
        self.conn = sqlite3.connect('dynamodb.db', check_same_thread = False)
        self.conn.isolation_level = None
        
    def _pragma(self, table_name):
        c = self.conn.cursor()
        c.execute('PRAGMA table_info(' + table_name + ')')
        rows = []
        for row in c:
            rows.append(row)
        c.close()
        return rows

    def describe_table(self, table_name):
        rows = self._pragma(table_name)
        if len(rows) == 0:
            raise DynamoDBResponseError('Requested resource not found: Table: ' + table_name + ' not found')

        key_schema = {
            "HashKeyElement":{"AttributeName": rows[1][1],"AttributeType": self.type_from_sqlite[rows[1][2]]}
        }
        
        if len(rows) > 2:
            key_schema["RangeKeyElement"] = {"AttributeName": rows[2][1],"AttributeType": self.type_from_sqlite[rows[2][2]]}
            
        return {'Table': {'TableName': table_name, 'KeySchema': key_schema}}
        
    def create_table(self, table_name, schema):
        hash_key_name = schema['HashKeyElement']['AttributeName']
        columns = 'json TEXT, ' + hash_key_name + ' ' + self.type_to_sqlite[schema['HashKeyElement']['AttributeType']]
        key_columns = hash_key_name
        
        if 'RangeKeyElement' in schema:
            range_key_name = schema['RangeKeyElement']['AttributeName']
            columns += ', ' + range_key_name + ' ' + self.type_to_sqlite[schema['RangeKeyElement']['AttributeType']]
            key_columns += ', ' + range_key_name
        
        self.conn.execute('CREATE TABLE `' + table_name + '` (' + columns + ', PRIMARY KEY (' + key_columns + '))')
        return {'Table': {'TableName': table_name, 'KeySchema': schema}}

    def delete_table(self, table_name):
        self.conn.execute('DROP TABLE ' + table_name)
        return {'Table': {'TableName': table_name}}

    def batch_write_item(self, request_items, object_hook=None):
        data = {'RequestItems': request_items}
        json_input = json.dumps(data)
        return self.make_request('BatchWriteItem', json_input,
                                 object_hook=object_hook)

    def put_item(self, table_name, item):
        values = "'" + json.dumps(item) + "', '" + str(item.hash_key) + "'"
        if item.range_key is not None:
            values += ", '" + str(item.range_key) + "'"
        
        sql = 'REPLACE INTO ' + table_name + ' VALUES (' + values + ')'
        self.conn.execute(sql)
    
    def delete_item(self, table_name, hash_key, range_key):
        pragma = self._pragma(table_name)

        hash_key_name = pragma[1][1]
        where = "`" + hash_key_name + "` = '" + hash_key + "'"  
        if len(pragma) > 2:
            range_key_name = pragma[2][1]
            where += " AND `" + range_key_name + "` = '" + range_key + "'"

        sql = 'DELETE FROM ' + table_name + ' WHERE ' + where
        self.conn.execute(sql)

    def query(self, table_name, hash_key, start=None, end=None,
              scan_index_forward=True):
        pragma = self._pragma(table_name)
        
        sql = 'SELECT * FROM ' + table_name + ' WHERE `' + pragma[1][1] + "` = '" + hash_key + "'"
        if start:
            sql += ' AND `' + pragma[2][1] + "` >= '" + str(start) + "'"
        if end:
            sql += ' AND `' + pragma[2][1] + "` <= '" + str(end) + "'"
        
        if scan_index_forward:
            sql += ' ORDER BY `' + pragma[2][1] + '` ASC'
        else:
            sql += ' ORDER BY `' + pragma[2][1] + '` DESC'
            
        c = self.conn.cursor()
        c.execute(sql)
        rows = []
        for row in c:
            item = {}
            for i in xrange(len(row)):
                item[pragma[i][1]] = row[i]
            rows.append(json.loads(item['json']))
        c.close()
        
        return {'Items': rows}
        
    def scan(self, table_name):
        pragma = self._pragma(table_name)
        c = self.conn.cursor()
        c.execute('SELECT * FROM ' + table_name)
        rows = []
        for row in c:
            item = {}
            for i in xrange(len(row)):
                item[pragma[i][1]] = row[i]
            rows.append(json.loads(item['json']))
        c.close()
        return {'Items': rows}
