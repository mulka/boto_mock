import sqlite3

class Layer1(object):
    type_to_sqlite = {'N': 'NUMERIC', 'S': 'TEXT'}
    type_from_sqlite = {'NUMERIC': 'N', 'TEXT': 'S'}
    
    def __init__(self):
        self.conn = sqlite3.connect('dynamodb.db', check_same_thread = False)
        
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
        
        key_schema = {
            "HashKeyElement":{"AttributeName": rows[0][1],"AttributeType": self.type_from_sqlite[rows[0][2]]}
        }
        
        if len(rows) > 1:
            key_schema["RangeKeyElement"] = {"AttributeName": rows[1][1],"AttributeType": self.type_from_sqlite[rows[1][2]]}
            
        return {'Table': {'TableName': table_name, 'KeySchema': key_schema}}
        
    def create_table(self, table_name, schema):
        columns = schema['HashKeyElement']['AttributeName'] + ' ' + self.type_to_sqlite[schema['HashKeyElement']['AttributeType']]
        
        if 'RangeKeyElement' in schema:
            columns += ', ' + schema['RangeKeyElement']['AttributeName'] + ' ' + self.type_to_sqlite[schema['RangeKeyElement']['AttributeType']]
        
        self.conn.execute('CREATE TABLE ' + table_name + ' (' + columns + ')')
        return {'Table': {'TableName': table_name}}

    def delete_table(self, table_name):
        self.conn.execute('DROP TABLE ' + table_name)
        return {'Table': {'TableName': table_name}}

    def put_item(self, table_name, item):
        values = "'" + str(item.hash_key) + "'"
        if item.range_key is not None:
            values += ", '" + str(item.range_key) + "'"
        
        sql = 'INSERT INTO ' + table_name + ' VALUES (' + values + ')'
        self.conn.execute(sql)
        
    def scan(self, table_name):
        pragma = self._pragma(table_name)
        c = self.conn.cursor()
        c.execute('SELECT * FROM ' + table_name)
        rows = []
        for row in c:
            item = {}
            for i in xrange(len(row)):
                item[pragma[i][1]] = row[i]
            rows.append(item)
        c.close()
        return {'Items': rows}
