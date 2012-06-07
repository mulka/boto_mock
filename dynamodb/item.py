class Item(dict):
    def __init__(self, table, attrs):
        self.table = table
        self._updates = None
        self._hash_key_name = self.table.schema.hash_key_name
        self._range_key_name = self.table.schema.range_key_name
        self[self._hash_key_name] = attrs[self._hash_key_name]
        if self._range_key_name:
            self[self._range_key_name] = attrs[self._range_key_name]
        for key, value in attrs.items():
            if key != self._hash_key_name and key != self._range_key_name:
                self[key] = value
        self._updates = {}

    @property
    def hash_key(self):
        return self[self._hash_key_name]

    @property
    def range_key(self):
        return self.get(self._range_key_name)

    @property
    def hash_key_name(self):
        return self._hash_key_name

    @property
    def range_key_name(self):
        return self._range_key_name

    def add_attribute(self, attr_name, attr_value):
        self._updates[attr_name] = ("ADD", attr_value)

    def delete_attribute(self, attr_name, attr_value=None):
        self._updates[attr_name] = ("DELETE", attr_value)

    def put_attribute(self, attr_name, attr_value):
        self._updates[attr_name] = ("PUT", attr_value)

    def save(self, expected_value=None, return_values=None):
        return self.table.layer2.update_item(self, expected_value,
                                             return_values)

    def delete(self):
        return self.table.layer2.delete_item(self)

    def put(self):
        return self.table.layer2.put_item(self)

    def __setitem__(self, key, value):
        if self._updates is not None:
            self.put_attribute(key, value)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        if self._updates is not None:
            self.delete_attribute(key)
        dict.__delitem__(self, key)