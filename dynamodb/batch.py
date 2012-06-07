class BatchWrite(object):
    def __init__(self, table, puts=None):
        self.table = table
        self.puts = puts or []

class BatchWriteList(list):
    def __init__(self, layer2):
        list.__init__(self)
        self.layer2 = layer2

    def add_batch(self, table, puts=None):
        self.append(BatchWrite(table, puts))

    def submit(self):
        return self.layer2.batch_write_item(self)
