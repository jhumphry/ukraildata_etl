# mockdb.py

''' mockdb.py - a mock DB API connection and cursor definition'''

class Cursor(object):

    def __init__(self, fp):
        self.fp = fp

    def execute(self, sql, params = None):
        self.fp.write("Dummy cursor executed SQL: '{}' with params '{}'\n".format(sql, repr(params)))

    def close(self):
        pass

class Connection(object):

    def __init__(self, fp):
        self.fp = fp

    def cursor(self):
        return Cursor(self.fp)

    def commit(self):
        self.fp.flush()

    def close(self):
        self.fp.close()
