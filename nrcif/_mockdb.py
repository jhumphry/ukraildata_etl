# mockdb.py

#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#

''' mockdb.py - a mock DB API connection and cursor definition'''

class Cursor(object):

    def __init__(self, fp):
        self.fp = fp

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False  # Don't suppress exceptions

    def execute(self, sql, params = None):
        self.fp.write("Executed SQL: '{}' with params '{}'\n".format(sql, repr(params)))

    def close(self):
        pass

class Connection(object):

    def __init__(self, fp):
        self.fp = fp

    def cursor(self):
        return Cursor(self.fp)

    def commit(self):
        self.fp.write("Committed transaction\n")
        self.fp.flush()

    def close(self):
        self.fp.close()
