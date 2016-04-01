# mockdb.py

# Copyright 2013 - 2016, James Humphry

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

import sys


class Cursor(object):
    '''A dummy database cursor object that implements a subset of DB-API
    methods and outputs the requests to a file or stdout.'''

    def __init__(self, log_file=sys.stdout):
        self.log_file = log_file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False  # Don't suppress exceptions

    def execute(self, sql, params=None):
        '''Log a request to execute some SQL with given parameters'''

        self.log_file.write("Executed SQL: '{}' with params '{}'\n"
                            .format(sql, repr(params)))

    def copy_from(self, file, table, sep='\t',
                  null='\\N', size=8192, columns=None):
        '''Log a request to execute a COPY command to upload bulk data. This
        is a Postgresql-specific command'''

        self.log_file.write("Executed a COPY from file '{}' to table: '{}'"
                            " with params {}\n"
                            .format(file.name,
                                    table,
                                    repr((sep, null, size, columns)))
                            )

    def copy_expert(self, sql, file, size=8192):
        '''Log a request to execute a COPY command to upload bulk data. This
        is a Postgresql-specific command'''

        self.log_file.write("Executed a COPY from file '{}' using SQL: '{}'"
                            " with size '{}'\n"
                            .format(file.name,
                                    sql,
                                    repr(size))
                            )

    def close(self):
        '''Close the dummy database cursor object. Does not close the
        associated output file.'''

        pass


class Connection(object):
    '''A dummy database object that implements a subset of DB-API methods and
    outputs the requests to a file or stdout.'''

    def __init__(self, log_file=sys.stdout):
        self.log_file = log_file
        self._autocommit = False

    def set_autocommit(self, value):
        '''Log an attempt to change the autocommit mode of the mock database
        connection object'''

        self._autocommit = value
        self.log_file.write("Set autocommit status to: {}\n".format(value))
        self.log_file.flush()

    autocommit = property(fset=set_autocommit)

    def cursor(self):
        '''Create a dummy cursor which uses the same output file.'''

        return Cursor(self.log_file)

    def commit(self):
        '''Log a request to commit a transaction.'''

        self.log_file.write("Committed transaction\n")
        self.log_file.flush()

    def close(self):
        '''Close the dummy database object. Closes the file associated with
        the object unless that is sys.stdout.'''

        self.log_file.write("Closing connection\n")
        self.log_file.flush()
        if self.log_file != sys.stdout:
            self.log_file.close()


def demonstrate_reader(reader_class):
    '''When called as a script, the CIF_Reader classes should read a suitable
    file and output the SQL that would be generated to stdout.'''

    if len(sys.argv) != 2:
        print("When called as a script, needs to be provided with a suitable"
              "file to process.")
        sys.exit(1)

    reader = reader_class(Cursor())
    with open(sys.argv[1], 'r') as input_file:
        for line in input_file:
            reader.process(line)
    print("Processing complete")
