# nrcif/__init__.py

# Copyright 2013, James Humphry

#  This package is free software; you can redistribute it and/or modify
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

'''nrcif - A module to read the CIF format for loading into an SQL database

This Python 3 module is intended to provide tools for reading the Network
Rail CIF format used to distribute rail timetables in the UK. Due to the
discrepancies between the available public documentation of the format and
the actual files, this module provide basic CIF tools that will need to be
customised for each source of CIF files.'''


class UnexpectedCIFRecord(Exception):
    pass

class CIFReader(object):
    '''A state machine with side-effects that forms a base for handling CIF files.'''

    # The following is a dictionary of sets of record types allowed after a
    # given record type.

    allowedtransitions = dict()
    allowedtransitions["Start_Of_File"] = (None,)

    process_methods = dict()

    # Which part of a record gives the type of the record?
    rslice = slice(0,2)

    # How wide is a standard record (yes, it varies...)
    rwidth = 80

    # Layouts gives a dict of CIFRecords keyed on the first two letters of the
    # record that specify the record type.
    layouts = dict()

    schema = "public"

    def __init__(self, cur):
        '''Requires a DB API cursor to the database contains the data'''

        self.cur = cur
        self.context = dict()
        self.sql = dict()
        self.state = "Start_Of_File"

        # This code pre-builds a dict of any process_ZZ methods that have been
        # added in sub-classes, to avoid having a try..except block in a
        # critical path.
        for i in self.allowedtransitions:
            try:
                self.process_methods[i] = self.__getattribute__("process_"+i)
            except AttributeError:
                pass

    def prepare_sql_insert(self, rtype, tablename, number_params = None):
        '''Prepare a suitable SQL insert statement for record type rtype in
        the schema self.schema, the table named tablename and with the specified
        number of parameters (inferred from the record type if not specified.'''

        if not number_params:
            number_params = self.layouts[rtype].sql_width

        sql_params = ",".join(["$"+str(x) for x in range(1,number_params+1)])
        params = ",".join(["%s" for x in range(1,number_params+1)])

        self.cur.execute('''PREPARE ins_{0}_{1} AS
            INSERT INTO {0}.{1} VALUES({2});'''.format(self.schema,
                                                        tablename,
                                                        sql_params))
        self.sql[rtype] = "EXECUTE ins_{0}_{1} ({2});".format(self.schema,
                                                                tablename,
                                                                params)

    def process(self, record):
        '''Process a record and call any specialist handlers that may have been
        defined in subclasses.'''

        record = record.replace("\n", " ")
        if len(record) < self.rwidth:
            record = record + " " * (self.rwidth-len(record))

        rtype = record[self.rslice]
        if rtype not in self.allowedtransitions[self.state]:
            raise UnexpectedCIFRecord("Unexpected '{0}' record following '{1}' record".format(rtype, self.state))

        self.state = rtype
        self.context[rtype] = self.layouts[rtype].read(record)
        if rtype in self.process_methods:
            self.process_methods[rtype]()

class CIFRecord(object):
    '''A class representing a record of a fixed-format CIF file'''

    def __init__(self, name, fields):
        self.name = name
        self.fields = list(fields)
        self.width = sum( (x.width for x in self.fields) )
        self.sql_width = sum ( (1 for x in self.fields if x.sql_type) )

    def read(self, text):
        '''Convert a fixed-format record into a list of Python values'''

        index = 0
        result = []

        for field in self.fields:
            t = field.read(text[index:index+field.width])
            if field.sql_type != None:
                result.append(t)
            index += field.width

        return result

    def read_dict(self, text):
        '''Convert a fixed-format record into a dict of Python values'''

        index = 0
        result = dict()

        for field in self.fields:
            t = field.read(text[index:index+field.width])
            if field.sql_type != None:
                result[field.name] = t
            index += field.width

        return result

    def generate_sql_ddl(self):
        '''Generate the description of the record's data fields in SQL format'''

        result = ""
        first_field = True
        for field in self.fields:
            if field.sql_type:
                if first_field:
                    first_field = False
                else:
                    result += ",\n"
                clean_name = field.name.replace(" ","_").replace("-","_").lower()
                result += "\t{0}\t\t{1}".format(clean_name,
                                                field.sql_type)
        return result


class DummyCursor(object):
    '''A dummy cursor object for use as a mock when testing CIF readers'''

    def execute(self, sql, params = None):
        print("Dummy cursor executed SQL: '{}' with params '{}'".format(sql, repr(params)))
