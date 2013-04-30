# nrcif.py

'''nrcif - A module to read the CIF format for loading into an SQL database

This Python 3 module is intended to provide tools for reading the Network
Rail CIF format used to distribute rail timetables in the UK. Due to the
discrepancies between the available public documentation of the format and
the actual files, this module provide basic CIF tools that will need to be
customised for each source of CIF files.'''

import datetime

from nrcif_fields import *

class CIFRecord(object):
    '''A class representing a record of a fixed-format CIF file'''

    def __init__(self, name, fields):
        self.name = name
        self.fields = fields

    def width(self):
        return sum( (x.width for x in self.fields) )

    def read(self, text):
        '''Convert a fixed-format record into a list of Python values'''

        index = 0
        result = []

        for field in self.fields:
            t = field.read(text[index:index+field.width])
            if t != None:
                result.append(t)
            index += field.width

        return result

    def generate_sql_ddl(self, tablename = None):
        if not tablename:
            tablename = self.name.replace(" ","_")

        result = "CREATE TABLE {} (\n".format(tablename)

        first_field = True
        for field in self.fields:
            if field.sql_type:
                if first_field:
                    first_field = False
                else:
                    result += ",\n"
                result += "\t{0} {1}".format(field.name.replace(" ","_"), field.sql_type)

        result += "\n);\n\n"
        return result


