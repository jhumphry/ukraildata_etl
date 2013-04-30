# nrcif.py

'''nrcif - A module to read the CIF format for loading into an SQL database

This Python 3 module is intended to provide tools for reading the Network
Rail CIF format used to distribute rail timetables in the UK. Due to the
discrepancies between the available public documentation of the format and
the actual files, this module provide basic CIF tools that will need to be
customised for each source of CIF files.'''

class CIFRecord(object):
    '''A class representing a record of a fixed-format CIF file'''

    def __init__(self, name, fields):
        self.name = name
        self.fields = fields
        self.width = sum( (x.width for x in self.fields) )

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

    def read_dict(self, text):
        '''Convert a fixed-format record into a dict of Python values'''

        index = 0
        result = dict()

        for field in self.fields:
            t = field.read(text[index:index+field.width])
            if t != None:
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

