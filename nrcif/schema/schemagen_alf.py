# schemagen_alf.py

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

'''Generate SQL that will create a suitable schema for storing data
from ATOC .ALF additional fixed link files. This is done dynamically to
ensure it keeps in sync with the definitions in nrcif.py, alf_reader.py
and nrcif_fields.py'''

from ..alf_reader import ALF


def gen_sql(DDL, CONS):

    SCHEMA = "alf"

    DDL.write('-- SQL DDL for data extracted from ATOC .ALF fixed link files\n'
              '-- in CSV format. Auto-generated by schemagen_alf.py\n\n')

    CONS.write('-- SQL constraints & indexes definitions for data extracted \n'
               '-- from ATOC .ALF timetable files in CSV format. \n'
               '-- Auto-generated by schemagen_alf.py\n\n')

    DDL.write("CREATE SCHEMA {0};\nSET search_path TO {0},public;\n\n"
              .format(SCHEMA))
    CONS.write("SET search_path TO {0},public;\n\n".format(SCHEMA))

    DDL.write('''-- Only one table\n''')
    DDL.write("CREATE TABLE alf (\n")

    first_field = False
    for i in ALF.layout:
        if not first_field:
            first_field = True
        else:
            DDL.write(",\n")
        fieldname = ALF.layout[i].name.lower().replace(" ", "_")
        DDL.write("\t{0}\t\t{1}".format(fieldname, ALF.layout[i].sql_type))

    DDL.write("\n\t);\n\n")

    CONS.write('''CREATE INDEX idx_alf_origin ON alf (origin);\n''')
    CONS.write('''CREATE INDEX idx_alf_destination ON alf (destination);\n''')

    DDL.write('''SET search_path TO "$user",public;\n''')
    CONS.write('''SET search_path TO "$user",public;\n\n''')
