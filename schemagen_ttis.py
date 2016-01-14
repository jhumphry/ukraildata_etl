# schemagen_ttis.py

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

'''Generate SQL that will create a suitable schema for storing data from ATOC
TTIS timetable .zip files. This is done dynamically to ensure it keeps in sync
with the definitions in the nrcif package.'''

import argparse

import nrcif.schema.schemagen_mca, nrcif.schema.schemagen_ztr
import nrcif.schema.schemagen_msn, nrcif.schema.schemagen_tsi
import nrcif.schema.schemagen_alf

parser = argparse.ArgumentParser()

parser.add_argument("DDL", help = "The destination for the SQL DDL file (default schema_ttis_ddl.gen.sql)",
                            nargs = "?", default = "schema_ttis_ddl.gen.sql", type=argparse.FileType("w"))
parser.add_argument("CONS", help = "The destination for the SQL constraints & indexes file (default schema_ttis_cons.gen.sql)",
                            nargs = "?", default = "schema_ttis_cons.gen.sql", type=argparse.FileType("w"))

parser_no = parser.add_argument_group("processing options")
parser_no.add_argument("--no-mca", help = "Don't generate for the main timetable data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-ztr", help = "Don't generate for the Z-Trains (manual additions) timetable data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-msn", help = "Don't generate for the main station data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-tsi", help = "Don't generate for the TOC specific interchange data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-alf", help = "Don't generate for the Additional Fixed Link data",
                    action = "store_true", default = False)

args = parser.parse_args()

jobs = ((args.no_mca, nrcif.schema.schemagen_mca),
        (args.no_ztr, nrcif.schema.schemagen_ztr),
        (args.no_msn, nrcif.schema.schemagen_msn),
        (args.no_tsi, nrcif.schema.schemagen_tsi),
        (args.no_alf, nrcif.schema.schemagen_alf))

with args.DDL as DDL, args.CONS as CONS:
    for job in jobs:
        if not job[0]:
            job[1].gen_sql(DDL, CONS)

