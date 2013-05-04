# schemagen_ttis.py

'''Generate SQL that will create a suitable schema for storing data from ATOC
TTIS timetable .zip files. This is done dynamically to ensure it keeps in sync
with the definitions in the nrcif package.'''

import argparse, contextlib

import nrcif.schema.schemagen_mca, nrcif.schema.schemagen_ztr
import nrcif.schema.schemagen_msn, nrcif.schema.schemagen_tsi
import nrcif.schema.schemagen_alf

parser = argparse.ArgumentParser()

parser.add_argument("DDL", help = "The destination for the SQL DDL file (default schema_ttis_ddl.gen.sql)",
                            nargs = "?", default = "schema_ttis_ddl.gen.sql", type=argparse.FileType("w"))
parser.add_argument("CONS", help = "The destination for the SQL constraints & indexes file (default schema_ttis_cons.gen.sql)",
                            nargs = "?", default = "schema_ttis_cons.gen.sql", type=argparse.FileType("w"))

parser_no = parser.add_argument_group("processing options")
parser_no.add_argument("--no-mca", help = "Don't generate for the provided main timetable data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-ztr", help = "Don't generate for the provided Z-Trains (manual additions) timetable data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-msn", help = "Don't generate for the provided main station data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-tsi", help = "Don't generate for the provided TOC specific interchange data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-alf", help = "Don't generate for the provided Additional Fixed Link data",
                    action = "store_true", default = False)

args = parser.parse_args()

jobs = ((args.no_mca, nrcif.schema.schemagen_mca),
        (args.no_ztr, nrcif.schema.schemagen_ztr),
        (args.no_msn, nrcif.schema.schemagen_msn),
        (args.no_tsi, nrcif.schema.schemagen_tsi),
        (args.no_alf, nrcif.schema.schemagen_alf))

with contextlib.closing(args.DDL) as DDL, contextlib.closing(args.CONS) as CONS:
    for job in jobs:
        if not job[0]:
            job[1].gen_sql(DDL, CONS)

