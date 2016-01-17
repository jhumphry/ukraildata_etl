# extract_ttis.py

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

''' extract_ttis.py - a utility for extracting data from data.atoc.org TTIS
    downloads into a PostgreSQL database'''

import sys
import os
import argparse
import zipfile
import contextlib

import psycopg2

import nrcif.mca_reader
import nrcif.ztr_reader
import nrcif.msn_reader
import nrcif.tsi_reader
import nrcif.alf_reader
import nrcif._mockdb


parser = argparse.ArgumentParser()
parser.add_argument("TTIS",
                    help="The TTIS .zip file containing the required data")

parser_no = parser.add_argument_group("processing options")
parser_no.add_argument("--no-mca",
                       help="Don't parse the provided main timetable data",
                       action="store_true", default=False)
parser_no.add_argument("--no-ztr", help="Don't parse the provided Z-Trains "
                                        "(manual additions) timetable data",
                       action="store_true", default=False)
parser_no.add_argument("--no-msn",
                       help="Don't parse the provided main station data",
                       action="store_true", default=False)
parser_no.add_argument("--no-tsi", help="Don't parse the provided TOC specific"
                                        " interchange data",
                       action="store_true", default=False)
parser_no.add_argument("--no-alf", help="Don't parse the provided Additional "
                                        "Fixed Link data",
                       action="store_true", default=False)
parser_no.add_argument("--old-naming",
                       help="Use old naming convention in TTIF file",
                       action="store_true", default=False)

parser_db = parser.add_argument_group("database arguments")
parser_db.add_argument("--dry-run", help="Dump output to a file rather than "
                                         "sending to the database",
                       nargs="?", metavar="LOG FILE", default=None,
                       type=argparse.FileType("x"))
parser_db.add_argument("--database",
                       help="PostgreSQL database to use (default ukraildata)",
                       action="store", default="ukraildata")
parser_db.add_argument("--user", help="PostgreSQL user for upload",
                       action="store",
                       default=os.environ.get("USER", "postgres"))
parser_db.add_argument("--password", help="PostgreSQL user password",
                       action="store", default="")
parser_db.add_argument("--host", help="PostgreSQL host (if using TCP/IP)",
                       action="store", default=None)
parser_db.add_argument("--port", help="PostgreSQL port (if required)",
                       action="store", type=int, default=5432)
parser_db.add_argument("--no-sync-commit", help="Disable synchronous commits",
                       action="store_true", default=False)
parser_db.add_argument("--work-mem", help="Size of working memory in MB ",
                       action="store", type=int, default=0)
parser_db.add_argument("--maintenance-work-mem",
                       help="Size of maintenance working memory in MB",
                       action="store", type=int, default=0)


args = parser.parse_args()

if not zipfile.is_zipfile(args.TTIS):
    print("{} is not a valid ZIP file".format(args.TTIS))
    sys.exit(1)

if args.dry_run:
    connection = nrcif._mockdb.Connection(args.dry_run)
else:
    if args.host:
        connection = psycopg2.connect(database=args.database,
                                      user=args.user,
                                      password=args.password,
                                      host=args.host,
                                      port=args.post)
    else:
        connection = psycopg2.connect(database=args.database,
                                      user=args.user,
                                      password=args.password)

if args.old_naming:
    # job wanted?, job handling class, file extension, needs MSN header fix?
    jobs = ((args.no_mca, nrcif.mca_reader.MCA, "MCA", False),
            (args.no_ztr, nrcif.ztr_reader.ZTR, "ZTR", False),
            (args.no_msn, nrcif.msn_reader.MSN, "MSN", True),
            (args.no_tsi, nrcif.tsi_reader.TSI, "TSI", False),
            (args.no_alf, nrcif.alf_reader.ALF, "ALF", False))
else:
    # job wanted?, job handling class, file extension, needs MSN header fix?
    jobs = ((args.no_mca, nrcif.mca_reader.MCA, "mca", False),
            (args.no_ztr, nrcif.ztr_reader.ZTR, "ztr", False),
            (args.no_msn, nrcif.msn_reader.MSN, "msn", True),
            (args.no_tsi, nrcif.tsi_reader.TSI, "tsi", False),
            (args.no_alf, nrcif.alf_reader.ALF, "alf", False))

with zipfile.ZipFile(args.TTIS, "r") as ttis, \
        connection.cursor() as cur:

    if args.no_sync_commit:
            cur.execute("SET SESSION synchronous_commit=off;")

    if args.work_mem != 0:
            cur.execute("SET SESSION work_mem=%s;", (args.work_mem*1024,))

    if args.maintenance_work_mem != 0:
            cur.execute("SET SESSION maintenance_work_mem=%s;",
                        (args.maintenance_work_mem*1024,))

    ttis_files = {x[-3:]: x for x in ttis.namelist()}

    for job in jobs:

        if not job[0]:
            handling_obj = job[1](cur)

            fpp = ttis.open(ttis_files[job[2]], "r")

            if job[3]:
                fpp.readline()  # Discard the MSN header line

            with contextlib.closing(fpp) as fp:
                counter = 0
                print("Processing {} file:".format(job[2]), end="", flush=True)
                for record in fp:
                    handling_obj.process(record.decode("ASCII"))
                    counter += 1
                    if counter == 100000:
                        counter = 0
                        print(".", end="", flush=True)
            print()
            connection.commit()

connection.autocommit = True
with connection.cursor() as cur:
    cur.execute("VACUUM ANALYZE;")
connection.close()
