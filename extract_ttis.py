# extract_ttis.py

''' extract_ttis.py - a utility for extracting data from data.atoc.org TTIS
    downloads into a PostgreSQL database'''

import mca_reader, ztr_reader, msn_reader, tsi_reader, alf_reader, mockdb

import psycopg2

import sys, os, argparse, zipfile, contextlib

parser = argparse.ArgumentParser()
parser.add_argument("TTIS", help = "The TTIS .zip file containing the required data")

parser_no = parser.add_argument_group("processing options")
parser_no.add_argument("--no-mca", help = "Don't parse the provided main timetable data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-ztr", help = "Don't parse the provided Z-Trains (manual additions) timetable data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-msn", help = "Don't parse the provided main station data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-tsi", help = "Don't parse the provided TOC specific interchange data",
                    action = "store_true", default = False)
parser_no.add_argument("--no-alf", help = "Don't parse the provided Additional Fixed Link data",
                    action = "store_true", default = False)

parser_db = parser.add_argument_group("database arguments")
parser_db.add_argument("--dry-run", help = "Dump output to a file rather than sending to the database",
                    nargs = "?", metavar = "LOG FILE", default = None, type=argparse.FileType("w"))
parser_db.add_argument("--database", help = "PostgreSQL database to use",
                    action = "store", default = "ukraildata")
parser_db.add_argument("--user", help = "PostgreSQL user for upload",
                    action = "store", default = os.getlogin())
parser_db.add_argument("--password", help = "PostgreSQL user password",
                    action = "store", default = "")
parser_db.add_argument("--host", help = "PostgreSQL host (if using TCP/IP)",
                    action = "store", default = None)
parser_db.add_argument("--port", help = "PostgreSQL port (if required)",
                    action = "store", type = int, default = 5432)

args = parser.parse_args()

if not zipfile.is_zipfile(args.TTIS):
    print("{} is not a valid ZIP file".format(args.TTIS))
    sys.exit(1)

if args.dry_run:
    connection = mockdb.Connection(args.dry_run)
else:
    if args.host:
        connection = psycopg2.connect(  database = args.database,
                                        user = args.user,
                                        password = args.password,
                                        host = args.host,
                                        port = args.post)
    else:
        connection = psycopg2.connect(  database = args.database,
                                        user = args.user,
                                        password = args.password)

# job wanted?, job handling class, file extension, needs MSN header fix?
jobs = ((args.no_mca, mca_reader.MCA, "MCA", False),
        (args.no_ztr, ztr_reader.ZTR, "ZTR", False),
        (args.no_msn, msn_reader.MSN, "MSN", True),
        (args.no_tsi, tsi_reader.TSI, "TSI", False),
        (args.no_alf, alf_reader.ALF, "ALF", False))


with zipfile.ZipFile(args.TTIS,"r") as ttis , \
        contextlib.closing(connection.cursor()) as cur:

    ttis_files = {x[-3:] : x for x in ttis.namelist()}

    for job in jobs:

        if not job[0]:
            handling_obj = job[1](cur)

            fpp = ttis.open(ttis_files[job[2]], "r")

            if job[3]:
                fpp.readline() # Discard the MSN header line

            with contextlib.closing(fpp) as fp:
                counter = 0
                print("Processing {} file:".format(job[2]), end = "", flush = True)
                for record in fp:
                    handling_obj.process(record.decode("ASCII"))
                    counter += 1
                    if counter == 100000:
                        connection.commit()
                        counter = 0
                        print(".", end = "", flush = True)
            print()
            connection.commit()

connection.autocommit = True
with contextlib.closing(connection.cursor()) as cur:
    cur.execute("VACUUM ANALYZE;")
connection.close()

