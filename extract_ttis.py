# extract_ttis.py

''' extract_ttis.py - a utility for extracting data from data.atoc.org TTIS
    downloads into a Postgresql database'''

import mca_reader, ztr_reader, msn_reader

import psycopg2

import sys, os, argparse, zipfile, contextlib

parser = argparse.ArgumentParser()
parser.add_argument("TTIS", help = "The TTIS .zip file containing the required data")
parser.add_argument("--no-mca", help = "Don't parse the provided main timetable data",
                    action = "store_true", default = False)
parser.add_argument("--no-ztr", help = "Don't parse the provided Z-Trains (manual additions) timetable data",
                    action = "store_true", default = False)
parser.add_argument("--no-msn", help = "Don't parse the provided main station data",
                    action = "store_true", default = False)

parser_db = parser.add_argument_group("database arguments")
parser_db.add_argument("--database", help = "Postgresql database to use",
                    action = "store", default = "ukraildata")
parser_db.add_argument("--user", help = "Postgresql user for upload",
                    action = "store", default = os.getlogin())
parser_db.add_argument("--password", help = "Postgresql user password",
                    action = "store", default = "")
parser_db.add_argument("--host", help = "Postgresql host (if using TCP/IP)",
                    action = "store", default = None)
parser_db.add_argument("--port", help = "Postgresql port (if required)",
                    action = "store", type = int, default = 5432)

args = parser.parse_args()

if not zipfile.is_zipfile(args.TTIS):
    print("{} is not a valid ZIP file".format(args.TTIS))
    sys.exit(1)

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


with zipfile.ZipFile(args.TTIS,"r") as ttis , \
        contextlib.closing(connection.cursor()) as cur:

    ttis_files = {x[-3:] : x for x in ttis.namelist()}

    if not args.no_mca:
        mca = mca_reader.MCA(cur)
        with contextlib.closing(ttis.open(ttis_files["MCA"], "r")) as mca_file:
            counter = 0
            print("Processing", end = "", flush = True)
            for record in mca_file:
                mca.process(record.decode("ASCII"))
                counter += 1
                if counter == 100000:
                    connection.commit()
                    counter = 0
                    print(".", end = "", flush = True)
        print()
        connection.commit()

    if not args.no_ztr:
        ztr = ztr_reader.ZTR(cur)
        with contextlib.closing(ttis.open(ttis_files["ZTR"], "r")) as ztr_file:
            counter = 0
            print("Processing", end = "", flush = True)
            for record in ztr_file:
                ztr.process(record.decode("ASCII"))
                counter += 1
                if counter == 100000:
                    connection.commit()
                    counter = 0
                    print(".", end = "", flush = True)
        print()
        connection.commit()

    if not args.no_msn:
        msn = msn_reader.MSN(cur)

        fpp = ttis.open(ttis_files["MSN"], "r")
        fpp.readline() # Discard the header

        with contextlib.closing(fpp) as msn_file:
            counter = 0
            print("Processing", end = "", flush = True)
            for record in msn_file:
                msn.process(record.decode("ASCII"))
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

