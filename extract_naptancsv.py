# extract_naptancsv.py

# Copyright 2013, James Humphry

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

''' extract_naptancsv.py - a utility for extracting rail data from data.gov.uk
    NaPTAN (CSV format) downloads into a PostgreSQL database'''

import nrcif._mockdb

import psycopg2

import sys, os, argparse, zipfile, contextlib

parser = argparse.ArgumentParser()
parser.add_argument("NaPTAN", help = "The NaPTAN .zip file containing the required RailReferences.csv data")

parser_no = parser.add_argument_group("processing options")
parser_no.add_argument("--no-index", help = "Don't create indexes in the database",
                    action = "store_true", default = False)

parser_db = parser.add_argument_group("database arguments")
parser_db.add_argument("--dry-run", help = "Dump output to a file rather than sending to the database",
                    nargs = "?", metavar = "LOG FILE", default = None, type=argparse.FileType("x"))
parser_db.add_argument("--database", help = "PostgreSQL database to use (default ukraildata)",
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

if not zipfile.is_zipfile(args.NaPTAN):
    print("{} is not a valid ZIP file".format(args.NaPTAN))
    sys.exit(1)

if args.dry_run:
    connection = nrcif._mockdb.Connection(args.dry_run)
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

with zipfile.ZipFile(args.NaPTAN,"r") as naptan, \
        contextlib.closing(connection.cursor()) as cur:

    if not 'RailReferences.csv' in naptan.namelist():
        print("{} does not contain a RailReferences.csv file".format(args.NaPTAN))
        sys.exit(1)

    connection.autocommit = True
    cur.execute('DROP SCHEMA IF EXISTS naptan CASCADE;')
    cur.execute('CREATE SCHEMA naptan;')
    cur.execute('''
        CREATE TABLE naptan.railreferences (
            atco VARCHAR,
            tiploc CHAR(7),
            crs CHAR(3),
            stationname VARCHAR,
            stationnamelang VARCHAR,
            gridtype CHAR(1),
            easting INTEGER,
            northing INTEGER );
    ''')
    if not args.no_index:
        cur.execute('''
            ALTER TABLE naptan.railreferences ADD PRIMARY KEY (tiploc);
        ''')
    connection.autocommit = False

    fpp = naptan.open('RailReferences.csv', 'r')
    fpp.readline() # Discard the header line

    sql_placeholder = ",".join(["$"+str(x) for x in range(1,9)])
    cur.execute('''PREPARE ins_naptan AS
            INSERT INTO naptan.railreferences VALUES({});'''.format(sql_placeholder))

    py_placeholder = ",".join(["%s"]*8)
    ins_statement = '''EXECUTE ins_naptan ({});'''.format(py_placeholder)

    with contextlib.closing(fpp) as fp:
        for record in fp:
            splitrecord = record.decode("ASCII").split(',')
            for i in range(0,6):
                splitrecord[i]=splitrecord[i].strip('"')
            for i in range(6,8):
                splitrecord[i]=int(splitrecord[i])
            cur.execute(ins_statement, splitrecord[0:8])
    connection.commit()

connection.autocommit = True
with contextlib.closing(connection.cursor()) as cur:
    cur.execute("VACUUM ANALYZE;")
connection.close()

