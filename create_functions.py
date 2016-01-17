# create_functions.py

# Copyright 2015 - 2016, James Humphry

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

''' create_functions.py - creates various useful functions in a database
    containing UK rail data.'''

import os
import argparse

import psycopg2

import nrcif.mockdb

parser = argparse.ArgumentParser()
parser.add_argument("source_path",
                    help="The directory containing the SQL sources "
                         "(default 'sql').",
                    nargs="?", metavar="SOURCE PATH", default="sql")

parser_db = parser.add_argument_group("database arguments")
parser_db.add_argument("--dry-run",
                       help="Dump output to a file rather than sending to the "
                            "database",
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

args = parser.parse_args()

if args.dry_run:
    connection = nrcif.mockdb.Connection(args.dry_run)
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

sources = [
    'alf_get_direct_connections.sql',
    'mca_get_full_timetable.sql',
    'mca_get_train_timetable.sql',
    'msn_earliest_departure.sql',
    'msn_find_station.sql',
    'util_get_direct_connections.sql',
    'util_isochron_latlon.sql',
    'util_isochron.sql',
    'util_iterate_reachable.sql',
    'util_natgrid_en_to_latlon.sql',
    'ztr_get_full_timetable.sql'
    ]

connection.autocommit = True

with connection.cursor() as cur:

    cur.execute('DROP SCHEMA IF EXISTS util CASCADE;')
    cur.execute('CREATE SCHEMA util;')

    for s in sources:
        file_path = os.path.join(args.source_path, s)
        # Some tools may create .sql files with a superfluous BOM at the
        # start, even though they are supposed to by UTF-8...
        fp = open(file_path, mode='r', encoding='utf_8_sig')
        cur.execute(fp.read())
        fp.close()

    connection.commit()
    cur.execute("VACUUM ANALYZE;")

connection.close()
