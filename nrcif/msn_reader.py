# msn_reader.py

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

'''msn_reader - Read ATOC station data and insert it into a database

This module reads data from an MSN file from ATOC containing station details
for UK rail stations and inserts it into a database. The module has to be
provided with a database cursor initially, and then fed with lines/records one
at a time. '''

import nrcif
import nrcif.msn_records
import nrcif._mockdb


class MSN(nrcif.CIFReader):
    '''A state machine with side-effects that handles MSN files.'''

    allowedtransitions = dict()
    allowedtransitions["Start_Of_File"] = frozenset(("A",))
    allowedtransitions["A"] = frozenset(("A", "B", "C", "L"))
    allowedtransitions["B"] = frozenset(("A", "B", "C", "L"))
    allowedtransitions["C"] = frozenset(("A", "C", "L"))
    allowedtransitions["L"] = frozenset(("L", "G", "R", "V", "Z", "0", "M",
                                         "-", "E"))
    allowedtransitions["G"] = frozenset(("G", "R", "V", "Z", "0", "M", "-",
                                         "E"))
    allowedtransitions["R"] = frozenset(("R", "V", "Z", "0", "M", "-", "E"))
    allowedtransitions["V"] = frozenset(("V", "Z", "0", "M", "-", "E"))

    # I'm not going to worry about which order the trailers come in, or
    # if they are repeated...
    allowedtransitions["Z"] = frozenset(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions["0"] = frozenset(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions["M"] = frozenset(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions["-"] = frozenset(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions[" "] = frozenset(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions["E"] = frozenset((None, ))

    rslice = slice(0, 1)

    rwidth = 82

    layouts = nrcif.msn_records.layouts

    schema = "msn"

    def __init__(self, cur):
        '''Requires a DB API cursor to the database that will contain the
        data'''

        super().__init__(cur)

        # Note that the MSN class does not keep any state, because if
        # you only consider the records that are not marked 'historic'
        # then there is no order-dependence!

        # The following ensures all context is valid for insertion into
        # the database, even if it is just a row of NULL/None
        for i in self.layouts.keys():
            self.context[i] = [None] * self.layouts[i].sql_width

        # Prepare SQL insert statements
        for i in ("A", "L", "V"):
            tablename = self.layouts[i].name.lower().replace(" ", "_")
            self.prepare_sql_insert(i, tablename)

    def process_A(self):
        '''Process station details (A) record'''
        self.cur.execute(self.sql["A"], self.context["A"])

    def process_L(self):
        '''Process station alias (L) record'''
        self.cur.execute(self.sql["L"], self.context["L"])

    def process_V(self):
        '''Process routeing groups (V) record'''
        self.cur.execute(self.sql["V"], self.context["V"])


def main():
    '''When called as a script, the MSN class should read an MSN file and
    output the SQL that would be generated to stdout. Due to the header this
    code is not identical to the other readers.'''

    import sys
    import contextlib
    if len(sys.argv) != 2:
        print("When called as a script, needs to be provided with an "
              "MSN file to process")
        sys.exit(1)
    cur = nrcif._mockdb.Cursor()
    msn = MSN(cur)
    input_file = open(sys.argv[1], 'r')
    input_file.readline()  # Discard the header
    with contextlib.closing(input_file) as remaining_input_file:
        for line in remaining_input_file:
            msn.process(line)
    print("Processing complete")

if __name__ == "__main__":
    main()
