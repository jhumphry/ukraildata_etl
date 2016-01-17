# ztr_reader.py

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

'''ztr_reader - Read ATOC timetable data and insert it into a database

This module reads data from an ZTR file from ATOC containing timetable
information for manual supplements to the rail timetable (ferries etc) and
inserts it into a database. The module has to be provided with a database
cursor initially, and then fed with lines/records one at a time.'''

import copy

import nrcif
import nrcif.records
import nrcif.mca_reader
import nrcif._mockdb

from nrcif.fields import *
from nrcif import CIFRecord

# All other fields seem to be missing
reduced_hd = CIFRecord("Header Record", (
                            EnforceField("Record Identity", "HD"),
                            SpareField("Spare", 79)
                            ))

corrected_bs = copy.deepcopy(nrcif.records.layouts["BS"])
corrected_bs.fields[3] = YYMMDD_1956_DateField("Date Runs From")
corrected_bs.fields[4] = YYMMDD_1956_DateField("Date Runs To")

corrected_bx = copy.deepcopy(nrcif.records.layouts["BX"])
corrected_bx.fields[4] = FlagField("Applicable Timetable Code", " YN")


class ZTR(nrcif.mca_reader.MCA):
    '''An extension of the MCA reader to deal with the slightly cut-back ZTR
    format.'''

    schema = "ztr"

    layouts = nrcif.records.layouts.copy()
    layouts["HD"] = reduced_hd
    layouts["BS"] = corrected_bs
    layouts["BX"] = corrected_bx

    def __init__(self, cur):
        '''Requires a DB API cursor to the database that will contain the
        data'''

        super().__init__(cur)


def main():
    import sys
    if len(sys.argv) != 2:
        print("When called as a script, needs to be provided with a "
              "ZTR file to process")
        sys.exit(1)
    cur = nrcif._mockdb.Cursor()
    ztr = ZTR(cur)
    with open(sys.argv[1], 'r') as input_file:
        for line in input_file:
            ztr.process(line)
    print("Processing complete")

if __name__ == "__main__":
    main()
