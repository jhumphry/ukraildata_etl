# alf_reader.py

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

'''alf_reader - Read ATOC Additional Links (Fixed) data

This module reads data from an ALF file from ATOC containing data on the links
between train stations other than timetabled rail services - eg stations
accessible by foot or tram.'''

import collections

from nrcif.fields import *

class ALF(object):
    '''A simple hander for ALF files.'''

    layout = collections.OrderedDict()
    layout["M"] = VarTextChoiceField("Mode", 8, (   "BUS", "TUBE", "WALK",
                                                    "FERRY", "METRO", "TRAM",
                                                    "TAXI", "TRANSFER"))
    layout["O"] = TextField("Origin", 3)
    layout["D"] = TextField("Destination", 3)
    layout["T"] = IntegerField("Link Time", 3) # changed to avoid clash with keyword
    layout["S"] = TimeField("Start Time")
    layout["E"] = TimeField("End Time")
    layout["P"] = IntegerField("Priority", 1)
    layout["F"] = DD_MM_YYYYDateField("Start Date")
    layout["U"] = DD_MM_YYYYDateField("End Date")
    layout["R"] = DaysField("Days of Week")

    def __init__(self, cur):

        self.cur = cur
        self.sql_insert = "INSERT INTO alf.alf VALUES({})".format(",".join(["%s"]*10))

    def process(self, record):

        fields = record.rstrip().split(",")

        values = {}
        for i in fields:
            key, ignore, value = i.partition("=")
            values[key] = self.layout[key].read(value)

        result = []
        for i in self.layout:
            if i in values:
                result.append(values[i])
            else:
                result.append(None)

        self.cur.execute(self.sql_insert, result)

def main():
    import sys, nrcif
    if len(sys.argv) != 2:
        print("When called as a script, needs to be provided with an ALF file to process")
        sys.exit(1)
    cur = nrcif.DummyCursor()
    alf = ALF(cur)
    with open(sys.argv[1], 'r') as fp:
        for line in fp:
            alf.process(line)
    print("Processing complete")

if __name__ == "__main__":
    main()