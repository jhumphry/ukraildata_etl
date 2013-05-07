# mca_reader.py

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

'''mca_reader - Read ATOC timetable data and insert it into a database

This module reads data from an MCA file from ATOC containing timetable
information for UK rail journeys and inserts it into a database. The module has
to be provided with a databae cursor initially, and then fed with lines/records
one at a time.'''

import nrcif, nrcif.records

class MCA(nrcif.CIFReader):
    '''A state machine with side-effects that handles MCA files.'''

    # Note that there is an issue with Location Note (LN) records in that the
    # allowed record AFTER the LN depends on the type of record BEFORE the LN,
    # which is difficult to validate with this approach. Luckily LN aren't used
    # any more - at least according to the documentation.

    allowedtransitions = dict()
    allowedtransitions["Start_Of_File"] = set(("HD",))
    allowedtransitions["HD"] = set(("TI", "TA", "TD", "AA", "BS", "ZZ"))
    allowedtransitions["TI"] = set(("TI", "TA", "TD", "AA", "BS", "ZZ"))
    allowedtransitions["TA"] = set(("TA", "TD", "AA", "BS", "ZZ"))
    allowedtransitions["TD"] = set(("TD", "AA", "BS", "ZZ"))
    allowedtransitions["AA"] = set(("AA", "BS", "ZZ"))
    # A BS giving an STP cancellation can be followed directly by another BS
    allowedtransitions["BS"] = set(("BS", "BX", "TN", "LO"))
    allowedtransitions["BX"] = set(("TN", "LO"))
    allowedtransitions["TN"] = set(("LO",))
    allowedtransitions["LO"] = set(("LI", "CR", "LT"))
    allowedtransitions["LN"] = set(("LI", "CR", "LT", "BS", "ZZ"))
    allowedtransitions["LI"] = set(("LI", "CR", "LT"))
    allowedtransitions["CR"] = set(("LI", "LT"))
    allowedtransitions["LT"] = set(("BS", "ZZ"))
    allowedtransitions["ZZ"] = set((None,))

    layouts = nrcif.records.layouts

    schema = "mca"

    def __init__(self, cur):
        '''Requires a DB API cursor to the database that will contain the data'''

        super().__init__(cur)
        self.train_UID = None
        self.date_runs_from = None
        self.stp_indicator = None
        self.loc_order = None
        self.xmidnight = None
        self.last_time = None

        # for typing convenience
        layouts = self.layouts

        # The following ensures all context is valid for insertion into the
        # database, even if it is just a row of NULL/None
        for i in layouts.keys():
            self.context[i] = [None] * layouts[i].sql_width

        # Prepare SQL insert statements
        bs_width =  layouts['BS'].sql_width + \
                    layouts['BX'].sql_width + \
                    layouts['TN'].sql_width
        self.prepare_sql_insert("BS", "basic_schedule", bs_width)

        for i in ('LO', 'LI', 'CR', 'LT', 'LN'):
            width = 5 + layouts[i].sql_width
            tablename = layouts[i].name.lower().replace(" ", "_")
            self.prepare_sql_insert(i, tablename, width)

        for i in ('AA', 'TI', 'TA', 'TD'):
            tablename = layouts[i].name.lower().replace(" ", "_")
            self.prepare_sql_insert(i, tablename)

    def process_TI(self):
        self.cur.execute(self.sql["TI"], self.context["TI"])

    def process_TA(self):
        self.cur.execute(self.sql["TA"], self.context["TA"])

    def process_TD(self):
        self.cur.execute(self.sql["TD"], self.context["TD"])

    def process_AA(self):
        self.cur.execute(self.sql["AA"], self.context["AA"])

    def process_BS(self):

        self.context["BX"] = [None] * self.layouts["BX"].sql_width
        self.context["TN"] = [None] * self.layouts["TN"].sql_width

        self.train_UID = self.context["BS"][1]
        self.date_runs_from = self.context["BS"][2]
        self.stp_indicator = self.context["BS"][21]

        # If the BS record is a short-term cancellation of a permanent service
        # there will be no further details or any locations given, so the record
        # may just as well be posted immediately.
        if self.stp_indicator == "C":
            self.cur.execute(self.sql["BS"], self.context["BS"] +
                                            self.context["BX"] +
                                            self.context["TN"])

    def process_LO(self):

        # Note - the LO state can only be reached from BS, so there must have
        # been a valid BS record before this point. However the BX and TN
        # context may be null.
        self.cur.execute(self.sql["BS"], self.context["BS"] +
                                        self.context["BX"] +
                                        self.context["TN"])
        self.LOC_order = 0
        self.xmidnight = False
        self.last_time = self.context["LO"][1]
        self.cur.execute(self.sql["LO"], [self.train_UID, self.date_runs_from,
                                            self.stp_indicator, self.LOC_order,
                                            False] + self.context["LO"])

    def process_LI(self):
        self.LOC_order += 1
        current_time = (self.context["LI"][1] or
                        self.context["LI"][2] or
                        self.context["LI"][3] )
        if not self.xmidnight and current_time < self.last_time:
            self.xmidnight = True
        else:
            self.last_time = current_time

        self.cur.execute(self.sql["LI"], [self.train_UID, self.date_runs_from,
                                            self.stp_indicator, self.LOC_order,
                                            self.xmidnight] + self.context["LI"])
    def process_CR(self):
        self.cur.execute(self.sql["CR"], [self.train_UID, self.date_runs_from,
                                            self.stp_indicator, self.LOC_order,
                                            self.xmidnight] + self.context["CR"])

    def process_LT(self):
        self.LOC_order += 1
        if not self.xmidnight and self.context["LT"][1] < self.last_time:
            self.xmidnight = True
        else:
            self.last_time = self.context["LT"][1]
        self.cur.execute(self.sql["LT"], [self.train_UID, self.date_runs_from,
                                            self.stp_indicator, self.LOC_order,
                                            self.xmidnight] + self.context["LT"])

    def process_LN(self):
        self.cur.execute(self.sql["LN"], [self.train_UID, self.date_runs_from,
                                            self.stp_indicator, self.LOC_order,
                                            self.xmidnight] + self.context["LN"])

def main():
    import sys
    if len(sys.argv) != 2:
        print("When called as a script, needs to be provided with an MCA file to process")
        sys.exit(1)
    cur = nrcif.DummyCursor()
    mca = MCA(cur)
    with open(sys.argv[1], 'r') as fp:
        for line in fp:
            mca.process(line)
    print("Processing complete")

if __name__ == "__main__":
    main()
