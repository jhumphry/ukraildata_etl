# mca_reader.py

'''mca_reader - Read ATOC timetable data and insert it into a database

This module reads data from an MCA file from ATOC containing timetable
information for UK rail journeys and inserts it into a database. The module has
to be provided with a cursor initially, and then fed with lines/records one at
a time.'''

from nrcif_records import layouts

class UnexpectedCIFRecord(Exception):
    pass

class MCA(object):
    '''A state machine with side-effects that handles MCA files.'''

    # the following is a dictionary of tuples of record types allowed after a
    # given record type. Note that there is an issue with Location Note (LN)
    # records in that the allowed record AFTER the LN depends on the type of
    # record BEFORE the LN, which is difficult to validate with this approach.
    # Luckily LN aren't used any more - at least according to the documentation.

    allowedtransitions = dict()
    allowedtransitions["Start_Of_File"] = ("HD",)
    allowedtransitions["HD"] = ("TI", "TA", "TD", "AA", "BS", "ZZ")
    allowedtransitions["TI"] = ("TI", "TA", "TD", "AA", "BS", "ZZ")
    allowedtransitions["TA"] = ("TA", "TD", "AA", "BS", "ZZ")
    allowedtransitions["TD"] = ("TD", "AA", "BS", "ZZ")
    allowedtransitions["AA"] = ("AA", "BS", "ZZ")

    # A BS giving an STP cancellation can be followed directly by another BS
    allowedtransitions["BS"] = ("BS", "BX", "TN", "LO")
    allowedtransitions["BX"] = ("TN", "LO")
    allowedtransitions["TN"] = ("LO",)
    allowedtransitions["LO"] = ("LI", "CR", "LT")
    allowedtransitions["LN"] = ("LI", "CR", "LT", "BS", "ZZ")
    allowedtransitions["LI"] = ("LI", "CR", "LT")
    allowedtransitions["CR"] = ("LI", "LT")
    allowedtransitions["LT"] = ("BS", "ZZ")
    allowedtransitions["ZZ"] = (None,)

    def __init__(self, cur):
        '''Requires a DB API cursor to the database that will contain the data'''

        self.cur = cur

        self.context = dict()
        self.sql = dict()
        self.train_UID = None
        self.loc_order = None
        self.state = "Start_Of_File"

        # The following ensures all context is valid for insertion into the
        # database, even if it is just a row of NULL/None
        for i in layouts.keys():
            self.context[i] = [None] * layouts[i].sql_width

        # Prepare SQL insert statements
        bs_width =  layouts['BS'].sql_width + \
                    layouts['BX'].sql_width + \
                    layouts['TN'].sql_width
        bs_params = ",".join(["%s" for x in range(1,bs_width+1)])
        self.sql['BS'] = "INSERT INTO mca.basic_schedule VALUES({})".format(bs_params)

        for i in ('LO', 'LI', 'CR', 'LT', 'LN'):
            width = 2 + layouts[i].sql_width
            tablename = layouts[i].name.lower().replace(" ", "_")
            params = ",".join(["%s" for x in range(1,width+1)])
            self.sql[i] = "INSERT INTO mca.{} VALUES({})".format(tablename, params)

        for i in ('AA', 'TI', 'TA', 'TD'):
            width = layouts[i].sql_width
            tablename = layouts[i].name.lower().replace(" ", "_")
            params = ",".join(["%s" for x in range(1,width+1)])
            self.sql[i] = "INSERT INTO mca.{} VALUES({})".format(tablename, params)

    def process(self, record):
        '''Process a record'''

        record = record.replace("\n", " ")
        if len(record) < 80:
            record = record + " " * (80-len(record))

        rtype = record[0:2]
        if rtype not in self.allowedtransitions[self.state]:
            raise UnexpectedCIFRecord("Unexpected '{0}' record following '{1}' record".format(rtype, self.state))
        self.state = rtype
        self.context[rtype] = layouts[rtype].read(record)
        try:
            self.__getattribute__("process_"+rtype)()
        except AttributeError:
            pass

    def process_TI(self):
        self.cur.execute(self.sql["TI"], self.context["TI"])

    def process_TA(self):
        self.cur.execute(self.sql["TA"], self.context["TA"])

    def process_TD(self):
        self.cur.execute(self.sql["TD"], self.context["TD"])

    def process_AA(self):
        self.cur.execute(self.sql["AA"], self.context["AA"])

    def process_BS(self):

        self.context["BX"] = [None] * layouts["BX"].sql_width
        self.context["TN"] = [None] * layouts["TN"].sql_width

        # If the BS record is a short-term cancellation of a permanent service
        # there will be no further details or any locations given, so the record
        # may just as well be posted immediately.
        if self.context["BS"][21] == "C":
            self.cur.execute(self.sql["BS"], self.context["BS"] +
                                            self.context["BX"] +
                                            self.context["TN"])

        self.train_UID = self.context["BS"][1]
        self.context["BX"] = [None] * layouts["BX"].sql_width
        self.context["TN"] = [None] * layouts["TN"].sql_width

    def process_LO(self):

        # Note - the LO state can only be reached from BS, so there must have
        # been a valid BS record before this point. However the BX and TN
        # context may be null.
        self.cur.execute(self.sql["BS"], self.context["BS"] +
                                        self.context["BX"] +
                                        self.context["TN"])

        self.LOC_order = 0
        self.cur.execute(self.sql["LO"], [self.train_UID, self.LOC_order] +
                                            self.context["LO"])

    def process_LI(self):
        self.LOC_order += 1
        self.cur.execute(self.sql["LI"], [self.train_UID, self.LOC_order] +
                                            self.context["LI"])
    def process_CR(self):
        self.cur.execute(self.sql["CR"], [self.train_UID, self.LOC_order] +
                                            self.context["CR"])

    def process_LT(self):
        self.LOC_order += 1
        self.cur.execute(self.sql["LT"], [self.train_UID, self.LOC_order] +
                                            self.context["LT"])

    def process_LN(self):
        self.cur.execute(self.sql["LN"], [self.train_UID, self.LOC_order] +
                                            self.context["LN"])

class DummyCursor(object):
    def execute(self, sql, params = None):
        print("Dummy cursor executed SQL: '{}' with params '{}'".format(sql, repr(params)))


def main():
    import sys
    if len(sys.argv) != 2:
        print("When called as a script, needs to be provided with an MCA file to process")
        sys.exit(1)
    cur = DummyCursor()
    mca = MCA(cur)
    with open(sys.argv[1], 'r') as fp:
        for line in fp:
            mca.process(line)
    print("Processing complete")

if __name__ == "__main__":
    main()
