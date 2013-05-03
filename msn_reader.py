# msn_reader.py

'''msn_reader - Read ATOC station data and insert it into a database

This module reads data from an MSN file from ATOC containing station details for
UK rail stations and inserts it into a database. The module has to be provided
with a databae cursor initially, and then fed with lines/records one at a time.
'''

import nrcif, msn_records

class MSN(nrcif.CIFReader):
    '''A state machine with side-effects that handles MSN files.'''

    allowedtransitions = dict()
    allowedtransitions["Start_Of_File"] = set(("A",))
    allowedtransitions["A"] = set(("A", "B", "C", "L"))
    allowedtransitions["B"] = set(("A", "B", "C", "L"))
    allowedtransitions["C"] = set(("A", "C", "L"))
    allowedtransitions["L"] = set(("L", "G", "R", "V", "Z", "0", "M", "-", "E"))
    allowedtransitions["G"] = set(("G", "R", "V", "Z", "0", "M", "-", "E"))
    allowedtransitions["R"] = set(("R", "V", "Z", "0", "M", "-", "E"))
    allowedtransitions["V"] = set(("V", "Z", "0", "M", "-", "E"))

    # I'm not going to worry about which order the trailers come in, or if they
    # are repeated...
    allowedtransitions["Z"] = set(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions["0"] = set(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions["M"] = set(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions["-"] = set(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions[" "] = set(("Z", "0", "M", "-", " ", "E"))
    allowedtransitions["E"] = set((None,))

    rslice = slice(0,1)

    rwidth = 82

    layouts = msn_records.layouts

    schema = "msn"

    def __init__(self, cur):
        '''Requires a DB API cursor to the database that will contain the data'''

        super().__init__(cur)

        # Note that the MSN class does not keep any state, because if you only
        # consider the records that are not marked 'historic' then there is no
        # order-dependence!

        # The following ensures all context is valid for insertion into the
        # database, even if it is just a row of NULL/None
        for i in self.layouts.keys():
            self.context[i] = [None] * self.layouts[i].sql_width

        # Prepare SQL insert statements
        for i in ("A", "L", "V"):
            tablename = self.layouts[i].name.lower().replace(" ", "_")
            self.prepare_sql_insert(i, tablename)

    def process_A(self):
        self.cur.execute(self.sql["A"], self.context["A"])

    def process_L(self):
        self.cur.execute(self.sql["L"], self.context["L"])

    def process_V(self):
        self.cur.execute(self.sql["V"], self.context["V"])


def main():
    import sys, contextlib
    if len(sys.argv) != 2:
        print("When called as a script, needs to be provided with an MSN file to process")
        sys.exit(1)
    cur = nrcif.DummyCursor()
    msn = MSN(cur)
    fpp = open(sys.argv[1], 'r')
    fpp.readline() # Discard the header
    with contextlib.closing(fpp) as fp:
        for line in fp:
            msn.process(line)
    print("Processing complete")

if __name__ == "__main__":
    main()
