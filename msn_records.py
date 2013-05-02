# msn_records.py

'''msn_records - Definition of MSN records

This module defines the record types found in ATOC Master Station Names files as
provided by data.atoc.org .'''

from nrcif_fields import *
from nrcif import CIFRecord

# The following is a dictionary of standard MSN CIF record types, keyed by the
# characters that must appear at the start of a MSN CIF record.

layouts = dict()

layouts["A"] = CIFRecord("Station Detail", (
                            EnforceField("Record Type", "A"),
                            SpareField("Spaces", 4),
                            TextField("Station Name", 30),
                            IntegerField("CATE Type", 1),
                            TextField("TIPLOC Code", 7),
                            TextField("Subsidiary 3-Alpha Code", 3),
                            SpareField("Spaces 2", 3),
                            TextField("_3-Alpha Code",3), # to avoid conflict
                            IntegerField("Easting", 5),
                            FlagField("Estimated", " E"),
                            IntegerField("Northing", 5),
                            IntegerField("Change Time", 2),
                            SpareField("CATE Footnote", 2),
                            SpareField("Spaces 3", 11),
                            SpareField("Region", 3),
                            SpareField("Spaces 4", 1)
                            ))

layouts["B"] = CIFRecord("Station Table Numbers", (
                            EnforceField("Record Type", "B"),
                            SpareField("Ignored", 81)
                            ))

layouts["C"] = CIFRecord("Station Comments", (
                            EnforceField("Record Type", "C"),
                            SpareField("Ignored", 81)
                            ))

layouts["L"] = CIFRecord("Station Alias", (
                            EnforceField("Record Type", "L"),
                            SpareField("Spaces 1", 4),
                            TextField("Station Name", 30),
                            SpareField("Spaces 2", 1),
                            TextField("Alias Name", 30),
                            SpareField("Spaces 3", 16)
                            ))

layouts["G"] = CIFRecord("Groups", (
                            EnforceField("Record Type", "G"),
                            SpareField("Ignored", 81)
                            ))

layouts["R"] = CIFRecord("Connection Details", (
                            EnforceField("Record Type", "R"),
                            SpareField("Ignored", 81)
                            ))

layouts["V"] = CIFRecord("Routeing Groups", (
                            EnforceField("Record Type", "V"),
                            SpareField("Spaces 1", 4),
                            TextField("Group Name", 30),
                            SpareField("Spaces 2", 1),
                            RouteingGroupField("Station"),
                            SpareField("Spaces 3", 6)
                            ))

layouts["Z"] = CIFRecord("Trailer 1 & 2", (
                            EnforceField("Record Type", "Z"),
                            SpareField("Ignored", 81)
                            ))

layouts["0"] = CIFRecord("Trailer 3", (
                            EnforceField("Record Type", "0"),
                            SpareField("Ignored", 81)
                            ))

layouts["M"] = CIFRecord("Trailer 4", (
                            EnforceField("Record Type", "M"),
                            SpareField("Ignored", 81)
                            ))

layouts["-"] = CIFRecord("3-Alpha Code Usage", (
                            EnforceField("Record Type", "-"),
                            SpareField("Ignored", 81)
                            ))

layouts[" "] = CIFRecord("3-Alpha Code Usage (2)", (
                            EnforceField("Record Type", " "),
                            SpareField("Ignored", 81)
                            ))
layouts["E"] = CIFRecord("Trailer 5", (
                            EnforceField("Record Type", "E"),
                            SpareField("Ignored", 81)
                            ))
