# nrcif.py

'''nrcif - A module to read the CIF format for loading into an SQL database

This Python 3 module is intended to provide tools for reading the Network
Rail CIF format used to distribute rail timetables in the UK. Due to the
discrepancies between the available public documentation of the format and
the actual files, this module provide basic CIF tools that will need to be
customised for each source of CIF files.'''

import datetime

from nrcif_fields import *

class CIFRecord(object):
    '''A class representing a record of a fixed-format CIF file'''

    def __init__(self, name, fields):
        self.name = name
        self.fields = fields
        self.width = sum( (x.width for x in self.fields) )

    def read(self, text):
        '''Convert a fixed-format record into a list of Python values'''

        index = 0
        result = []

        for field in self.fields:
            t = field.read(text[index:index+field.width])
            if t != None:
                result.append(t)
            index += field.width

        return result

    def read_dict(self, text):
        '''Convert a fixed-format record into a dict of Python values'''

        index = 0
        result = dict()

        for field in self.fields:
            t = field.read(text[index:index+field.width])
            if t != None:
                result[field.name] = t
            index += field.width

        return result

    def generate_sql_ddl(self):
        '''Generate the description of the record's data fields in SQL format'''

        result = ""
        first_field = True
        for field in self.fields:
            if field.sql_type:
                if first_field:
                    first_field = False
                else:
                    result += ",\n"
                result += "\t{0}\t\t{1}".format(field.name.replace(" ","_").lower(),
                                                field.sql_type)
        result += "\n"
        return result

# The following is a dictionary of standard CIF record types, keyed by the two
# characters that must appear at the start of a CIF record.

Layouts = dict()

Layouts["HD"] = CIFRecord("Header Record", (
                            EnforceField("Record Identity", "HD"),
                            TextField("File Mainframe Identity", 20),
                            DDMMYYDateField("Date of Extract"),
                            TimeField("Time of Extract"),
                            TextField("Current-File-Ref", 7),
                            TextField("Last-File-Ref", 7),
                            FlagField("Bleed-off/Update Ind", "UF"),
                            TextField("Version", 1),
                            DDMMYYDateField("User Extract Start Date"),
                            DDMMYYDateField("User Extract End Date"),
                            SpareField("Spare", 20)
                            ))

Layouts["BS"] = CIFRecord("Basic Schedule", (
                            EnforceField("Record Identity", "BS"),
                            FlagField("Transaction Type", "NDR"),
                            TextField("Train UID", 6),
                            YYMMDDDateField("Date Runs From"),
                            YYMMDDDateField("Date Runs To"),
                            DaysField("Days Run"),
                            FlagField("Bank Holiday Running", " XEG"),
                            FlagField("Train Status", "BFPST12345"),
                            TextField("Train Category", 2),
                            TextField("Train Identity", 4),
                            TextField("Headcode", 4),
                            SpareField("Course Indicator", 1),
                            TextField("Train Service Code", 8),
                            FlagField("Portion ID", " Z01248"),
                            TextField("Power Type", 3),
                            TextField("Timing Load", 4),
                            IntegerField("Speed", 3),
                            TextField("Operating Characteristics", 6),
                            FlagField("Train Class", " BS"),
                            FlagField("Sleepers", " BFS"),
                            FlagField("Reservations", " AERS"),
                            SpareField("Connection Indicator", 1),
                            TextField("Catering Code", 4),
                            TextField("Service Branding", 4),
                            SpareField("Spare", 1),
                            FlagField("STP Indicator", " CNOP")
                            ))

Layouts["BX"] = CIFRecord("Basic Schedule Extra Details", (
                            EnforceField("Record Identity", "BX"),
                            SpareField("Traction Class", 4),
                            TextField("UIC Code", 5),
                            TextField("ATOC Code", 2),
                            FlagField("Applicable Timetable Code", "YN"),
                            TextField("RSID", 8),
                            FlagField("Data Source", " T"),
                            SpareField("Spare", 57)
                            ))

Layouts["LO"] = CIFRecord("Origin Location", (
                            EnforceField("Record Identity", "LO"),
                            TextField("Location", 8),
                            TimeHField("Scheduled Departure"),
                            TimeField("Public Departure"),
                            TextField("Platform", 3),
                            TextField("Line", 3),
                            TextField("Engineering Allowance", 2),
                            TextField("Pathing Allowance", 2),
                            ActivityField("Activity"),
                            TextField("Performance Allowance", 2),
                            SpareField("Spare", 37)
                            ))

Layouts["LI"] = CIFRecord("Intermediate Location", (
                            EnforceField("Record Identity", "LI"),
                            TextField("Location", 8),
                            TimeHField("Scheduled Arrival"),
                            TimeHField("Scheduled Departure"),
                            TimeHField("Scheduled Pass"),
                            TimeField("Public Arrival"),
                            TimeField("Public Departure"),
                            TextField("Platform", 3),
                            TextField("Line", 3),
                            TextField("Path", 3),
                            ActivityField("Activity"),
                            TextField("Engineering Allowance", 2),
                            TextField("Pathing Allowance", 2),
                            TextField("Performance Allowance", 2),
                            SpareField("Spare", 20)
                            ))

Layouts["CR"] = CIFRecord("Changes en Route", (
                            EnforceField("Record Identity", "CR"),
                            TextField("Location", 8),
                            TextField("Train Category", 2),
                            TextField("Train Identity", 4),
                            TextField("Headcode", 4),
                            SpareField("Course Indicator", 1),
                            TextField("Train Service Code", 8),
                            FlagField("Portion ID", " Z01248"),
                            TextField("Power Type", 3),
                            TextField("Timing Load", 4),
                            IntegerField("Speed", 3),
                            TextField("Operating Characteristics", 6),
                            FlagField("Train Class", " BS"),
                            FlagField("Sleepers", " BFS"),
                            FlagField("Reservations", " AERS"),
                            SpareField("Connection Indicator", 1),
                            TextField("Catering Code", 4),
                            TextField("Service Branding", 4),
                            SpareField("Traction Class", 4),
                            TextField("UIC Code", 5),
                            TextField("RSID", 8),
                            SpareField("Spare", 5)
                            ))

Layouts["LT"] = CIFRecord("Terminating Location", (
                            EnforceField("Record Identity", "LT"),
                            TextField("Location", 8),
                            TimeHField("Scheduled Arrival"),
                            TimeField("Public Arrival"),
                            TextField("Platform", 3),
                            TextField("Path", 3),
                            ActivityField("Activity"),
                            SpareField("Spare", 43)
                            ))

Layouts["TN"] = CIFRecord("Train Specific Note", (
                            EnforceField("Record Identity", "TN"),
                            FlagField("Note Type", " GW"),
                            TextField("Note", 77)
                            ))

Layouts["LN"] = CIFRecord("Location Specific Note", (
                            EnforceField("Record Identity", "LN"),
                            FlagField("Note Type", " GW"),
                            TextField("Note", 77)
                            ))

Layouts["AA"] = CIFRecord("Associations", (
                            EnforceField("Record Identity", "AA"),
                            FlagField("Transaction Type", "NDR"),
                            TextField("Main Train-UID", 6),
                            TextField("Associated Train-UID", 6),
                            YYMMDDDateField("Association-start-date"),
                            YYMMDDDateField("Association-end-date"),
                            DaysField("Association-days"),
                            TextField("Association-category", 2),
                            FlagField("Association-date-ind", "SNP"),
                            TextField("Association-location", 7),
                            TextField("Base-location-suffix", 1),
                            TextField("Assoc-location-suffix", 1),
                            SpareField("Diagram Type", 1),
                            FlagField("Association Type", "PO"),
                            SpareField("Spare", 31),
                            FlagField("STP Indicator", " CNOP")
                            ))

Layouts["TI"] = CIFRecord("TIPLOC Insert", (
                            EnforceField("Record Identity", "TI"),
                            TextField("TIPLOC Code", 7),
                            TextField("Capitals Identification", 2),
                            TextField("Nalco", 6),
                            TextField("NLC check character", 1),
                            TextField("TPS Description", 26),
                            TextField("Stanox", 5),
                            SpareField("PO MCP Code", 4),
                            TextField("CRS Code", 3),
                            TextField("16 character description", 16),
                            SpareField("Spare", 8)
                            ))

Layouts["TA"] = CIFRecord("TIPLOC Amend", (
                            EnforceField("Record Identity", "TA"),
                            TextField("TIPLOC Code", 7),
                            TextField("Capitals Identification", 2),
                            TextField("Nalco", 6),
                            TextField("NLC check character", 1),
                            TextField("TPS Description", 26),
                            TextField("Stanox", 5),
                            SpareField("PO MCP Code", 4),
                            TextField("CRS Code", 3),
                            TextField("16 character description", 16),
                            TextField("New TIPLOC", 7),
                            SpareField("Spare", 1)
                            ))

Layouts["TD"] = CIFRecord("TIPLOC Delete", (
                            EnforceField("Record Identity", "TD"),
                            TextField("TIPLOC Code", 7),
                            SpareField("Spare", 71)
                            ))

Layouts["ZZ"] = CIFRecord("Trailer Record", (
                            EnforceField("Record Identity", "ZZ"),
                            SpareField("Spare", 78)
                            ))
