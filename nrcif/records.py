# nrcif.records.py

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

'''nrcif.records - Definition of the CIF format records

This module defines the record types found in most National Rail CIF format
files containing UK rail timetable data.'''

from nrcif.fields import ActivityField, EnforceField, FlagField, SpareField
from nrcif.fields import DDMMYYDateField, YYMMDDDateField, DaysField
from nrcif.fields import TimeField, TimeHField
from nrcif.fields import IntegerField, TextField, VarTextField

from nrcif import CIFRecord

# The following is a dictionary of standard CIF record types, keyed by the two
# characters that must appear at the start of a CIF record.

layouts = dict()

layouts["HD"] = CIFRecord("Header Record", (
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

layouts["BS"] = CIFRecord("Basic Schedule", (
                            EnforceField("Record Identity", "BS"),
                            FlagField("Transaction Type", "NDR"),
                            TextField("Train UID", 6),
                            YYMMDDDateField("Date Runs From"),
                            YYMMDDDateField("Date Runs To"),
                            DaysField("Days Run"),
                            FlagField("Bank Holiday Running", " XEG"),
                            FlagField("Train Status", " BFPST12345"),
                            TextField("Train Category", 2),
                            TextField("Train Identity", 4),
                            TextField("Headcode", 4),
                            SpareField("Course Indicator", 1),
                            TextField("Train Service Code", 8),
                            FlagField("Portion ID", " Z01248"),
                            TextField("Power Type", 3),
                            TextField("Timing Load", 4),
                            IntegerField("Speed", 3, optional=True),
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

layouts["BX"] = CIFRecord("Basic Schedule Extra Details", (
                            EnforceField("Record Identity", "BX"),
                            SpareField("Traction Class", 4),
                            TextField("UIC Code", 5),
                            TextField("ATOC Code", 2),
                            FlagField("Applicable Timetable Code", "YN"),
                            TextField("RSID", 8),
                            FlagField("Data Source", " T"),
                            SpareField("Spare", 57)
                            ))

layouts["LO"] = CIFRecord("Origin Location", (
                            EnforceField("Record Identity", "LO"),
                            TextField("Location", 7),
                            TextField("Location Suffix", 1),
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

layouts["LI"] = CIFRecord("Intermediate Location", (
                            EnforceField("Record Identity", "LI"),
                            TextField("Location", 7),
                            TextField("Location Suffix", 1),
                            TimeHField("Scheduled Arrival", optional=True),
                            TimeHField("Scheduled Departure", optional=True),
                            TimeHField("Scheduled Pass", optional=True),
                            TimeField("Public Arrival"),
                            TimeField("Public Departure"),
                            TextField("Platform", 3),
                            TextField("Line", 3),
                            TextField("_Path", 3),  # to avoid conflict
                            ActivityField("Activity"),
                            TextField("Engineering Allowance", 2),
                            TextField("Pathing Allowance", 2),
                            TextField("Performance Allowance", 2),
                            SpareField("Spare", 20)
                            ))

layouts["CR"] = CIFRecord("Changes en Route", (
                            EnforceField("Record Identity", "CR"),
                            TextField("Location", 7),
                            TextField("Location Suffix", 1),
                            TextField("Train Category", 2),
                            TextField("Train Identity", 4),
                            TextField("Headcode", 4),
                            SpareField("Course Indicator", 1),
                            TextField("Train Service Code", 8),
                            FlagField("Portion ID", " Z01248"),
                            TextField("Power Type", 3),
                            TextField("Timing Load", 4),
                            IntegerField("Speed", 3, optional=True),
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

layouts["LT"] = CIFRecord("Terminating Location", (
                            EnforceField("Record Identity", "LT"),
                            TextField("Location", 7),
                            TextField("Location Suffix", 1),
                            TimeHField("Scheduled Arrival"),
                            TimeField("Public Arrival"),
                            TextField("Platform", 3),
                            TextField("_Path", 3),  # to avoid conflict
                            ActivityField("Activity"),
                            SpareField("Spare", 43)
                            ))

layouts["TN"] = CIFRecord("Train Specific Note", (
                            EnforceField("Record Identity", "TN"),
                            FlagField("Note Type", " GW"),
                            VarTextField("Note", 77)
                            ))

layouts["LN"] = CIFRecord("Location Specific Note", (
                            EnforceField("Record Identity", "LN"),
                            FlagField("Note Type", " GW"),
                            VarTextField("Note", 77)
                            ))

layouts["AA"] = CIFRecord("Associations", (
                            EnforceField("Record Identity", "AA"),
                            FlagField("Transaction Type", "NDR"),
                            TextField("Main Train-UID", 6),
                            TextField("Associated Train-UID", 6),
                            YYMMDDDateField("Association-start-date"),
                            YYMMDDDateField("Association-end-date"),
                            DaysField("Association-days"),
                            TextField("Association-category", 2),
                            FlagField("Association-date-ind", " SNP"),
                            TextField("Association-location", 7),
                            TextField("Base-location-suffix", 1),
                            TextField("Assoc-location-suffix", 1),
                            SpareField("Diagram Type", 1),
                            FlagField("Association Type", " PO"),
                            SpareField("Spare", 31),
                            FlagField("STP Indicator", " CNOP")
                            ))

layouts["TI"] = CIFRecord("TIPLOC Insert", (
                            EnforceField("Record Identity", "TI"),
                            TextField("TIPLOC Code", 7),
                            TextField("Capitals Identification", 2),
                            TextField("Nalco", 6),
                            TextField("NLC check character", 1),
                            TextField("TPS Description", 26),
                            TextField("Stanox", 5),
                            SpareField("PO MCP Code", 4),
                            TextField("CRS Code", 3),
                            # _ used to avoid conflict
                            TextField("_16 character description", 16),
                            SpareField("Spare", 8)
                            ))

layouts["TA"] = CIFRecord("TIPLOC Amend", (
                            EnforceField("Record Identity", "TA"),
                            TextField("TIPLOC Code", 7),
                            TextField("Capitals Identification", 2),
                            TextField("Nalco", 6),
                            TextField("NLC check character", 1),
                            TextField("TPS Description", 26),
                            TextField("Stanox", 5),
                            SpareField("PO MCP Code", 4),
                            TextField("CRS Code", 3),
                            # _ used to avoid conflict
                            TextField("_16 character description", 16),
                            TextField("New TIPLOC", 7),
                            SpareField("Spare", 1)
                            ))

layouts["TD"] = CIFRecord("TIPLOC Delete", (
                            EnforceField("Record Identity", "TD"),
                            TextField("TIPLOC Code", 7),
                            SpareField("Spare", 71)
                            ))

layouts["ZZ"] = CIFRecord("Trailer Record", (
                            EnforceField("Record Identity", "ZZ"),
                            SpareField("Spare", 78)
                            ))
