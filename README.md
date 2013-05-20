# UKRailData_ETL

This project aims to load the UK rail timetable and station data provided by
the Association of Train Operating Companies (at data.atoc.org) into
PostgreSQL so that it can be analysed more easily. The data provided by ATOC
is a .zip file containing a mixture of formats which have a certain degree
of quirkiness. The publicly available documentation for these formats varies
in its existence, thoroughness and accuracy. This project only aims to
extract the basic data from the files into a database while maintaining its
basic structure, and from there the parts of the data that are useful for
your specific purposes can be extracted.

It was developed on Python 3.3, but should also work with Python 3.2. The
Psycopg package is required to upload the results to the PostgreSQL database.

### Note

_This project has not been reviewed or endorsed by ATOC, Network Rail or any
other associated organisation. Some of the file format processing is based
on examining the available files and making judicious guesses, so this
project should not be used where anything significant is at stake._

This project is available under the GPL v2 or later, as described in the
file `COPYING`.

> This program is distributed in the hope that it will be useful, but
> WITHOUT ANY WARRANTY; without even the implied warranty of
> MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
> Public License for more details.

## Provided programs and package

### `nrcif`

This Python package contains modules that define the data types used in the
ATOC downloads, builds them into the record types defined in the various file
formats and then processes the files based on the allowed transitions between
record types.

### `extract_ttis.py`

This script acts as a front-end for the `nrcif` module. It can be run from the
command line as follows:

    $ python3 extract_ttis.py --help
    usage: extract_ttis.py [-h] [--no-mca] [--no-ztr] [--no-msn] [--no-tsi]
                           [--no-alf] [--dry-run [LOG FILE]] [--database DATABASE]
                           [--user USER] [--password PASSWORD] [--host HOST]
                           [--port PORT]
                           TTIS

    positional arguments:
      TTIS                  The TTIS .zip file containing the required data

    optional arguments:
      -h, --help            show this help message and exit

    processing options:
      --no-mca              Don't parse the provided main timetable data
      --no-ztr              Don't parse the provided Z-Trains (manual additions)
                            timetable data
      --no-msn              Don't parse the provided main station data
      --no-tsi              Don't parse the provided TOC specific interchange data
      --no-alf              Don't parse the provided Additional Fixed Link data

    database arguments:
      --dry-run [LOG FILE]  Dump output to a file rather than sending to the
                            database
      --database DATABASE   PostgreSQL database to use (default ukraildata)
      --user USER           PostgreSQL user for upload
      --password PASSWORD   PostgreSQL user password
      --host HOST           PostgreSQL host (if using TCP/IP)
      --port PORT           PostgreSQL port (if required)

Options are available to skip certain types of data contained within the
.zip file. The `MCA` file takes the great majority of the processing time, so is
likely to be worth skipping if some other part of the data is more interesting.

### `schemagen_ttis.py`

This script is used to generate two SQL files, one (DDL) that contains
commands to create the data definitions for the various schema and tables
that wil be used and one (CONS) that contains indexes and constraints that
the data tables should meet. By generating these file automatically, it
should be easier to ensure that they match the definitions in the code.

    $ python3 schemagen_ttis.py --help
    usage: schemagen_ttis.py [-h] [--no-mca] [--no-ztr] [--no-msn] [--no-tsi]
                             [--no-alf]
                             [DDL] [CONS]

    positional arguments:
      DDL         The destination for the SQL DDL file (default
                  schema_ttis_ddl.gen.sql)
      CONS        The destination for the SQL constraints & indexes file (default
                  schema_ttis_cons.gen.sql)

    optional arguments:
      -h, --help  show this help message and exit

    processing options:
      --no-mca    Don't generate for the provided main timetable data
      --no-ztr    Don't generate for the provided Z-Trains (manual additions)
                  timetable data
      --no-msn    Don't generate for the provided main station data
      --no-tsi    Don't generate for the provided TOC specific interchange data
      --no-alf    Don't generate for the provided Additional Fixed Link data

### `extract_naptancsv.py`

This script extracts data on rail stations from NAtional Public Transport
Access Nodes database dumps supplied in `.csv` format inside a `.zip` container.
These are available from data.gov.uk. While most of the data relates to bus
transport, there is one file that gives information on rail stations that may
be of higher quality than the `.MSN` files supplied by ATOC - for example the
station names are nicer and the station locations seem to be specified with
greater than 100m accuracy.

The script will create and populate its own schema so there is no separate
schema generation.

    $ python3 extract_naptancsv.py --help
    usage: extract_naptancsv.py [-h] [--no-index] [--dry-run [LOG FILE]]
                                [--database DATABASE] [--user USER]
                                [--password PASSWORD] [--host HOST] [--port PORT]
                                NaPTAN

    positional arguments:
      NaPTAN                The NaPTAN .zip file containing the required
                            RailReferences.csv data

    optional arguments:
      -h, --help            show this help message and exit

    processing options:
      --no-index            Don't create indexes in the database

    database arguments:
      --dry-run [LOG FILE]  Dump output to a file rather than sending to the
                            database
      --database DATABASE   PostgreSQL database to use (default ukraildata)
      --user USER           PostgreSQL user for upload
      --password PASSWORD   PostgreSQL user password
      --host HOST           PostgreSQL host (if using TCP/IP)
      --port PORT           PostgreSQL port (if required)

### `plot_isochron.py`

This script takes the output from the util.isochron_latlon
function and plots isochron contours on a map of the United Kingdom. It
requires the Basemap extension to Matplotlib and therefore is  _Python 2 only_.

    $ python2 plot_isochron.py --help
    usage: plot_isochron.py [-h] [--no-labels] [--database DATABASE] [--user USER]
                            [--password PASSWORD] [--host HOST] [--port PORT]
                            STATION DEPARTURE

    positional arguments:
      STATION              The TIPLOC code or station name
      DEPARTURE            The departure time and date in the format '2013-01-01
                           15:45'

    optional arguments:
      -h, --help           show this help message and exit
      --no-labels          Do not add city labels

    database arguments:
      --database DATABASE  PostgreSQL database to use (default ukraildata)
      --user USER          PostgreSQL user for upload
      --password PASSWORD  PostgreSQL user password
      --host HOST          PostgreSQL host (if using TCP/IP)
      --port PORT          PostgreSQL port (if required)

It may take between thirty seconds and a few minutes to prepare the
data and calculate the contours. A typical invocation might be:

    python2 plot_isochron.py 'sheffield' '2013-05-09 08:00'

## Supplied SQL and PL/pgSQL helper functions and routines

In the `sql/` directory there are several useful functions and routines to help
process the data.

-   `msn.find_station`

    Given a character string, this function first attempts to see if it is a
    valid TIPLOC code. If it is not it looks for any station that has a name
    or alias similar to it, and returns the TIPLOC of that station. If there
    is more than one match it prioritises by the cate_type field, so large
    interchanges with similar names will be selected over small branch line
    stations.

-   `mca.get_train_timetable`

    Given a Train UID reference and a date, this function will return the key
    information on the train's journey on that date. If the train continues
    after midnight (i.e. into the next day) the relevant entries will have the
    `xmidnight` flag set to mark this.

-   `mca.get_full_timetable` and `ztr.get_full_timetable`

    These functions return the full timetable for a particular date from the MCA
    and ZTR files respectively. This includes the stops on that date from trains
    that started on the previous day but continued after midnight, and excludes
    the stops for trains that started on the the specified date but continued
    after midnight of the next day.

    If you are interested in looking at the properties of the UK rail timetable
    it is probably best to pull a particular day's timetable into a temporary
    table, as you then do not have to worry about the details of Short-Term Plan
    changes, or the other scheduled changes to services.

-   `util.get_direct_connections`

    This function takes the name of a table containing a timetable produced
    by the previous functions, a station TIPLOC and a time and produces a list
    of all the direct connections that can be made from this station (i.e.
    without changes).

-   `alf.get_direct_connections`

    This function is supplied with a station TIPLOC, a time and a date and
    produces a list of the direct connections from that station. It complements
    util.get_direct_connections as it supplies the fixed links between
    stations, such as the tube connections between London terminals.

-   `msn.earliest_departure`

    The inter-change time recommended at a station is usually five minutes but
    is sometimes more. This function gives the earliest departure time from a
    station given an arrival time, but will not wrap around midnight (so an
    arrival time of 23:59 will always produce an earliest departure time of
    23:59).

-   `util.iterate_reachable`

    This function is supplied with a table containing a timetable, a
    location, a time, a date and a limit on iterations. It creates a
    temporary table and inserts the start location and time into it. It then
    iterates repeatedly over this table, finding the direct connections from
    this location and time and for each destination seeing if the arrival
    times found are better than the best arrival times known for that
    destination, and if so replacing the best known route. It takes account
    of fixed links and station-specific inter-change times. It will not wrap
    over midnight. Currently it does not take account of TOC-specific
    interchange times. Another script `util_iterate_reachable_example.sql` shows
    how to use this function.

-   `util.isochron` and `util.isochron_latlon`

    These functions take in a station name, a departure time and date and
    produce a table of stations together with the fastest possible journey to
    that station. The location of the station is supplied either as Eastings and
    Northings or Lattitudes and Longitudes.

-   `util.natgrid_en_to_latlon` and `util.natgrid_en_to_latlon_M`

    These functions are used to convert Eastings and Northings to Lattitude and
    Longitude using the definitions used by the UK Ordinance Survey National
    Grid. These are based on the 1830 Airey ellipsoid so will not match up very
    accurately with lattitudes and longitudes based on the GRS80 ellipsoid that
    is used to define GPS co-ordinates. However as the underlying station data
    is not very accurate this is probably not a problem.

## Data that can be processed by this project

The data provided to the public by ATOC consists of "Full refresh CIF"
packages zipped into a file with a name such as `ttfnnn.zip` where nnn is a
number from 000 to 999. Within this zip file are files containing different
types of data, each of which is extracted into the associated schema (so `MCA`
data ends up in the `mca` schema in PostgreSQL):

-   `TTISFnnn.MCA`

    This is the main timetable file. It contains details on most of the
    permanent scheduled services (including changes), information on STP
    (short-term plan) changes to the permanent scheduled services due to
    things such as engineering works, the associations between trains (when
    one train 'becomes another', for example) and basic TIPLOC information
    (TIPLOC represents TIming Point LOCations - a station can have more than
    one TIPLOC). Some parts of `MCA` files have not been tested as they are
    not present in the available "Full refresh CIF" files.

-   `TTISFnnn.ZTR`

    This is the manual trains file, containing trains that for whatever
    reason are not entered into the main timetable file. The data format
    used is based around the `MCA` format, but with puzzling omissions and
    changes.

-   `TTISFnnn.FLF`

    This is the Fixed Link File, giving details of links between nearby
    stations other than scheduled train services - for example bus links or
    Tube transfers. It is not processed as all the information is present in
    the `ALF` files.

-   `TTISFnnn.ALF`

    This is the Additional Fixed Link file. It contains all the information
    in the `FLF` files, but can additionally contain more than one link
    option between stations.

-   `TTISFnnn.MSN`

    This is the Master Station Names file. It contains useful details for
    each station such as the associated TIPLOC, the geographical location,
    the time to allow for interchanges and the CATE type, which indicates
    how useful the station is as an interconnection.

-   `TTISFnnn.SET`

    This is a fixed string UCFCATE which exists for no apparent reason. It
    is not processed.

-   `TTISFnnn.DAT`

    This lists the files provided. It is not currently processed, as the files
    provided are always the same.

The data provided by data.gov.uk for NaPTAN is much simpler to process, as
the only relevant file ( `RailReferences.csv` ) is simply a standard
comma-separated values file with no particular quirks. The revision
information from this file is not uploaded.

## Performance tips

Scanning through the files and generating the SQL necessary to insert the
data generally takes around three minutes on my laptop whereas actually
inserting the data takes at least eleven times as long. Creating indexes
after the data is loaded is only around one minute. The good news is that
once the data is loaded and indexed, subsequent processing should be
substantially faster.

### Restarting Postgresql

In order to get the best performance when inserting data and creating
indexes, it is advisable to restart the PostgreSQL server without
synchronous commits of the write-ahead log. This allows the possibility of
data-loss (but not corruption) if there is a crash, so after the data is
loaded the server should be restarted using the normal configuration. It is
also useful to temporarily increase the amount of working memory that
PostgreSQL is allowed to use, as the default settings are rather
conservative. The following commands work for a standard installation from
Ubuntu, once a symbolic link has been made to
`/etc/postgresql/9.1/main/postgresql.conf` from the data directory
`/var/lib/postgresql/9.1/main/`.

-   To restart PostgreSQL without synchronous commits and with increased
    buffers to improve loading speed:

    sudo -u postgres /usr/lib/postgresql/9.1/bin/pg_ctl -D /var/lib/postgresql/9.1/main/ -o "-c synchronous_commit=off -c work_mem=256MB -c maintenance_work_mem=256MB" restart

-   To restart PostgreSQL with increased buffers to make large queries run more
    efficiently:

    sudo -u postgres /usr/lib/postgresql/9.1/bin/pg_ctl -D /var/lib/postgresql/9.1/main/ -o "-c work_mem=256MB -c maintenance_work_mem=256MB" restart

-   To restart PostgreSQL with the usual settings:

    sudo -u postgres /usr/lib/postgresql/9.1/bin/pg_ctl -D /var/lib/postgresql/9.1/main/ restart
