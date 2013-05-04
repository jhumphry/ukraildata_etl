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
    changes (why are all trains listed 45 years in the future?).

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

### mockdb.py

This is a mock database stub that is used instead of Psycopg when the --dry-run
option of `extract_ttis.py` is used. It can safely be ignored.

## Performance

Scanning through the files and generating the SQL necessary to insert the
data generally takes around three minutes on my laptop whereas actually
inserting the data takes at least eleven times as long. Creating indexes
after the data is loaded is only around one minute. The good news is that
once the data is loaded and indexed, subsequent processing should be
substantially faster.

### Restarting Postgresql

In order to get the best performance when inserting data and creating
indexes, it is advisable to restart the PostgreSQL server without fsync.
This allows the possibility of data-loss or corruption if there is a crash,
so after the data is loaded the server should be restarted using the normal
configuration. It is also useful to temporarily increase the amount of
working memory that PostgreSQL is allowed to use, as the default settings
are rather conservative. The following commands work for a standard
installation from Ubuntu, once a symbolic link has been made to
`/etc/postgresql/9.1/main/postgresql.conf` from the data directory
`/var/lib/postgresql/9.1/main/`.

    sudo -u postgres /usr/lib/postgresql/9.1/bin/pg_ctl -D /var/lib/postgresql/9.1/main/ -o "-F -c work_mem=256MB -c maintenance_work_mem=256MB" restart

    sudo -u postgres /usr/lib/postgresql/9.1/bin/pg_ctl -D /var/lib/postgresql/9.1/main/ restart

