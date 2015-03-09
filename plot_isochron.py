# plot_isochron.py

# Copyright 2013, James Humphry

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

''' plot_isochron.py - Plot isochron figures showing the contours of the time
    needed to reach locations from a given station on a given day/time.
    Python 2 only due to basemap limitation.'''

import os, sys, contextlib, argparse, datetime

import psycopg2

import numpy as np
import matplotlib
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt

def read_departure(s):
    return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M')

parser = argparse.ArgumentParser()
parser.add_argument("STATION", help = "The TIPLOC code or station name")
parser.add_argument("DEPARTURE", help = "The departure time and date in the format '2013-01-01 15:45'",
                        type = read_departure)
parser.add_argument("--no-labels", help = "Do not add city labels",
                    action = "store_true", default = False)

parser_db = parser.add_argument_group("database arguments")
parser_db.add_argument("--database", help = "PostgreSQL database to use (default ukraildata)",
                    action = "store", default = "ukraildata")
parser_db.add_argument("--user", help = "PostgreSQL user for upload",
                    action = "store", default = os.getlogin())
parser_db.add_argument("--password", help = "PostgreSQL user password",
                    action = "store", default = "")
parser_db.add_argument("--host", help = "PostgreSQL host (if using TCP/IP)",
                    action = "store", default = None)
parser_db.add_argument("--port", help = "PostgreSQL port (if required)",
                    action = "store", type = int, default = 5432)
args = parser.parse_args()

if args.host:
    connection = psycopg2.connect(  database = args.database,
                                    user = args.user,
                                    password = args.password,
                                    host = args.host,
                                    port = args.post)
else:
    connection = psycopg2.connect(  database = args.database,
                                    user = args.user,
                                    password = args.password)

lat=[]
lon=[]
delay=[]

label_cities = set(('CAMBDGE', 'EDINBUR', 'KNGX   ',
                        'EXETERC', 'CRDFCEN', 'BHAMNWS',
                        'MNCRPIC', 'SOTON  ', 'ABRDEEN',
                        'DRHM   ', 'BANGOR ', 'OBAN   ',
                        'BLFSTCL', 'DUBLINC', 'PENZNCE',
                        'LOWSTFT', 'CATZTUS', 'CARLILE',
                        'SCRBSTR', 'NEWQUAY', 'DOVERP '))
cities = dict()

with contextlib.closing(connection.cursor()) as cur:
    cur.callproc('msn.find_station', (args.STATION,))
    station = cur.fetchone()[0]
    if not station:
        print "Station cannot be identified"
        sys.exit(1)

    label_cities.add(station)

    cur.callproc('util.isochron_latlon', (station,
                                        args.DEPARTURE.time(),
                                        args.DEPARTURE.date()))
    row = cur.fetchone()
    while row:
        locd, delayd, yd, xd = row
        if locd in label_cities:
            cities[locd]=(xd,yd)
        lat.append(yd)
        lon.append(xd)
        delay.append(delayd)
        row = cur.fetchone()

m = Basemap(llcrnrlon=-10.5,llcrnrlat=49.5,urcrnrlon=3.5,urcrnrlat=59.5,
            resolution='h',projection='tmerc',lon_0=-4.36,lat_0=54.7)

x, y = m(np.asarray(lon), np.asarray(lat))
delay = np.asarray(delay)

m.drawmapboundary(fill_color='white')
m.drawcoastlines()
m.drawcountries()

m.drawparallels(np.arange(-40,61.,2.))
m.drawmeridians(np.arange(-20.,21.,2.))

m.contourf(x=x, y=y, data=delay, tri=True)

m.colorbar()
m.drawmapboundary(fill_color=None)

if not args.no_labels:
    for j in cities:
        x, y = m(cities[j][0], cities[j][1])
        m.plot(x, y, 'kx')
        plt.text(x-8000,y+3500,j,size='small', color='k')

plt.title("Isochron map of UK rail journeys from {} {}".format(station,
                            args.DEPARTURE.strftime('%Y-%m-%d %H:%M')))
plt.show()