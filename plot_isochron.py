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

import sys, contextlib

import numpy as np

import matplotlib
#matplotlib.use('GTK3Agg')

from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt

lat=[]
lon=[]
delay=[]

label_cities = frozenset(('CAMBDGE', 'EDINBUR', 'KNGX   ',
                        'EXETERC', 'CRDFCEN', 'BHAMNWS',
                        'MNCRPIC', 'SOTON  ', 'ABRDEEN',
                        'DRHM   ', 'BANGOR ', 'OBAN   ',
                        'BLFSTCL', 'DUBLINC', 'PENZNCE',
                        'LOWSTFT', 'CATZTUS', 'CARLILE',
                        'SCRBSTR', 'NEWQUAY', 'DOVERP '))
cities = dict()

fpp = open(sys.argv[1],'r')
fpp.readline() # skip header
with contextlib.closing(fpp) as fp:
    for line in fp:
        locd, delayd, yd, xd = line.split(',')
        if locd.strip('"') in label_cities:
            cities[locd.strip('"')]=(xd,yd)
        lat.append(float(yd))
        lon.append(float(xd))
        delay.append(float(delayd))

m = Basemap(llcrnrlon=-10.5,llcrnrlat=49.5,urcrnrlon=3.5,urcrnrlat=59.5,
            resolution='h',projection='tmerc',lon_0=-4.36,lat_0=54.7)

x, y = m(np.asarray(lon), np.asarray(lat))
delay = np.asarray(delay)

m.drawmapboundary(fill_color='white')
m.drawcoastlines()
m.drawcountries()
m.fillcontinents(color=(0,0,0,0),lake_color=None)

m.drawparallels(np.arange(-40,61.,2.))
m.drawmeridians(np.arange(-20.,21.,2.))

m.contourf(x=x, y=y, data=delay, tri=True)

m.colorbar()
m.drawmapboundary(fill_color=None)

for j in cities:
    x, y = m(cities[j][0], cities[j][1])
    m.plot(x, y, 'kx')
    plt.text(x-8000,y+3500,j,size='small', color='k')

plt.title("Isochron map of UK rail journeys from CAMBDGE 2013-05-09 08:00")
plt.show()
