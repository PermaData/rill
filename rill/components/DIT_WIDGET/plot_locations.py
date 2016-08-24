from mpl_toolkits.basemap import Basemap
from .matplotlib import pyplot as plt
import numpy as np
import pandas as pd


def plot_coords(lats, lons, lcx, lcy, ucx, ucy, dx, dy):
    map = Basemap(
        projection='merc',
        lat_0=57,
        lon_0=-135,
        resolution='l',
        area_thresh=0.1,
        llcrnrlon=lcx,
        llcrnrlat=lcy,
        urcrnrlon=ucx,
        urcrnrlat=ucy)
    map.drawcoastlines()
    map.drawcountries()
    map.fillcontinents(color='#e6e6fa')
    map.drawmapboundary()
    parallels = np.arange(lcy, ucy, dy)
    map.drawparallels(parallels, labels=[1, 0, 0, 0], fontsize=12)
    meridians = np.arange(lcx, ucx, dx)
    map.drawmeridians(meridians, labels=[0, 0, 0, 1], fontsize=12)
    x, y = map(lons, lats)
    map.plot(x, y, 'ro', markersize=6)  # o-circle, s-square
    return None


def degree2latlon(deg, min, sec, dir):
    direction = {'N': 1, 'S': -1, 'E': 1, 'W': -1}
    new_dir = dir  # new direction
    return (
        int(deg) + int(min) / 60.0 + int(sec) / 3600.0) * direction[new_dir]


filename = 'C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\Russian_soil_temp_original_data\Russia_soil_temp.csv'
df = pd.read_csv(filename)
lons = Series(df.ix[:, 10]).values
lats = Series(df.ix[:, 11]).values
print len(lats)
plot_coords(lats, lons, 30, 40, 180, 75, 20, 10)

filename = 'C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\preprocessed_metadata\ggd605\ggd605_scheff_location_data.csv'
df = pd.read_csv(filename)
# https://pypi.python.org/pypi/utm
utm_E = Series(df.ix[:, 4]).values
utm_N = Series(df.ix[:, 5]).values
n = len(utm_E)
print n
lons = [0 for x in range(n)]
lats = [0 for x in range(n)]
for i in range(n):
    [lats[i], lons[i]] = utm.to_latlon(utm_E[i], utm_N[i], 19, 'U')

plot_coords(lats, lons, -67.5, 54.6, -66.5, 55.2, 0.2, 0.1)
plt.title('Schefferville Permafrost Temperature Database')
# plt.savefig('C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\PD_manuscript\Figures\Schefferville_stations.png', transparent=True, bbox_inches='tight')
plt.show()

plot_coords(lats[1], lons[1], -130, 35, -50, 65, 20, 10)
plt.title('Schefferville Permafrost Temperature Database', fontsize=16)
plt.savefig(
    'C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\PD_manuscript\Figures\Schefferville_loc.png',
    transparent=True,
    bbox_inches='tight')

filename = 'C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\preprocessed_data\ggd402\yabrhl.csv'
df = pd.read_csv(filename)
# https://pypi.python.org/pypi/utm
deg_N = Series(df.ix[:, 3]).values
deg_E = Series(df.ix[:, 2]).values
n = len(deg_N)
print deg_N[1]
lons = [0 for x in range(n)]
lats = [0 for x in range(n)]

for i in range(n):
    str_N = str(deg_N[i])
    lats[i] = degree2latlon(
        int(str_N[0:2]), int(str_N[2:4]), int(str_N[4:6]), 'N')
    str_E = str(deg_E[i])
    lons[i] = degree2latlon(
        int(str_E[0:2]), int(str_E[2:4]), int(str_E[4:6]), 'E')

plot_coords(lats, lons, 67, 67, 73, 70, 1, 0.5)
plt.title('Yamal Peninsula, Russia', fontsize=16)
# plt.savefig('C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\PD_manuscript\Figures\Yamal_stations.png', transparent=True, bbox_inches='tight')
plt.show()

# plot_coords(lats[1], lons[1], 70, 60, 75, 63-130, 35, -50, 65, 20, 10)
plt.clf()
plot_coords(68.5, 70.5, 30, 40, 180, 75, 20, 10)
plt.title('Yamal Peninsula, Russia', fontsize=16)
# plt.savefig('C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\PD_manuscript\Figures\Yamal_loc.png', transparent=True, bbox_inches='tight')
plt.show()

plt.clf()
plot_coords(67, 78, 30, 40, 180, 75, 20, 10)
plt.title('Western Siberia, Russia', fontsize=16)
# plt.savefig('C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\PD_manuscript\Figures\west_siberia.png', transparent=True, bbox_inches='tight')
plt.show()


def string2degree(str_d):
    nn = len(str_d)
    # print str_d
    for j in range(nn):
        if str_d[j] == ' ':
            i1 = j
        if str_d[j] == '.':
            i2 = j
    # print i1,i2
    # print [ int(str_d[0:i1]) ,int(str_d[i1+1:i2]), int(str_d[i2+1:nn])]
    sdd = str_d[0:i1]
    smm = str_d[i1 + 1:i2]
    sss = str_d[i2 + 1:nn]
    # print sdd, smm, sss
    if sdd == '':
        sdd = '0'
    if smm == '':
        smm = '0'
    if sss == '':
        sss = '0'

    # print [ int(sdd) ,int(smm), int(sss)]
    return [int(sdd), int(smm), int(sss)]


filename = 'C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\preprocessed_metadata\ggd503\site_descriptions.csv'
df = pd.read_csv(filename)
# https://pypi.python.org/pypi/utm
deg_N = Series(df.ix[:, 4]).values
deg_E = Series(df.ix[:, 5]).values
n = len(deg_N)
# print n
lons = [0 for x in range(n)]
lats = [0 for x in range(n)]
# format dd mm ss.sss    dd mm ss.sss

# print deg_E[153]
# string2degree(str_d)

for i in range(n):
    str_N = str(deg_N[i])
    str_E = str(deg_E[i])
    if str_N[0] != '0' and str_E[0] != '0':
        deg = string2degree(str_N)
        dd = deg[0]
        mm = deg[1]
        ss = deg[2]
        lats[i] = degree2latlon(dd, mm, ss, 'N')

        deg = string2degree(str_E)
        dd = deg[0]
        mm = deg[1]
        ss = deg[2]
        lons[i] = degree2latlon(dd, mm, ss, 'W')
        # print[deg_N[i], deg_E[i]]
        # print[lats[i],lons[i]]

plot_coords(lats, lons, -145, 45, -50.5, 83, 20, 10)
# plot_coords(lats[1], lons[1], -75, 55, -50, 135, 2, 3 )
plt.title('Canada Geothermal Data Collection', fontsize=16)
# plt.savefig('C:\Users\elchin\Documents\Projects\PermData\FGDC_to_GTNP\PD_manuscript\Figures\CGDC_stations.png', transparent=True, bbox_inches='tight')
plt.show()
