"""LOOKING AHEAD: This module is an example of what may be the final structure
of the project."""

import rill
import utm

import log


@rill.component
@rill.inport('Latitude')
@rill.inport('Longitude')
@rill.outport('Easting')
@rill.outport('Northing')
@rill.outport('Zone')
@rill.outport('Letter')
@rill.inport('LOGIN')
@rill.outport('LOGOUT')
def latlong_to_utm(Latitude, Longitude, Easting, Northing, Zone, Letter, LOGIN, LOGOUT):
    ename = 'Easting.column'
    nname = 'Northing.column'
    zname = 'Zone.column'
    lname = 'Letter.column'
    LOGFILE = LOGIN.receive_once()
    with open(Latitude.receive_once()) as lat, \
         open(Longitude.receive_once()) as lon, \
         open(ename, 'w') as E, \
         open(nname, 'w') as N, \
         open(zname, 'w') as Z, \
         open(lname, 'w') as L,
         open(LOGFILE, 'w') as LOG:
        #
        formatstr = '{}\n'
        LOG.write("Began conversion from lat/lon to UTM")
        for coord in zip(lat, lon):
            e, n, z, l = utm.from_latlon(*coord)
            E.write(formatstr.format(e))
            N.write(formatstr.format(n))
            Z.write(formatstr.format(z))
            L.write(formatstr.format(l))

        LOG.write("Finished conversion.")

        Easting.send(ename)
        Northing.send(nname)
        Zone.send(zname)
        Letter.send(lname)
        LOGOUT.send(LOGFILE)

if __name__ == '__main__':
    net = rill.engine.network.Network()
    net.add_component('latlong_to_utm', latlong_to_utm,
                      Latitude='')
