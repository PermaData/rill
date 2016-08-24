"""Reads a netCDF file defining a grid mapping location to some useful
information."""
import sys

import netCDF4 as nc

from .common import readwrite as io
from .common import definitions as d

__all__ = ['map_read']


def map_read(infile, outfile, lat_i, long_i, grid, grid_config, grid_meaning):
    """
    Inputs:
        infile: The input csv file with the location data
        outfile: The file to print processed data to
        lat_i: column index of latitudes in infile
        long_i: column index of longitudes in infile
        grid: The file mapping grid position -> information
        grid_config: The file mapping grid type -> information about
            reading it
        grid_meaning: The file mapping information -> useful formats.
            If None, just return the information.
    """
    with open(infile, 'rb') as fi:
        with open(outfile, 'wb') as fo:
            locs = csv.reader(fi)
            push = csv.writer(fo)

            data = nc.Dataset(grid)
            array = data.variables['grid']  # varname in master

            (min_lon, min_lat, width_lon, width_lat, offset_lon, offset_lat,
             num_lon, num_lat) = get_config(grid_config, infile)

            out = []
            for line in locs:
                lon = line[long_i]
                lat = line[lat_i]
                ind_lat = (lat - min_lat) // width_lat
                ind_lon = (long - min_lon) // width_lon
                if (ind_lat < 0 or ind_lat >= num_lat or ind_lon < 0 or
                        ind_lon >= num_lon):
                    raise IndexError('Coordinate not in grid')
                identifier = array[ind_lat][ind_lon]
                out.append(get_meaning(grid_meaning, identifier))


def get_config(grid_config, name):
    """Read the grid_config file to get information for reading.
    Inputs:
        grid_config: name of config file.
        name: Name of grid entry to use
    """
    with open(grid_config) as f:
        target = "'" + name.strip("'") + "'"
        for line in f:
            if (line.startswith(target)):
                subs = line.split()
                if (subs[1] != "'lat_lon'"):
                    raise ValueError('Type must be lat_lon')
                return tuple(subs[2:])
    raise RuntimeError('The grid name {0} was not found in file {1}.'.format(
        name, grid_config))


def get_meaning(grid_meaning, number):
    """Read grid_meaning to get useful information out.
    Inputs:
        grid_meaning: Name of mapping file.
        number: Type identifier. Does not actually need to be a number.
    """
    if (grid_meaning is not None):
        with open(grid_meaning) as f:
            for line in f:
                if line.startswith(number):
                    return line.split()[1]
        raise RuntimeError(
            'The type number {0} was not found in file {1}.'.format(
                number, grid_meaning))
    else:
        return number


class ConfigData:
    def __init__(self, config):
        self.min_lon = config[0]
        self.min_lat = config[1]
        self.width_lon = config[2]
        self.width_lat = config[3]
        self.offset_lon = config[4]
        self.offset_lat = config[5]
        self.num_lon = config[6]
        self.num_lat = config[7]

    def locate_grid(self, grid, lat, long):
        ind_lat = (lat - self.min_lat) // self.width_lat
        ind_lon = (long - self.min_lon) // self.width_lon
        if (ind_lat < 0 or ind_lat >= self.num_lat or ind_lon < 0 or
                ind_lon >= self.num_lon):
            raise IndexError('Coordinate not in grid')
        data = nc.Dataset(grid)
        array = data.variables['grid']
        return array[ind_lat][ind_lon]

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    map_read(*args)
