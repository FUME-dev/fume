"""
Description:

"""

"""
This file is part of the FUME emission model.

FUME is free software: you can redistribute it and/or modify it under the terms of the GNU General
Public License as published by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

FUME is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

Information and source code can be obtained at www.fume-ep.org

Copyright 2014-2023 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
Copyright 2014-2023 Charles University, Faculty of Mathematics and Physics, Prague, Czech Republic
Copyright 2014-2023 Czech Hydrometeorological Institute, Prague, Czech Republic
Copyright 2014-2017 Czech Technical University in Prague, Czech Republic
"""

import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
from netCDF4 import Dataset, date2num

dimnames = ['x', 'y', 'z']
dimlen = {}

def write_netcdf(met_data, dts, filename, exclude = []):
    """Auxiliary function to write netcdf file from imported FUME met fields for quick check"""
    ncf = Dataset(filename, 'w')    
    numtimes = len(dts)
    numvar = len(met_data[0])
    # find out the dimensions
    maxdim = 1
    for d in met_data[0]:
        if len(d.data.shape) > maxdim:
            dims = d.data.shape
            maxdim = len(dims)
            dimlen['x'], dimlen['y'] = dims[0], dims[1]
            try: # try the x dimension or do nothing if not present
                dimlen['z'] = dims[2]
            except IndexError: 
                pass

    time = ncf.createDimension("time", None)

    for i in range(maxdim):
        ncf.createDimension(dimnames[i], dimlen[dimnames[i]])
    
    times = ncf.createVariable("time","f8",("time",))
    times.units = 'hours since 2000-01-01 00:00'
    times.calendar = 'gregorian'
    times[:] = date2num(dts,units=times.units,calendar=times.calendar)
    ncdata = []
    for d in met_data[0]:
        if len(d.data.shape) == 2:
            ncdata.append(ncf.createVariable(d.name, 'f4', ("time", dimnames[1], dimnames[0] ) ))
        elif len(d.data.shape) == 3:
            ncdata.append(ncf.createVariable(d.name, 'f4', ("time", dimnames[2], dimnames[1], dimnames[0] ) ))
        else:
            log.fmt_warning("Unexpected data size {}", d.data.shape)

    for t in range(numtimes):
        for v in range(numvar):
            if met_data[0][v].name not in exclude:
                ncdata[v][t, ...] = met_data[t][v].data.transpose()
