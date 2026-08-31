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

Copyright 2014-2026 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
Copyright 2014-2026 Charles University, Faculty of Mathematics and Physics, Prague, Czech Republic
Copyright 2014-2026 Czech Hydrometeorological Institute, Prague, Czech Republic
Copyright 2014-2017 Czech Technical University in Prague, Czech Republic
"""

import numpy as np
from netCDF4 import Dataset, date2num, num2date
import lib.ep_logging
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_rtcfg
log = lib.ep_logging.Logger(__name__)
from met.ep_met_data import ep_met_data

dimnames = ['x', 'y', 'z']
dimlen = {}
times_units = 'hours since 2000-01-01 00:00'
times_calendar = 'gregorian'

def write_netcdf(met_data_all, dts, filename):
    """Function simulating legacy write_netcdf still used in ep_heating"""
    ncf = write_netcdf_header(met_data_all[0], dts, filename)
    try:
        for it, dt in enumerate(dts):
            write_netcdf_timestep(met_data_all[it], it, dt, ncf)
    finally:
        ncf.close()

def write_netcdf_header(met_data, dts, filename):
    """Function to write header of netcdf file from imported FUME met fields"""
    try:
        ncf = Dataset(filename, 'w')
        # find out the dimensions
        maxdim = 1
        for d in met_data:
            if len(d.data.shape) > maxdim:
                dims = d.data.shape
                maxdim = len(dims)
                dimlen['x'], dimlen['y'] = dims[0], dims[1]
                try: # try the x dimension or do nothing if not present
                    dimlen['z'] = dims[2]
                except IndexError:
                    pass

        ncf.createDimension("time", None)

        for i in range(maxdim):
            ncf.createDimension(dimnames[i], dimlen[dimnames[i]])

        times = ncf.createVariable("time","f8",("time",))
        times.units = times_units
        times.calendar = times_calendar
        dts2 = []
        for d in dts:
            dts2.append(d.replace(tzinfo=None))
        times[:] = date2num(dts2,units=times.units,calendar=times.calendar)
        for d in met_data:
            if len(d.data.shape) == 2:
                ncf.createVariable(d.name, 'f4', ("time", dimnames[1], dimnames[0] ) )
            elif len(d.data.shape) == 3:
                ncf.createVariable(d.name, 'f4', ("time", dimnames[2], dimnames[1], dimnames[0] ) )
            else:
                log.fmt_warning("Unexpected data size {}", d.data.shape)
        ep_rtcfg['met_ts_mapping'] = []
        return ncf
    except Exception as e:
        log.fmt_error('Error creating the saved meteo file {}.\n Error: {}', filename, e)
        raise e

def write_netcdf_timestep(met_data, it, dt, ncf):
    try:
        for d in met_data:
            v = d.name
            ncf.variables[v][it, ...] = d.data.transpose()
        ep_rtcfg['met_ts_mapping'].append(it)
    except Exception as e:
        log.fmt_error('Error saving timestep {} into meteo file {}.\n Error: {}', it, ncf.filepath(), e)
        raise e


def check_netcdf_file(dts):
    """Function to check header and structure of the stored met netcdf file into FUME met fields"""
    filename = ep_cfg.input_params.met.met_netcdf_file
    met_vars = list(ep_rtcfg['required_met'])
    if len(met_vars) == 0:
        # no meteorology needed
        log.debug('No meteorology needed to import.')
        return

    try:
        ncf = Dataset(filename, 'r')
    except:
        log.fmt_warning('The saved meteo file {} does not exists.', filename)
        return False
    try:
        # test time dimensions
        try:
            times = ncf.variables["time"]
        except:
            log.fmt_warning('The saved meteo file {} does not contain "time" variable.', filename)
            return False
        if times.units != times_units or times.calendar != times_calendar:
            log.fmt_warning('The "time" variable in the saved meteo file {} has wrong units or calendar.', filename)
            return False
        if len(times) < len(dts):
            log.fmt_warning('Insufficient length of the variable "time" in the saved meteo file {}.', filename)
            return False
        dt_times = num2date(times[:], units=times_units, calendar=times_calendar)
        dtsz = []
        for i in range(len(dts)):
            dtsz.append(dts[i].replace(tzinfo=None))
        dtsi = []
        for i in range(len(dts)):
            try:
                ts = np.where(dt_times == dtsz[i])[0][0]
                dtsi.append(ts)
            except:
                log.fmt_warning('The {}-th timestep {} does not exists in the saved meteo file {}.', \
                                i, dts[i], filename)
                return False
        # check existence and dimensions and read the variables
        if not 'zf' in ncf.variables:
            log.fmt_warning('Required variable "zf" is not in the saved meteo file {}.', filename)
            return False
        zf = ncf.variables['zf'][0,:,0,0].data
        for v in met_vars:
            if not v in ncf.variables:
                log.fmt_warning('Required variable {} is not in the saved meteo file {}.', v, filename)
                return False
            var =  ncf.variables[v]
            if var.shape[3] != ep_cfg.domain.nx or var.shape[2] != ep_cfg.domain.ny:
                log.fmt_warning('Horizontal dimensions of the variable {} does not fit in the saved meteo file {}.', \
                                v, filename)
                return False
            if var.shape[1] < len(ep_rtcfg.model_levels):
                log.fmt_warning('Isufficient vertical dimensions of the variable {} in the saved meteo file {}.', \
                                v, filename)
                return False
            for i in range(len(ep_rtcfg.model_levels)):
                if abs(zf[i]-ep_rtcfg.model_levels[i]) > 1:
                    log.fmt_warning('Vertical level {} does not fit in the saved meteo file {}: {} vs. {}.', \
                                    i, filename, zf[0,i,0,0], ep_rtcfg.model_levels[i])
                    return False
        # save mapping of the case timesteps to netcdf meteo timesteps
        ep_rtcfg['met_ts_mapping'] = dtsi
    except Exception as error:
        log.fmt_warning('Unspecific error during reading header of the saved meteo file {}.', filename)
        log.warning(error)
        return False
    finally:
        ncf.close()
    return True


def read_netcdf_timestep(it):
    """Function to read one timestep from the stored met netcdf file into FUME met fields"""
    filename = ep_cfg.input_params.met.met_netcdf_file
    met_vars = list(ep_rtcfg['required_met'])
    met_data = np.empty((len(met_vars)), dtype=object)
    try:
        ncf = Dataset(filename, 'r')
        # read variables into met_data
        its = ep_rtcfg['met_ts_mapping']
        log.fmt_debug('Reading timestep {} from netcdf ts {}.', it, its)
        for iv,v in enumerate(met_vars):
            log.fmt_debug('Reading variable {}, name {}.', iv, v)
            var = ncf.variables[v]
            vdata = var[its[it],...].transpose().data
            vdo = ep_met_data(v, vdata)
            met_data[iv] = vdo
    except Exception as e:
        log.fmt_error('Error during reading the saved meteo file {}.\n{}', filename, e)
        raise e
    finally:
        ncf.close()
    return met_data
