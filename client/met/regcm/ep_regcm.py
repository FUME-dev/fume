"""
Description:
ep_regcm.py: reads RegCM meteorological data
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

import os

import numpy as np
from datetime import timedelta
from netCDF4 import Dataset
from lib.ep_config import ep_cfg
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
import  met.ep_met_data 

model_names = {
"tas" : "tas",
"ta"  : "ta",
"qas" : "qas",
"qa"  : "qa",
"rsds": "rsds",
"par" : None,
"ps"  : "ps",
"pa"  : None,
"zf"  : None,
"ua"  : "u",
"va"  : "v",
"uas"  : "uas",
"vas"  : "vas",
"windspd"  : None,
"wndspd10m": None,
"pr"  : "pr",
"pr24": None,
"soilm1" : "mrso",
"soit1":"tf"
}

def tolerance(dt1,dt2,delta_t = 60.0):
    if abs((dt1-dt2).seconds) <= delta_t:
        return(True)
    else:
        return(False)


def get_regcm(ncf, t, fields):
    """Get the fields from specific RegCM input file (given by file handle) for a given timestep"""
    numfields = len(fields)
    data_list = np.empty((numfields),dtype=object) # this numpy will contains the met_data class objects for the timestep t

    for i,f in enumerate(fields):
        if RegCM_names[f]:
            data = (ncf.variables[model_names[f][0]][t, ...]).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(f, data)
        elif f  == 'wndspd10m':
            datau = ncf.variables['ua'][t, ...]
            datav = ncf.variables['va'][t, ...]
            data  = np.transpose(np.sqrt(np.square(datau) + np.square(datav)))
            data_list[i] = met.ep_met_data.ep_met_data(f, data)
        elif f == 'pr24':
            datac  = (ncf.variables['RAINC'][t, ...] ).filled(0.0)
            datanc = (ncf.variables['RAINNC'][t, ...] ).filled(0.0)
            data   = (datac + datanc).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(f, data)
        elif f == 'par':
            data = (ncf.variables['rsds'][t, ...]).transpose()/2.0
            data_list[i] = met.ep_met_data.ep_met_data(f, data)

        return(data_list)


def ep_regcm_met(fields,datestimes):
    """ Main import module, returns list of numpy arrays corresponding to the needed met fields for the list of datestimes"""
    # first, find out the model dates and save along the information of in which file it is
    ncf = []
    modeldates = []
    delta_t = ep_cfg.input_params.met.met_tolerance # in seconds
    delta_t_obj = timedelta(seconds = delta_t)

    for f in ep_cfg.input_params.met.met_files:
        fn = os.path.join(ep_cfg.input_params.met.met_path, f)
        try:
            ncf.append(Dataset(fn,'r'))
        except (RuntimeError, OSError):
            log.fmt_debug('EE: Fatal error. Unable to open file {}. \nExit!', fn)
            raise

        times_tmp = ncf[0].variables['time']
        modeldates_tmp = num2date(times_tmp[:],units=times_tmp.units,calendar=times_tmp.calendar)
        for t, dt in enumerate(modeldates_tmp):
            modeldates.append((ncf[-1], dt, t)) # save the file pointer, the model time and the index within the file

    if not isinstance(datestimes,list):
        datestimes = [datestimes]
    numtimes = len(datestimes)

    f = []
    mdt = 0

    if datestimes[0] < (modeldates[0][1]-delta_t_obj) or datestimes[-1] > (modeldates[-1][1]+delta_t_obj):
        ra
        raise ValueError('EE: Fatal error. The provided files does not cover the requested time period: from {} to {}'.format(datestimes[0], datestimes[-1]))

    # loop over the the requested timesteps

    for dt in datestimes:
        while  dt > modeldates[mdt][1]:
            mdt += 1
        
        if tolerance(dt,modeldates[mdt][1], delta_t = delta_t ): # tolerance in seconds
            log.fmt_debug('II: reading {} from {} ...', dt, modeldates[mdt][0].filepath())

            ncdata = get_regcm(modeldates[mdt][0], modeldates[mdt][2], fields)
            f.append(ncdata)
        else:
            log.fmt_debug('II: reading {} from between {} and {} ...', dt, modeldates[mdt-1][0].filepath(), modeldates[mdt][0].filepath())
            ncdata1 = get_regcm(modeldates[mdt-1][0], modeldates[mdt-1][2], fields)
            ncdata2 = get_regcm(modeldates[mdt][0], modeldates[mdt][2], fields)
            td_1 = (dt - modeldates[mdt-1][1]).seconds
            td_2 = (modeldates[mdt][1] - dt).seconds
            f.append([(d1 * td_2 + d2 * td_1)/float(td_1 + td_2) for d1,d2 in zip(ncdata1, ncdata2)] )
        
    return(f)
