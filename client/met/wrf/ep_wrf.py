"""ep_wrf.py: reads WRF meteorological data"""


import os
import sys

import numpy as np
from datetime import datetime, timedelta, timezone
from glob import glob
from netCDF4 import Dataset
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times,ep_rtcfg
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
import  met.ep_met_data

model_names = {
"tas" : ( "T2", 2 ),
"ta"  : None, #( "T", 3 ), # perturbation potential temperature: Total pot. temp. in K = T + 300 (T is the perturbation pot. temp.); "normal" temp. = pot. temp *  (p/p_0)^kappa (p_0 = 100000 Pa)
"qas" : ( "Q2", 2 ),
"qa"  : ( "QA", 3 ),
"rsds": ( "RGRND", 2),
"par" : ( "SWDOWN", 2),
"ps"  : ( "PSFC", 2),
"pa"  : None, # !!! the perturbation pressure; real pressure = P + PB
"zf"  : None, # (PH+PHB)/g
"ua"  : None, # due to staggering ("U", 3 ),
"va"  : None, # due to staggering ( "V", 3),
"uas"  :( "U10", 2 ),
"vas"  : ( "V10", 2 ),
"cldfra" : ("CLDFRA", 3),
"ust" : ("UST", 2),
"pblz" : ("PBLH", 2),
"wndspd"  : None,
"wndspd10m": None,
"pr" : None, # RAINC+RAINNC
"pr24": None,
'soim1': ("SMOIS", 3),  # m3 m-3
'soit1': ("TSLB", 3),
'soim' : None, # the upper layer soil moisture (2D)
'soit' : None, # the upper layer soil temperature (2D)
'sltyp' : None,
'snowem' : None #("SNOW", 2) # kg m-2
}

def tolerance(dt1,dt2,delta_t = 60.0):
    if abs((dt1-dt2).seconds) <= delta_t:
        return(True)
    else:
        return(False)


def wrfdate2dt(wrfdatestr):
    byyyy = int(wrfdatestr.split('_')[0].split('-')[0]) #2000-01-24_12:00:00
    bmm   = int(wrfdatestr.split('_')[0].split('-')[1]) #2000-01-24_12:00:00
    bdd   = int(wrfdatestr.split('_')[0].split('-')[2]) #2000-01-24_12:00:00
    bhh   = int(wrfdatestr.split('_')[1].split(':')[0]) #2000-01-24_12:00:00
    bmin  = int(wrfdatestr.split('_')[1].split(':')[1])
    bss   = int(wrfdatestr.split('_')[1].split(':')[2])
    return(datetime(byyyy, bmm,  bdd, bhh, bmin, bss, tzinfo=timezone.utc))


def get_wrf(ncf, t, met_vars):
    """Get the fields from specific WRF input file (given by file handle) for a given timestep"""
    numfields = len(met_vars)
    data_list = np.empty((numfields),dtype=object) # this numpy will contains the met_data class objects for the timestep t

    for i,v in enumerate(met_vars):
        if model_names[v]:
            data = (ncf.variables[model_names[v][0]][t, ...]).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v == 'soim':
            data = ncf.variables['SMOIS'][t, 0, ...].transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v == 'soit':
            data = ncf.variables['TSLB'][t, 0, ...].transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v == 'zf':
            g = 9.8
            dataph = ncf.variables['PH'][t, ...]/g
            dataphb = ncf.variables['PHB'][t, ...]/g
            datahgt = ncf.variables['HGT'][t, ...]
            datah = (dataph+dataphb-datahgt)
            data = (0.5*(datah[1:,:,:]+datah[:-1,:,:])).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v  == 'ta':
            datapt = ncf.variables['T'][t, ...] + 300.0
            kappa = 0.2854
            p0 = 100000.0
            datap = ncf.variables['P'][t, ...]
            datapb= ncf.variables['PB'][t, ...]
            datapress = datap+datapb
            data = (datapt*(datapress/p0)**kappa).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v  == 'pa':
            # need to the perturbation pressure to the background hydrostatic pressure P+PB
            datap = ncf.variables['P'][t, ...]
            datapb= ncf.variables['PB'][t, ...]
            data = (datap + datapb).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v == 'ua': # unstagger the wind into dot-points
            # south_north, west_east_stag -> south_north, west_east
            dataus = ncf.variables['U'][t, ...]
            data = (0.5*(dataus[:,:,1:] + dataus[:,:,:-1])).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v == 'va': # unstagger the wind into dot-points
            # south_north_stag, west_east -> south_north, west_east
            datavs = ncf.variables['V'][t, ...]
            data = (0.5*(datavs[:,1:,:] + datavs[:,:-1,:])).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v == 'wndspd': # unstagger the wind speed into dot-points
            # south_north_stag, west_east -> south_north, west_east
            dataus = ncf.variables['U'][t, ...]
            datau = (0.5*(dataus[:,:,1:] + dataus[:,:,:-1])).transpose()
            datavs = ncf.variables['V'][t, ...]
            datav = (0.5*(datavs[:,1:,:] + datavs[:,:-1,:])).transpose()
            data = np.sqrt(datau**2 + datav**2)
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v  == 'wndspd10m':
            datau = ncf.variables['U10'][t, ...]
            datav = ncf.variables['V10'][t, ...]
            data  = np.transpose(np.sqrt(np.square(datau) + np.square(datav)))
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v  == 'pr' or v == 'pr24':
            datac  = (ncf.variables['RAINC'][t, ...] ).filled(0.0)
            datanc = (ncf.variables['RAINNC'][t, ...] ).filled(0.0)
            data   = (datac + datanc).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v == 'par':
            data = (ncf.variables['SWDOWN'][t, ...]).transpose()
            data_list[i] = met.ep_met_data.ep_met_data(v, data)
        elif v == 'snowem':
            data = (ncf.variables['SNOW'][t, ...]).transpose()/1000.
            data_list[i] = met.ep_met_data.ep_met_data(v, data)

    return(data_list)


def ep_wrf_met_modeldates(datestimes):
    """
    Main import module, returns list of numpy arrays corresponding to the needed met fields in fields for the list of datestimes (datetime class) 
        Parameters:
            datestimes: list of python datetime objects for which one needs met data
        Returns:
            modeldates: list of meteo model timesteps covering datestimes
    """
    
    # first, find out the model dates and save along the information of in which file it is
    ncf = []
    modeldates = []
    delta_t = ep_cfg.input_params.met.met_tolerance # in seconds
    delta_t_obj = timedelta(seconds = delta_t)

    for f in ep_cfg.input_params.met.met_files:
        fp = os.path.join(ep_cfg.input_params.met.met_path, f)
        for fn in glob(fp):
            try:
                ncf.append(Dataset(fn,'r'))
                log.fmt_debug('NetCDF file {} added to meteorological inputs.', fn)
            except (RuntimeError, OSError):
                log.fmt_error('EE: Fatal error. Unable to open file {}. \nExit!', fn)
                raise

            times_tmp = ncf[-1].variables['Times'][:]

            ntimes = times_tmp.shape[0]
            numchars = times_tmp.shape[1]

            t = 0
            for i in range(ntimes):
                timestr = ''.join(times_tmp[i,j].decode('utf-8') for j in range(numchars))
                modeldates.append((ncf[-1], wrfdate2dt(timestr), t)) # save the file pointer, the model time and the index within the file
                t += 1

    if not isinstance(datestimes,list):
        datestimes = [datestimes]
    #numtimes = len(datestimes)

    modeldates.sort(key=lambda x: x[1])
    if datestimes[0] < (modeldates[0][1]-delta_t_obj) or datestimes[-1] > (modeldates[-1][1]+delta_t_obj):
        raise ValueError('EE: Fatal error. The provided files does not cover the requested time period: from {} to {}'.format(datestimes[0], datestimes[-1]))

    return modeldates

def ep_wrf_met(dt, modeldates, met_vars):
    """
    Main import module, returns list of numpy arrays corresponding to the needed met fields in met_vars for the list of datestimes (datetime class)
        Parameters:
            dt: datetime of the required meteo data
            modeldates: list of available wrf model datetimes
            met_vars: list of strings of field names
        Returns:
            metdata: list of ep_met_data_objects for  for given timestep dt
    """

    mdt = 0
    delta_t = ep_cfg.input_params.met.met_tolerance # in seconds
    # loop over the requested timesteps
    while  dt > modeldates[mdt][1]:
        mdt += 1

    if tolerance(dt,modeldates[mdt][1], delta_t = delta_t ): # tolerance in seconds
        log.fmt_debug('II: reading {} from {} ...', dt, modeldates[mdt][0].filepath())
        metdata = get_wrf(modeldates[mdt][0], modeldates[mdt][2], met_vars)
    else:
        log.fmt_debug('II: reading {} from between {} and {} ...'.format(dt, modeldates[mdt-1][0].filepath(), modeldates[mdt][0].filepath()))
        metdata1 = get_wrf(modeldates[mdt-1][0], modeldates[mdt-1][2], met_vars)
        metdata2 = get_wrf(modeldates[mdt][0], modeldates[mdt][2], met_vars)
        td_1 = (dt - modeldates[mdt-1][1]).seconds
        td_2 = (modeldates[mdt][1] - dt).seconds
        metdata = [(d1 * td_2 + d2 * td_1)/float(td_1 + td_2) for d1,d2 in zip(metdata1, metdata2)]
    return metdata


def ep_wrf_met_deaccumulate(met_vars, datestimes):
    # we need special treatment for WRF accumulated precip
    """
    if 'pr' in met_vars:
        pr_i = met_vars.index('pr')
        newpr = []
        dt1 =  datestimes[0]
        mdt = 0
        while  dt1 > modeldates[mdt][1]: # find the first FUME datetime larger then the 
            mdt += 1

        # get previous model timestep for pr
        ncdata1 = get_wrf(modeldates[mdt-1][0], modeldates[mdt-1][2], ['pr'])

        origpr = f[0][pr_i].data[:]
        deltat = abs((dt1-modeldates[mdt-1][1]).seconds)
        newpr.append((origpr - ncdata1[0].data[:])/deltat)

        #f[0][pr_i] = met.ep_met_data.ep_met_data( 'pr', (origpr - ncdata1[0].data[:])/deltat)

        for i in range(1,numtimes):
            
            newpr.append( (f[i][pr_i].data[:] - f[i-1][pr_i].data[:])/abs((datestimes[i]-datestimes[i-1]).seconds) )

        for i in range(numtimes):
            f[i][pr_i] =  met.ep_met_data.ep_met_data('pr', newpr[i] )
            
    return(f)
    """
