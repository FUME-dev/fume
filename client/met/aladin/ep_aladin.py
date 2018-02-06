# ep_aladin.py: reads, imports and interpolates ALADIN meteorological data

__author__ = "Peter Huszar"
__license__ = "GPL"
__email__ = "huszarpet@gmail.com"

import os
import sys

import numpy as np
from datetime import datetime, timedelta
import pygrib

from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times,ep_rtcfg,ep_debug
from lib.ep_geo_tools import *
import met.ep_met as ep_met

ALADIN_names = { # here we put the indicatorOfParameter key value
"tas" : (11, "heightAboveGround", 2),
"ta"  : (11, "hybrid", None),
"qas" : (51, "heightAboveGround", 2),
"qa"  : (51, "hybrid", None),
"rsds": (111, "heightAboveGround", 0),
"par" : None,
"ps"  : (1, "heightAboveGround", 0),
"pa"  : (1, "hybrid", None),
"zf"  : (6, "hybrid", None),
"ua"  : (33, "hybrid", None),
"va"  : (34, "hybrid", None),
"uas" : (33, "heightAboveGround" , 10),
"vas" : (34, "heightAboveGround" , 10),
"windspd"  : None,
"wndspd10m": None,
"pr" : (61, "heightAboveGround", 0 ),
"pr24":(61, "heightAboveGround", 0 ),
'soim1': (86, "heightAboveGround", 0), #  sfcSoilMoist / rho_h2o * 100
'soit1': (11, "heightAboveGround", 0), 
'sltyp' : None
}

def ep_aladin_files_open(infilename):
    """returns file objects """

    file_obj =  pygrib.open(infilename)
    return (file_obj)
    
def ep_aladin_files_close(*args):
    """close the file obejct listed in files_obj """
 
    for f in args:
        f.close()

def ep_aladin_get_met(fields, fobj):
    """ gets meteorology from one ALADIN grib file - assumes that the file contains only ONE timestep  """
    infilename = fobj.name
    infileindex = pygrib.index(infilename, 'indicatorOfParameter', 'typeOfLevel', 'level')
    infileindex3d = pygrib.index(infilename, 'indicatorOfParameter', 'typeOfLevel')
    numfields = len(fields)
    data_list = np.empty((numfields),dtype=object)
    i = -1
    for f in fields:
        i += 1
        if ALADIN_names[f] != None:
            data = infileindex.select(indicatorOfParameter=ALADIN_names[f][0],typeOfLevel=ALADIN_names[f][1], level=ALADIN_names[f][2])[0].values
            data_list[i] = ep_met.ep_met_data(f,data[:,:,np.newaxis])
        elif f == 'wndspd10m':
            datau = infileindex.select(indicatorOfParameter=ALADIN_names['uas'][0],typeOfLevel='heightAboveGround', level=10)[0].values
            datav = infileindex.select(indicatorOfParameter=ALADIN_names['vas'][0],typeOfLevel='heightAboveGround', level=10)[0].values
            data = np.sqrt(datau**2 + datav**2)
            data_list[i]  = ep_met.ep_met_data(f,data[:,:,np.newaxis])
        elif f == 'par':
            data = 0.5 * infileindex.select(indicatorOfParameter=ALADIN_names['rsds'][0],typeOfLevel='heightAboveGround', level=0)[0].values
            data_list[i] = ep_met.ep_met_data(f,data[:,:,np.newaxis])
        elif f == 'pr24':
            data =infileindex.select(indicatorOfParameter=ALADIN_names['pr'][0],typeOfLevel='heightAboveGround', level=0)[0].values
            data_list[i] = ep_met.ep_met_data(f,data[:,:,np.newaxis])
        else:
            ep_debug('Unable to extract field {} from the ALADIN files.'.format(f))
            raise KeyError 
    return(data_list)



def ep_aladin_met(fields,datestimes):
    """ Main import module, returns list of numpy arrays corresponding to the needed met fields in argument "fields" for the list of datestimes (datetime class) """
    files_list_abs = ep_cfg.input_params.met.met_paths
    filetype = ep_cfg.input_params.met.met_type
#    print(files_list_abs)

    if not isinstance(datestimes,list):
        datestimes = [datestimes]


    dt_start, dt_end = datestimes[0], datestimes[-1]

    # check if files cover the requested "datestimes" timeperiod
    ep_debug('check if files cover the requested timeperiod: start - {}, end - {}'.format(dt_start, dt_end))
    print(files_list_abs)
    fs = ep_aladin_files_open(files_list_abs[0])
    fe = ep_aladin_files_open(files_list_abs[-1])
    first_met_dt, dummy = ep_met.met_get_be_datestimes(fs , filetype)
    dummy,  last_met_dt = ep_met.met_get_be_datestimes(fe , filetype)
    ep_aladin_files_close(fe) # we close the last file - do not need anymore

    if not (dt_start >= first_met_dt and dt_end <= last_met_dt):
            ep_debug('Error in ep_aladin: the requested timeperiod (start: {}, end: {}) is not covered by the meteorological data which starts at {} and ends at {}:'.format(dt_start,dt_end,first_met_dt,last_met_dt ) )
            ep_aladin_files_close(fs)
            raise ValueError

    # pointers - contain the file index in the list from which we search the requested datetime
    ep_rtcfg['aladin_file_index'] = 0
    i = 0
    numtimes = len(datestimes)
    # if pr24 is in fields, we need to introduce a second pointer which points to the -24 file (which is the first one or a later one)    
    if 'pr24' in fields:
        ii = 0
        ep_rtcfg['aladin_file_index_pr24'] = ii
        

    fopen = [] # list of file objects

    f1 = ep_aladin_files_open(files_list_abs[i])
    f2 = ep_aladin_files_open(files_list_abs[i+1])
    fopen.append(f1)
    fopen.append(f2)
    print('File {} opened.'.format(files_list_abs[i]))
    print('File {} opened.'.format(files_list_abs[i+1]))


    meteorology = np.empty((numtimes),dtype=object)   

    if 'par' in fields:
        # generate the -1 timestep
        tstep = ep_cfg.run_params.time_params.timestep
        dt_step = timedelta(seconds = tstep)
        dtm1 = datestimes[0] - dt_step
        lfound = False
        while not lfound:
            dt_met1, dummy = ep_met.met_get_be_datestimes(f1 , filetype)
            dt_met2, dummy = ep_met.met_get_be_datestimes(f2 , filetype)
            print(dtm1,dt_met1)
            if dtm1 == dt_met1:
                lfound = True
                par0 = ep_aladin_get_met(['par'], f1)
            elif dtm1 > dt_met1 and dtm1 < dt_met2:
                lfound = True
                met_data_1 = ep_aladin_get_met(['par'], f1)
                met_data_2 = ep_aladin_get_met(['par'], f2)
                td_1 = (dtm1 - dt_met1).seconds # timedelta instance length
                td_2 = (dt_met2 - dtm1).seconds # timedelta instance length
                par0 = (met_data_1 * td_2 + met_data_2 * td_1)/float(td_1 + td_2)
            else:
                i += 1
                f1 = f2
                f2 = ep_aladin_files_open(files_list_abs[i+1])
                fopen.append(f2)
                print('File {} opened.'.format(files_list_abs[i+1]))

    lpr24 = False
    if 'pr24'  in fields: #24h running percipitation
    # first, create datetimes -24
        m24 = timedelta(hours=24)
        datestimes_m24 = [max(first_met_dt,dt - m24) for dt in datestimes]
        nfirsttstep = datestimes_m24.count(first_met_dt)
        meteorology_pr24 = np.empty((numtimes),dtype=object)
        lpr24 = True

    t = 0 
    
    while  t < numtimes:
        dt_met1, dummy = ep_met.met_get_be_datestimes(f1 , filetype)
        dt_met2, dummy = ep_met.met_get_be_datestimes(f2 , filetype)

        if datestimes[t] == dt_met1:
            
            meteorology[t] = ep_aladin_get_met(fields, f1) 
            print('Got meteorology for {}'.format(datestimes[t]))
            t += 1
        elif datestimes[t] > dt_met1 and datestimes[t] < dt_met2:
            
            met_data_1 = ep_aladin_get_met(fields, f1)
            met_data_2 = ep_aladin_get_met(fields, f2)
            td_1 = (datestimes[t] - dt_met1).seconds # timedelta instance length
            td_2 = (dt_met2 - datestimes[t]).seconds # timedelta instance length
            meteorology[t] = (met_data_1 * td_2 + met_data_2 * td_1)/float(td_1 + td_2)
            print('Got meteorology for {}'.format(datestimes[t]))
            t += 1
        else:
            i += 1
            f1 = f2
            f2 = ep_aladin_files_open(files_list_abs[i+1])
            fopen.append(f2)
            print('File {} opened.'.format(files_list_abs[i+1]))


    if lpr24: # getting data for dt-24h for accumulated 24h precipiation
        tt = 0
    
        while tt < numtimes:
            dt_met1, dummy = ep_met.met_get_be_datestimes(fopen[ii] , filetype)
            dt_met2, dummy = ep_met.met_get_be_datestimes(fopen[ii+1] , filetype)
            if datestimes_m24[tt] == dt_met1:
                  
                meteorology_pr24[tt] = ep_aladin_get_met(['pr24'], fopen[ii])
                tt += 1
            elif datestimes_m24[tt] > dt_met1 and datestimes_m24[tt] < dt_met2:
                    
                met_data_1 = ep_aladin_get_met(['pr24'], fopen[ii])
                met_data_2 = ep_aladin_get_met(['pr24'], fopen[ii+1])
                td_1 = (datestimes_m24[tt] - dt_met1).seconds # timedelta instance length
                td_2 = (dt_met2 - datestimes_m24[tt]).seconds # timedelta instance length
                meteorology_pr24[tt] = (met_data_1 * td_2 + met_data_2 * td_1)/float(td_1 + td_2)
                tt += 1
            else:
                ii += 1
                    


    try:
       ipar = fields.index('par')
 #      print('Calculating par')
       for k in range(numtimes,1,-1):
           meteorology[k-1][ipar] = (meteorology[k-1][ipar] - meteorology[k-2][ipar])/3600.0 #tstep * 3600.
       meteorology[0][ipar] = (meteorology[0][ipar] - par0[0])/3600.0
    except: # FIXME
        pass 

    try:
        ipr24 = fields.index('pr24')
        for t in range(numtimes):
            meteorology[t][ipr24] = (meteorology[t][ipr24] - meteorology_pr24[t][0] ) / (datestimes[t]-datestimes_m24[t]).second * 86400 # scaling to 24 if needed
    except: # FIXME
        pass

    ep_aladin_files_close(*fopen)
    
            


    # interpolating to the case grid" 
    if ep_cfg.input_params.met.met_interp == True:
        meteorology_interp = [ep_met.met_interp(m) for m in meteorology]
        return(meteorology_interp)
    else:
        return(meteorology) 

