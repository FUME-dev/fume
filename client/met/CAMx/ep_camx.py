# ep_wrf.py: reads, imports and interpolates CAMx input meteorological data

__author__ = "Peter Huszar"
__license__ = "GPL"
__email__ = "huszarpet@gmail.com"

import os
import sys
sys.path.append('/data/Work/Projects/TACR/emise/emisproc-main/client')
sys.path.append('/data/Work/Projects/TACR/emise/emisproc-main/client/met')

import numpy as np
from datetime import datetime, timedelta
#import pygrib
from netCDF4 import Dataset
from lib.ep_config import init_global
init_global('/data/Work/Projects/TACR/emise/config/ep_main.conf')
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times,ep_rtcfg,ep_debug
from lib.ep_geo_tools import *
import ep_met

endian  = 'big'

def ep_camx_files_open(infilename):
    """returns file objects """
    file_obj =  open(infilename,'rb')
    return (file_obj)
    
def ep_camx_files_close(*args):
    """close the file obejct listed in files_obj """
#   if not isinstance(files_obj,list):
#       files_obj = [files_obj]
 
    for f in args:
        f.close()

def ep_camx_get_met_2d(fobj):
    """ gets all meteorology from one 2D CAMx_ v6+ file """
    ft, notes, itzone_out, numfld, bdate, btime, edate, etime = fio.read_record(fobj, endian, '40s240siiifif')
    if (ft.decode('utf-8') != 'A   V   E   R   A   G   E               '):
        print(ft.decode('utf-8'),'!')
        print('EE: {} not a met input file!!!'.format(fobj))
        raise NameError
        
    plon,plat,iutm,xorg,yorg,delx,dely,nx,ny,nz,iproj,istag,tlat1,tlat2,rdum = fio.read_record(fobj, endian, 'ffiffffiiiiifff')
    ione,ione,nx,ny = fio.read_record(fobj, endian, 'iiii')
    fmt_str = 40*numfld*'s'
    record = fio.read_record(fobj, endian, fmt_str)
    field_1 = ''.join([ record[i].decode('utf-8') for i in range(40) ])
    if field_1 != 'T   S   U   R   F   _   K               ':
        raise ValueError('EE: not a 2D surface met file!!!')
               


    
    
#    return(data_list)



def ep_wrf_met(fields,datestimes):
    """ Main import module, returns list of numpy arrays corresponding to the need met fields in fields for the list of datestimes (datetime class) """
    files_list_abs = ep_cfg.input_params.met.met_paths
    filetype = ep_cfg.input_params.met.met_type
#    print(files_list_abs)

    if not isinstance(datestimes,list):
        datestimes = [datestimes]


    dt_start, dt_end = datestimes[0], datestimes[-1]

    # check if files cover the requested "datestimes" timeperiod
    ep_debug('check if files cover the requested timeperiod: start - {}, end - {}'.format(dt_start, dt_end))
    fs = ep_aladin_files_open(files_list_abs[0])
    fe = ep_aladin_files_open(files_list_abs[-1])
    first_met_dt, dummy = ep_met.met_get_be_datestimes(fs , filetype)
    dummy,  last_met_dt = ep_met.met_get_be_datestimes(fe , filetype)
    ep_aladin_files_close(fe) # we close the last file - do not need anymore

    if not (dt_start >= first_met_dt and dt_end <= last_met_dt):
            ep_debug('Error in ep_aladin: the requested timeperiod (start: {}, end: {}) is not covered by the meteorological data which starts at {} and ends at {}:'.format(dt_start,dt_end,first_met_dt,last_met_dt ) )
            ep_aladin_files_close(fs)
            raise ValueError

    # pointers - contain the file index in the list rom which we search the requested datetime
#    if  'aladin_file_index' not in ep_rtcfg:
    ep_rtcfg['aladin_file_index'] = 0
    i = 0
    numtimes = len(datestimes)
    

    fopen = []
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
        print(dtm1)
        lfound = False
        while not lfound:
            dt_met1, dummy = ep_met.met_get_be_datestimes(f1 , filetype)
            dt_met2, dummy = ep_met.met_get_be_datestimes(f2 , filetype)
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
                print(i)
                f1 = f2
                f2 = ep_aladin_files_open(files_list_abs[i+1])
                fopen.append(f2)
                print('File {} opened.'.format(files_list_abs[i+1]))





    t = 0 
    while t < numtimes:
        lfound = False
        while not lfound and t < numtimes:
            dt_met1, dummy = ep_met.met_get_be_datestimes(f1 , filetype)
            dt_met2, dummy = ep_met.met_get_be_datestimes(f2 , filetype)
#            print(dt_met1,dt_met2)
            if datestimes[t] == dt_met1:
                lfound = True
                meteorology[t] = ep_aladin_get_met(fields, f1) 
                print('Got meteorology for {}'.format(datestimes[t]))
                t += 1
            elif datestimes[t] > dt_met1 and datestimes[t] < dt_met2:
                lfound = True
                met_data_1 = ep_aladin_get_met(fields, f1)
                met_data_2 = ep_aladin_get_met(fields, f2)
                td_1 = (datestimes[t] - dt_met1).seconds # timedelta instance length
                td_2 = (dt_met2 - datestimes[t]).seconds # timedelta instance length
                meteorology[t] = (met_data_1 * td_2 + met_data_2 * td_1)/float(td_1 + td_2)
                print('Got meteorology for {}'.format(datestimes[t]))
                t += 1
            else:
                i += 1
                print(i)
                f1 = f2
                f2 = ep_aladin_files_open(files_list_abs[i+1])
                fopen.append(f2)
                print('File {} opened.'.format(files_list_abs[i+1]))


        


    try:
       ipar = fields.index('par')
       print('Calculating par')
       for k in range(numtimes,1,-1):
           meteorology[k-1][ipar] = (meteorology[k-1][ipar] - meteorology[k-2][ipar])/tstep * 3600.
       meteorology[0][ipar] = meteorology[0][ipar] - par0[0]
    except: # FIXME
        pass


    ep_aladin_files_close(*fopen)
    
    if 'pr24'  in fields: #24h running percipitation
        # first, create datetimes -24
        m24 = timedelta(hours=24)
        datestimes_m24 = [dt - m24 for dt in datestimes]
        f0 = ep_aladin_files_open(files_list_abs[0])
        dt0, dummy = ep_met.met_get_be_datestimes(f0 , filetype)
        datestimes_m24_previous = [dt - m24 for dt in datestimes if dt - m24 < dt0 ]
        datestimes_m24_previous = [dt - m24 for dt in datestimes if dt - m24 >= dt0 ]

            


    # interpolating to the case grid" 
    if ep_cfg.input_params.met.met_interp == True:
        meteorology_interp = [ep_met.met_interp(m) for m in meteorology]
        return(meteorology_interp)
    else:
        return(meteorology) 

if __name__  == "__main__":
    #fobj = ep_aladin_files_open()
#    fields = ['tas', 'qas', 'par', 'wndspd10m']
#    fields = ['pr24']
    filename='/data/Work/Projects/TACR/emise/input/met/CAMx/camx.2d.20050110'
    fobj = ep_camx_files_open(filename)
    ep_camx_get_met_2d(fobj)
#    datestimes = ep_dates_times()
#    met = ep_aladin_met(fields, datestimes)
#    print(type(met[0]),type(met[0][0]))
