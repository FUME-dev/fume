# ep_wrf.py: reads, imports and interpolates WRF meteorological data

__author__ = "Peter Huszar"
__license__ = "GPL"
__email__ = "huszarpet@gmail.com"

import os
import sys

import numpy as np
from datetime import datetime, timedelta, timezone
#import pygrib
from netCDF4 import Dataset
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times,ep_rtcfg,ep_debug
from lib.ep_geo_tools import *
import met.ep_met as ep_met



WRF_names = {
"tas" : ( "T2", 2 ),
"ta"  : ( "T", 3 ), # perturbation potential temperature: Total pot. temp. in K = T + 300 (T is the perturbation pot. temp.); "normal" temp. = pot. temp *  (p/p_0)^kappa (p_0 = 100000 Pa)
"qas" : ( "Q2", 2 ),
"qa"  : ( "QA", 3 ),
"rsds": ( "RGRND", 2),
"par" : ( "SWDOWN", 2),
"ps"  : ( "PSFC", 2),
"pa"  : ( "P", 3 ), # !!! the perturbation pressure; real pressure = P + PB
"zf"  : None,
"ua"  : ("U", 3 ),
"va"  : ( "V", 3),
"uas"  :( "U10", 2 ),
"vas"  : ( "V10", 2 ),
"windspd"  : None,
"wndspd10m": None,
"pr" : None, # RAINC+RAINNC
"pr24": None,
'soim1': None, 
'soit1': None, 
'sltyp' : None

}

def wrfdate2dt(wrfdatestr):
    byyyy = int(wrfdatestr.split('_')[0].split('-')[0]) #2000-01-24_12:00:00
    bmm   = int(wrfdatestr.split('_')[0].split('-')[1]) #2000-01-24_12:00:00
    bdd   = int(wrfdatestr.split('_')[0].split('-')[2]) #2000-01-24_12:00:00
    bhh   = int(wrfdatestr.split('_')[1].split(':')[0]) #2000-01-24_12:00:00
    bmin  = int(wrfdatestr.split('_')[1].split(':')[1])
    bss   = int(wrfdatestr.split('_')[1].split(':')[2])
    return(datetime(byyyy, bmm,  bdd, bhh, bmin, bss))
   

class WRFreader():
    _netcdf_type = 'NETCDF4'

    tzinfo = timezone(timedelta(hours = ep_cfg.input_params.met.met_itzone ))

    def __init__(self):

        self._files_opened = []
        self._timesteps = []

        self.p = [0,0]
        self._files_opened.append(Dataset(os.path.join(ep_cfg.input_params.met.met_path,
        ep_cfg.input_params.met.met_files[0] ),'r', format=self._netcdf_type  ))
        self.pp = [0,0 ]

        infile = self._files_opened[0]
        start_date=infile.START_DATE
        if_bdatetime = wrfdate2dt(start_date) # datetime(byyyy, bmm,  bdd, bhh, bmin, bss) 
        # save the first timestep
        self._timesteps.append(if_bdatetime.replace(tzinfo=self.tzinfo))
 
    def dt(self,p=None):
        if p == None:
            infile = self._files_opened[self.p[0]]
            t = self.p[1]
        else:
            infile = self._files_opened[p[0]]
            t = p[1]
            nt = len(infile.dimensions["Time"])
            if t > nt-1:
                raise IndexError('the requested timestep is not available in file'
                ' {}'.format(ep_cfg.input_params.met.met_files[p[0]]))
        timestr =b''.join(infile.variables['Times'][t,:]).decode('utf-8')
        dt = wrfdate2dt(timestr).replace(tzinfo=self.tzinfo)

        return(dt)      

    def next(self):
        """ place reader to the next position which can be in the same file or at the beginning of the next file  """
        self.pp[0] = self.p[0]
        self.pp[1] = self.p[1]

        infile = self._files_opened[self.p[0]]
        nt = len(infile.dimensions["Time"])
        if self.p[1] < nt:
            self.p[1] += 1
            self._timesteps.append(self.dt())
        elif self.p[0] < len(ep_cfg.input_params.met.met_files)-1: # this not the last file 
            self.p[1] = 0
            self.p[0] += 1
            self._files_opened.append(Dataset(os.path.join(ep_cfg.input_params.met.met_path, 
            ep_cfg.input_params.met.met_files[self.p[0]] ),'r', format=self._netcdf_type  ))
            self._timesteps.append(self.dt())
        else: # last file and last timestep
            ep_debug('No more meteorogy')
            raise IndexError

    def getmet(self,fields,p=None):
        # gets the meteorology corresponding to the list of metnames


        """ gets meteorology from one WRF file for self.p or custom pointer if specified 
        """
        numfields = len(fields)
        data_list = np.empty((numfields),dtype=object)
        if p:
            p1 = p
        else:
            p1 = self.p
        
        infile = self._files_opened[p1[0]]
        k = p1[1]

        

        for i,f in enumerate(fields):
            
            if WRF_names[f]:
                data = infile.variables[WRF_names[f][0]][k, ...]
                data2 = infile.variables[WRF_names[f][0]][:]

                if WRF_names[f][1] == 2: 
                    data = data.transpose(1,0)    
                    data_list[i] = ep_met.ep_met_data(f,data[:,:,np.newaxis])
                else:
                    data = data.transpose(2,1,0)
                    data_list[i] = ep_met.ep_met_data(f,data[:,:,:])
            elif f  == 'wndspd10m':
                datau = infile.variables['U10'][k, ...]
                datav = infile.variables['V10'][k, ...]
                data  = np.sqrt(np.square(datau) + np.square(datav))
                data = data.transpose(1,0)
                data_list[i] = ep_met.ep_met_data(f,data[:,:,np.newaxis])
            elif f == 'pr' or f == 'pr24':
                datac  = infile.variables['RAINC'][k, ...] #/tstep # to get preci
                datanc = infile.variables['RAINNC'][k, ...]
                data   = (datac + datanc).transpose(1,0)
                data_list[i] = ep_met.ep_met_data(f,data[:,:,np.newaxis])
            elif f == 'soit1':
                data = infile.variables['TSLB'][k,0, ...]
                data = data.transpose(1,0)
                data_list[i] = ep_met.ep_met_data(f, data[:,:,np.newaxis])
            elif f == 'soim1':
                data = infile.variables['SMOIS'][k,0, ...]
                data = data.transpose(1,0)
                data_list[i] = ep_met.ep_met_data(f, data[:,:,np.newaxis])
            elif f == 'par':
                data = infile.variables['SWDOWN'][k, ...]
                data = data.transpose(1,0)
                data_list[i] = ep_met.ep_met_data(f, data[:,:,np.newaxis])


                                        
        return(data_list)

    def close(self):
        for f in self._files_opened:
            f.close()

def ep_wrf_met(fields,datestimes):
    """ Main import module, returns list of numpy arrays corresponding to the needed met fields in fields for the list of datestimes (datetime class) """


    if not isinstance(datestimes,list):
        datestimes = [datestimes]

    numtimes = len(datestimes)

    meteorology = np.empty((numtimes),dtype=object)


    # initialize the reader
    reader = WRFreader()


    # loop over the the requested timesteps
    
    for t,dt in enumerate(datestimes):
        # find the first  timestep greater than the requested timestep
        while reader.dt() < dt: 
            try:
                reader.next()
            except:
                ep_debug('Error reading next timestep in meteorology. The last successfully read was '
                ' {}'.format(reader.dt()))
                raise


        # the reader is placed at the first position greater than dt
        readerdt = reader.dt()
        if dt == readerdt:
            meteorology[t] = reader.getmet(fields) 
            print('Got meteorology for {}'.format(dt))
        elif dt < readerdt:
            dt1 = readerdt
            dt2 = reader.dt(p=reader.pp)
            met_data_1 = reader.getmet(fields, p=reader.pp)
            met_data_2 = reader.getmet(fields)
            td_1 = (dt - dt1).seconds # timedelta instance length
            td_2 = (dt2 - dt).seconds # timedelta instance length
            meteorology[t] = (met_data_1 * td_2 + met_data_2 * td_1)/float(td_1 + td_2)
            print('Got meteorology for {}'.format(dt))



    # we need special treatment if 24-hour running mean precipitation is needed
    if 'pr24' in fields:
        #create -24 h datetime
        
        m24 = timedelta(hours=24)
        datestimes_m24 = [dt - m24 for dt in datestimes]
        
        # the first timestep in the files:
        dt0 = reader._timesteps[0]
        pr24 = np.empty((numtimes),dtype=object)
        pr24_zero = [ep_met.ep_met_data('pr24', np.zeros((ep_cfg.input_params.met.met_nx, ep_cfg.input_params.met.met_ny,1), dtype=float))]

        reader_pr24 = WRFreader()
        datestimes_m24_bdt0 = [dt for dt in datestimes_m24 if dt < dt0]
        datestimes_m24_adt0 = [dt for dt in datestimes_m24 if dt >= dt0]

        tt = -1
        for t,dt in enumerate(datestimes_m24_bdt0):
            tt += 1
            pr24[tt] = pr24_zero
        nbdt = tt + 1 # number of "-24" timesteps before the first timestep in the file
        for t,dt in enumerate(datestimes_m24_adt0):
            tt += 1
            while reader_pr24.dt() < dt:
                reader_pr24.next()

            readerdt = reader_pr24.dt()
            if dt == readerdt:
                pr24[tt] = reader_pr24.getmet(['pr24'])
                #print('Got meteorology for {}'.format(dt))
            elif dt < readerdt:
                dt1 = readerdt
                dt2 = reader_pr24.dt(p=reader_pr24.pp)
                met_data_1 = reader_pr24.getmet(['pr24'], p=reader_pr24.pp)
                met_data_2 = reader_pr24.getmet(['pr24'])
                td_1 = (dt - dt1).seconds # timedelta instance length
                td_2 = (dt2 - dt).seconds # timedelta instance length
                pr24[tt] = (met_data_1 * td_2 + met_data_2 * td_1)/float(td_1 + td_2)
#                print('Got meteorology for {}'.format(dt))


        # modify 'pr24' in meteorology with the pr24 array
        # 24 h = 86400
        ipr24 = fields.index('pr24')
        for t in range(nbdt):
            meteorology[t][ipr24] = (meteorology[t][ipr24] - pr24[t][0])/(datestimes_m24_bdt0[t] - dt0).seconds * 86400
        for t in range(nbdt,numtimes):
            meteorology[t][ipr24] =  meteorology[t][ipr24] - pr24[t][0]

        reader_pr24.close()      
            
    reader.close()
    

    # interpolating to the case grid" 
    if ep_cfg.input_params.met.met_interp == True:
        meteorology_interp = [ep_met.met_interp(m) for m in meteorology]
        return(meteorology_interp)
    else:
        return(meteorology) 

