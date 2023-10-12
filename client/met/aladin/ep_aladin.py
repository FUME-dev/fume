"""
Description:
ep_aladin.py: reads ALADIN meteorological data
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
from datetime import datetime, timedelta, timezone
import pygrib
from lib.ep_config import ep_cfg
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
import  met.ep_met_data 



model_names = { # here we put the indicatorOfParameter, typeOfLevel, and level key values. Last position is used for scaling (original data are multiplied by this value)
"tas" : (11, "heightAboveGround", 2,1.),
"ta"  : (11, "hybrid", None,1.),
"qas" : (51, "heightAboveGround", 2,1.),
"qa"  : (51, "hybrid", None,1.),
"rsds": (111, "heightAboveGround", 0,1.), # Accumulated Surface net(=downward-upward) solar radiation [J/m^2]
"par" : (111, "heightAboveGround", 0,1.),
"ps"  : (1, "heightAboveGround", 0,1.),
"pa"  : (1, "hybrid", None,1.),
"zf"  : (6, "hybrid", None,1.),
"ua"  : (33, "hybrid", None,1.),
"va"  : (34, "hybrid", None,1.),
"uas" : (33, "heightAboveGround" , 10,1.),
"vas" : (34, "heightAboveGround" , 10,1.),
"windspd"  : None,
"wndspd10m": None,
"pr" : (61, "heightAboveGround", 0 ,1.), # indicatorOfParameter=61 in ALADIN gribs corresponds to the accumulated total precipitation [kg/m^2]
"pr24":(61, "heightAboveGround", 0 ,1.),
'soim1': (86, "heightAboveGround", 0, 0.1), # indicatorOfParameter=86 in ALADIN gribs corresponds to the soil moisture in top 1 cm [kg/m2]. It must be multiplied by 1/rho_h2o*100 to be converted to m3/m3
'soit1': (11, "heightAboveGround", 0, 1.),
'sltyp' : None
}


def tolerance(dt1,dt2,delta_t = 60.0):
    if abs((dt1-dt2).seconds) <= delta_t:
        return(True)
    else:
        return(False)

def grb2dt(fobj):
        msg = fobj.message(1)
        dataDate = msg.dataDate
        dataTime = msg.dataTime
        stepUnits = msg.stepUnits
        startStep = msg.startStep
        syyyy = dataDate//10000
        smmdd = dataDate % 10000
        smm = smmdd//100
        sdd = smmdd % 100
        shh = dataTime//100
        smmin = dataTime % 100
        tdelta = timedelta(hours=stepUnits*startStep)
        return(datetime(syyyy, smm, sdd, shh, smmin, tzinfo=timezone.utc)+tdelta)

def get_aladin(grbf, t, fields, deaccumulated = None, totaltstep = None):
    numfields = len(fields)
    data_list = np.empty((numfields),dtype=object) # this numpy will contains the met_data class objects for the timestep t
    infilename = grbf.name
    infileindex = pygrib.index(infilename, 'indicatorOfParameter', 'typeOfLevel', 'level')
    infileindex3d = pygrib.index(infilename, 'indicatorOfParameter', 'typeOfLevel')

    for i,f in enumerate(fields):
        if model_names[f]: 
            if f not in ['pr', 'par']:
                if model_names[f][1] == "hybrid":
                    data = np.flip(np.array([v.values*model_names[f][3] for v in infileindex3d.select(indicatorOfParameter=model_names[f][0], typeOfLevel = "hybrid")[:]]).transpose(), axis=2)
                else:
                    data = (infileindex.select(indicatorOfParameter=model_names[f][0],typeOfLevel=model_names[f][1], level=model_names[f][2])[0].values * model_names[f][3]).transpose()
                
                data_list[i] = met.ep_met_data.ep_met_data(f, data)
            else:
                data = deaccumulated[f][totaltstep].transpose() 
                data_list[i] = met.ep_met_data.ep_met_data(f, data)
        elif f  == 'wndspd10m':
            datau = infileindex.select(indicatorOfParameter=model_names['uas'][0],typeOfLevel=model_names['uas'][1], level=model_names['uas'][2])[0].values * model_names['uas'][3]
            datav = infileindex.select(indicatorOfParameter=model_names['vas'][0],typeOfLevel=model_names['vas'][1], level=model_names['vas'][2])[0].values * model_names['vas'][3]
            data = (np.sqrt(datau**2 + datav**2)).transpose()
            data_list[i]  = met.ep_met_data.ep_met_data(f,data[:,:])
        elif f == 'pr24':
            data   = (deaccumulated['pr'][totaltstep]*3600*24).transpose() # kg.m-2.s-1 * 1 hour * 24
            data_list[i] = met.ep_met_data.ep_met_data(f, data)

    return(data_list)

def ep_aladin_met(fields,datestimes):
    """
    Main import module, returns list of numpy arrays corresponding to the needed met fields in fields for the list of datestimes (datetime class) 
        Parameters:
            fields: list of strings of field names
            datestimes: list of python datetime objects for which one needs met data
        Returns:
            f: list of ep_met_data_objects for each timestep defined in datestimes            
    """

    # first, find out the model dates and save along the information of in which file it is
    grbf =  []
    modeldates = []
    delta_t = ep_cfg.input_params.met.met_tolerance # in seconds
    delta_t_obj = timedelta(seconds = delta_t)

    for f in ep_cfg.input_params.met.met_files:
        fn = os.path.join(ep_cfg.input_params.met.met_path, f)
        try:
            grbf.append(pygrib.open(fn))
        except (RuntimeError, OSError): 
            log.fmt_error('EE: Fatal error. Unable to open file {}. \nExit!', fn)
            raise

        ntimes = 1 # we assume one grb file contains one timestep

        t = 0
        modeldates.append((grbf[-1], grb2dt(grbf[-1]), t ))

    if not isinstance(datestimes,list):
        datestimes = [datestimes]

    numtimes = len(datestimes)

    if datestimes[0] < (modeldates[0][1]-delta_t_obj) or datestimes[-1] > (modeldates[-1][1]+delta_t_obj):
        
        raise ValueError('EE: Fatal error. The provided files does not cover the requested time period: from {} to {}'.format(datestimes[0], datestimes[-1]))

    # some fields are accumulated (eg. pr, PAR etc.). These are here read from from the files and are de-accumulated according to the previous files and the startStep parameter
    deaccumulated = None
    if ('pr' in fields) or ('par' in fields) or ('pr24' in fields):
        log.debug('II: pr/par/pr24 in fields: doing de-accumulation.')
        deaccumulated = {}
        infilenames = [f.name for f in grbf]
        infileindex = [pygrib.index(f, 'indicatorOfParameter', 'typeOfLevel', 'level') for f in infilenames]
        nx = grbf[0].message(1).Nx
        ny = grbf[0].message(1).Ny
        stepUnits = grbf[0].message(1).stepUnits
        if 'pr' in fields or 'pr24' in fields:
            deaccumulated['pr'] = []
            for i,index in enumerate(infileindex):
                startStep = grbf[i].message(1).startStep
                if startStep == 0: # the first model output, everything is zero
                    deaccumulated['pr'].append(np.zeros((ny, nx)))
                else: # higher startstep means we need to substract the previous timestep
                    try:
                        pr0 = infileindex[i-1].select(indicatorOfParameter=model_names['pr'][0],typeOfLevel=model_names['pr'][1], level=model_names['pr'][2])[0].values * model_names['pr'][3]
                    except:
                        log.error('EE: ALADIN gribfile has gridStep > 0 and no previous file provided.')
                        raise

                    pr1 = infileindex[i].select(indicatorOfParameter=model_names['pr'][0],typeOfLevel=model_names['pr'][1], level=model_names['pr'][2])[0].values * model_names['pr'][3]
                    pr = (pr1-pr0)/(36*stepUnits) # to get the 
                    deaccumulated['pr'].append(pr)
    
        if 'par' in fields:
            deaccumulated['par'] = []
            for i,index in enumerate(infileindex):
                startStep = grbf[i].message(1).startStep
                if startStep == 0: # the first model output, everything is zero
                    deaccumulated['par'].append(np.zeros((ny, nx)))
                else: # higher startstep means we need to substract the previous timestep
                    try:
                        par0 = infileindex[i-1].select(indicatorOfParameter=model_names['rsds'][0],typeOfLevel=model_names['rsds'][1], level=model_names['rsds'][2])[0].values * model_names['pr'][3]
                    except:
                        log.error('EE: ALADIN gribfile has gridStep > 0 and no previous file provided.')
                        raise

                    par1 = infileindex[i].select(indicatorOfParameter=model_names['rsds'][0],typeOfLevel=model_names['rsds'][1], level=model_names['rsds'][2])[0].values * model_names['rsds'][3]
                    par = 0.5*(par1-par0)/(36*stepUnits) # 0.5 as PAR is about half of the total downward shortwave radiation
                    deaccumulated['par'].append(par)


                    
                    
            
        
    f = []
    mdt = 0
   


    # loop over the the requested timesteps

    for dt in datestimes:
        while  dt > modeldates[mdt][1]:
            mdt += 1
        
        if tolerance(dt,modeldates[mdt][1], delta_t = delta_t ): # tolerance in seconds
            log.fmt_debug('II: reading {} from {} ...', dt, modeldates[mdt][0].name)

            ncdata = get_aladin(modeldates[mdt][0], modeldates[mdt][2], fields, deaccumulated = deaccumulated, totaltstep = mdt)
            f.append(ncdata)
        else:
            log.fmt_debug('II: reading {} from between {} and {} ...', dt, modeldates[mdt-1][0].filepath(), modeldates[mdt][0].filepath())
            ncdata1 = get_aladin(modeldates[mdt-1][0], modeldates[mdt-1][2], fields, deaccumulated = deaccumulated, totaltstep = mdt)
            ncdata2 = get_aladin(modeldates[mdt][0], modeldates[mdt][2], fields, deaccumulated = deaccumulated, totaltstep = mdt)
            td_1 = (dt - modeldates[mdt-1][1]).seconds
            td_2 = (modeldates[mdt][1] - dt).seconds
            f.append([(d1 * td_2 + d2 * td_1)/float(td_1 + td_2) for d1,d2 in zip(ncdata1, ncdata2)] )


        
    return(f)

