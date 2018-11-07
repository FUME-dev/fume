#!/usr/bin/env python3

# ep_met.py: imports meteorological data - interpolates to the case grid and exports data to the database

__author__ = "Peter Huszar"
__license__ = "GPL"

import sys
import struct
import numpy as np
from datetime import datetime, timedelta, timezone
import psycopg2
import os
from netCDF4 import Dataset, num2date
import lib.ep_io_fortran as fio

from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times,ep_create_srid,ep_create_grid, ep_rtcfg, ep_debug
from lib.ep_geo_tools import *
import pygrib 
import scipy.interpolate as spint
import scipy.spatial.qhull as qhull
from importlib import import_module
import pytz
"""
We use IPCC-abbrevations for internal meteorological variable naming
tas - temperature at surface [K]
ta  - 3D temperature [K]
qas - specific humidity at the surface [kg/kg]
qa  - 3D specific humidty
rsds - surface incident SW radiation [W/m2]
par - photosyntetically active radiation [W/m2]
pa - 3D pressure [Pa]
zf - layer interface heights [m]
uas - U-wind anemometer height (usually 10m) [m/s]
vas - V-wind anemometer height (usually 10m) [m/s]
ua - U-wind [m/s]
va - V-wind [m/s]
wndspd - 3D wind speed [m/s]
wndspd10m- wind speed at anemometer height (usually 10m) [m/s]
pr - precipiation flux [kg m-2 s-1]
pr24 - accumulated precipitation [kg m-2]
soim1 - Soil moisture [kg/m3] 1m
soilt - Soil temperature [K] 1m
"""

dim_names = {
"tas" : 2,
"ta"  : 3,
"qas" : 2,
"qa"  : 3,
"rsds": 2,
"par" : 2,
"ps"  : 2,
"pa"  : 3,
"zf"  : 3,
"uas"  : 2,
"vas"  : 2,
"ua"  : 3,
"va"  : 3,
"wndspd": 3,
"wndspd10m": 2,
"pr": 2,
"pr24": 2,
"soim1": 2,
"soit1": 2,
"sltyp" : 2
}


MCIP_names = {
"tas" : "TEMP2",
"ta"  : "TA",
"qas" : "Q2",
"qa"  : "QV",
"rsds": "RGRND",
"par" : None,
"ps"  : "PRSFC",
"pa"  : "PRES",
"zf"  : "ZF",
"ua"  : None,
"va"  : None,
"uas"  : None,
"vas"  : None,
"windspd"  : None,
"wndspd10m": "WSPD10",
"pr"  : None, ##RN+RC
"pr24": None
}
WRF_names = {
"tas" : "T2",
"ta"  : "T", # perturbation potential temperature: Total pot. temp. in K = T + 300 (T is the perturbation pot. temp.); "normal" temp. = pot. temp *  (p/p_0)^kappa (p_0 = 100000 Pa)
"qas" : "Q2",
"qa"  : "QA",
"rsds": "RGRND",
"par" : None,
"ps"  : "PSFC",
"pa"  : "P",# !!! the perturbation pressure; real pressure = P + PB
"zf"  : None,
"ua"  : "U",
"va"  : "V",
"uas"  : "U10",
"vas"  : "V10",
"windspd"  : None,
"wndspd10m": None,
"pr" : None, # RAINC+RAINNC
"pr24": None

}
RegCM_names = {
"tas" : "tas",
"ta"  : "t",
"qas" : "qas",
"qa"  : "qv",
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
"pr24": None
}
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
"pr24": None,
'soim1': (86, "heightAboveGround", 0), #  sfcSoilMoist / rho_h2o * 100
'soit1': (86, "heightAboveGround", 0),
'sltyp' : None
}

# domain parameters

nx = ep_cfg.domain.nx
ny = ep_cfg.domain.ny
nz = ep_cfg.domain.nz
delx = ep_cfg.domain.delx
dely = ep_cfg.domain.dely
xorg = ep_cfg.domain.xorg
yorg = ep_cfg.domain.yorg


# timeparam

itzone_out    = ep_cfg.run_params.time_params.itzone_out
bdatetime = ep_cfg.run_params.time_params.dt_init
ntimeint  = ep_cfg.run_params.time_params.num_time_int
tdelta    = ep_cfg.run_params.time_params.timestep
datestimes = ep_dates_times()




class ep_met_data():
    def getName(self):
        return self.name

    def getData(self):
        return self.data

    def getDim(self):
        return(self.data.size)

    def __init__(self, name, npdata):
        self.name = name
        self.data = npdata.copy()

    def __mul__(self,x):
        newdata = self.data * x    
        newname = self.name
        return (ep_met_data(newname, newdata))

    def __truediv__(self,x):
        newdata = self.data / x
        newname = self.name
        return (ep_met_data(newname, newdata))

    def __add__(self,x):
        if self.name != x.name:
            print('Error in ep_met_data: addition of two datafields of different name: {} and {}'.format(self.name,x.name))
            return (NotImplemented)
        else:
            newdata = self.data + x.data
            newname = self.name
            return (ep_met_data(newname, newdata))

    def __sub__(self,x):
        if self.name != x.name:
            print('Error in ep_met_data: subtraction of two datafields of different name: {} and {}'.format(self.name,x.name))
            return (NotImplemented)
        else:
            newdata = self.data - x.data
            newname = self.name
            return (ep_met_data(newname, newdata))


def get_met(met):
    if len(met) != 0:
        ep_debug('Getting meteorology for {}'.format(met))
        met_type  = ep_cfg.input_params.met.met_type
        if met_type == 'ALADIN':
            mtls = 'aladin' 
        elif met_type == 'WRF':
            mtls = 'wrf'   
        elif met_type == 'RegCM':
            mtls = 'regcm'
        else:
            print('EE: Unknown met model.')
            raise ValueError

        #from met.aladin.ep_aladin import ep_aladin_met
        
        mod_name = 'met.{}.ep_{}'.format(mtls,mtls)
        func_name = 'ep_{}_met'.format(mtls)
        mod_obj = import_module(mod_name)
        func_obj = getattr(mod_obj, func_name)
        
        ep_rtcfg['met'] = func_obj(met,datestimes)
    else:
        ep_debug('No meteorology needed to import.')

        
""" Interpolating done in two points following 

http://stackoverflow.com/questions/20915502/speedup-scipy-griddata-for-multiple-interpolations-between-two-irregular-grids

Thanks to Jaime!

    First the qhull Delaunay triangulation is called and the weights are returned. This is done only for the first interpolation. (slow)
    After these weights are used to each data. (superfast)
"""
def interp_weights(xy, uv,d=2):
    tri = qhull.Delaunay(xy)
    simplex = tri.find_simplex(uv)
    vertices = np.take(tri.simplices, simplex, axis=0)
    temp = np.take(tri.transform, simplex, axis=0)
    delta = uv - temp[:, d]
    bary = np.einsum('njk,nk->nj', temp[:, :d, :], delta)
    return (vertices, np.hstack((bary, 1 - bary.sum(axis=1, keepdims=True))))

def interpolate(values, vtx, wts):
    return (np.einsum('nj,nj->n', np.take(values, vtx), wts))

def met_interp(met_data):
    """Interpolating data into the case grid """

    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny    

    if  'lmet_interp' not in ep_rtcfg.keys():
        ep_debug('II: ep.met.met_interp: first interpolation => calculating interpolation weights for the met and case grid')
        srid = ep_cfg.domain.srid
        proj4 = ep_rtcfg['projection_params']['proj4string']
        delx = ep_cfg.domain.delx
        dely = ep_cfg.domain.dely
        xorg = ep_cfg.domain.xorg
        yorg = ep_cfg.domain.yorg
        
        case_proj = pyproj.Proj(ep_rtcfg['projection_params']['proj4string'])
        met_proj = pyproj.Proj( ep_cfg.input_params.met.met_proj)
        lon = np.empty((nx,ny),dtype=float)
        lat = np.empty((nx,ny),dtype=float)
        nx_met = ep_cfg.input_params.met.met_nx
        ny_met = ep_cfg.input_params.met.met_ny
        xorg_met = ep_cfg.input_params.met.met_xorg
        yorg_met = ep_cfg.input_params.met.met_yorg
        dx_met   = ep_cfg.input_params.met.met_dx
        npoints_met = nx_met*ny_met
        points_met = np.empty((nx_met * ny_met,2),dtype=float)
        ii = 0
        for j in range(ep_cfg.input_params.met.met_ny):
            for i in range(ep_cfg.input_params.met.met_nx):
                points_met[ii][0] = xorg_met+i*dx_met+dx_met/2-nx_met*dx_met/2
                points_met[ii][1] = yorg_met+j*dx_met+dx_met/2-ny_met*dx_met/2
                ii += 1    
    
        points_case     = np.empty((nx * ny,2),dtype=float)

        ii = 0
        for j in range(ny):
            for i in range(nx):
                points_case[ii][0], points_case[ii][1] = pyproj.transform(case_proj, met_proj, xorg+i*delx+delx/2-nx*delx/2, yorg+j*dely+dely/2-ny*dely/2)
                ii += 1



        vtx, wts = interp_weights(points_met, points_case)
        ep_rtcfg['lmet_interp'] = 1
        ep_rtcfg['met_interp_vtx'] = vtx
        ep_rtcfg['met_interp_wts'] = wts


    else:
        vtx = ep_rtcfg['met_interp_vtx']
        wts = ep_rtcfg['met_interp_wts']

    
    met_data_i = []
    for d in met_data[:]:
        nzz = d.data.shape[2]
        data_i_3d = np.empty((nx, ny, nzz), dtype=float)
        for i in range(nzz):
            values = d.data[:,:,i]
            values = values.flatten(order='C')
            ep_debug('II: ep_met.ep_interp: regridding for {}.'.format(d.name))
            data_i = interpolate(values, vtx, wts)
            if ep_cfg.input_params.met.met_type == 'WRF':
                data_i_3d[:,:,i] = data_i.reshape((ny, nx), order = 'F').transpose(1,0)
            else:
                data_i_3d[:,:,i] = data_i.reshape((ny, nx), order = 'C').transpose(1,0)


        met_data_i.append(ep_met_data(d.name, data_i_3d))
    return(met_data_i)

def met_export_grid():

    inputfiles = ep_cfg.input_params.met.met_paths
    mettype = ep_cfg.input_params.met.met_type

    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny
    nz = ep_cfg.domain.nz
    delx = ep_cfg.domain.delx
    dely = ep_cfg.domain.dely
    xorg = ep_cfg.domain.xorg
    yorg = ep_cfg.domain.yorg

    projection = ep_rtcfg['projection_params']['projection']
    p_alp      = ep_rtcfg['projection_params']['p_alp']
    p_bet      = ep_rtcfg['projection_params']['p_bet']
    p_gam      = ep_rtcfg['projection_params']['p_gam']
    XCENT      = ep_rtcfg['projection_params']['lon_central']
    YCENT      = ep_rtcfg['projection_params']['lat_central']

    nx_met, ny_met, nz_met, delx_met, dely_met, xorg_met, yorg_met, projection_met, XCENT_met, YCENT_met, p_alp_met, p_bet_met, p_gam_met =  get_projection_domain_params(inputfiles[0], ftype=mettype) 

    proj_met = create_projection(projection_met, XCENT_met, YCENT_met, p_alp_met, p_bet_met, p_gam_met)
    
    lsame = (nx == nx_met) and (ny == ny_met) and (delx == delx_met) and (dely == dely_met) and (xorg == xorg_met) and (yorg == yorg_met) and (projection == projection_met) and (p_alp == p_alp_met) and (p_bet == p_bet_met) and (p_gam == p_gam_met)
    
    if not lsame: # met domain differs from the 'case' domain. Need to export the frid into the database for interpolation
        srid = ep_create_srid(dtype='met')
        cur = ep_connection.cursor()
        from lib.ep_libutil import ep_connection # import the DB connection object
        cur = ep_connection.cursor()
        
        try:
            cur.callproc('ep_create_grid', [schema, 'met_grid', nx_met,
                                        ny_met, delx_met,
                                        dely_met, xorg_met,
                                        yorg_met, srid])
            cur.close()
            ep_connection.commit()
        except Exception as e:
            print("create_grid: unable to create met domain table {}. \n Error: {}".format(grid_name, e))
            ep_connection.rollback()
            raise e
            

    ep_rtcfg['met_lsame'] = lsame

def met_get_be_datestimes(infilename, filetype, endian='big'):
    """this function reads the met input file and returns the beginnig and last datetime object the file contains"""   
    if isinstance(infilename, str):
    ## opening the file
        if filetype == 'MCIP' or filetype == 'WRF' or filetype == 'RegCM':
            inf = Dataset(infile, 'r', format='NETCDF4')
        
        elif filetype == 'CAMx':
            inf = open(infile, mode='rb')
        elif filetype == 'ALADIN':
            inf = pygrib.open(infile)
        else:
            print('EE: Unknown file format {}!'.format(filetype))
            raise ValueError
        infile = inf
    else:
        infile = infilename
    
    if filetype == 'MCIP':
        # get datestimes from file
       
        nt = len(infile.dimensions["TSTEP"])

        #print(nt)
        byyyy = infile.SDATE//1000
        bjjj  = infile.SDATE % 1000
        bhh   = infile.STIME//10000
        bminss= infile.STIME%10000
        bmin = bminss//100
        bss =  bminss%100
        #print(byyyy,bjjj,bhh,bmin,bss)
        if_bdatestimes = datetime(byyyy, 1 ,1, 0,0,0) + timedelta(days=int(bjjj-1), hours=int(bhh), minutes=int(bmin), seconds=int(bss) )
#        if_bdatestimes =  timedelta(days=int(bjjj), hours=int(bhh), minutes=int(bmin), seconds=int(bss) )
        #print(fbdatestimes)              
        tstep = infile.TSTEP//10000*3600 # in seconds
        #print (tstep)
        tstep_deltat = timedelta(seconds=int(tstep))
        if_bdatetime = if_bdatestimes
        if_edatetime = if_bdatestimes + (nt - 1)*tstep_deltat
    elif filetype == 'WRF':
        nt = len(infile.dimensions["Time"])
        start_date=infile.START_DATE
        byyyy = int(start_date.split('_')[0].split('-')[0]) #2000-01-24_12:00:00
        bmm   = int(start_date.split('_')[0].split('-')[1]) #2000-01-24_12:00:00
        bdd   = int(start_date.split('_')[0].split('-')[2]) #2000-01-24_12:00:00
        bhh   = int(start_date.split('_')[1].split(':')[0]) #2000-01-24_12:00:00
        bmin  = int(start_date.split('_')[1].split(':')[1]) #2000-01-24_12:00:00
        bss   = int(start_date.split('_')[1].split(':')[2]) #2000-01-24_12:00:00
        
        if_bdatestimes = datetime(byyyy, bmm, bdd, bhh, bmin, bss) 
        tstep = infile.DT # in minutes
        tstep_deltat = timedelta(minutes=int(tstep))
        if_bdatetime = if_bdatestimes
        if_edatetime = if_bdatestimes + (nt - 1)*tstep_deltat

    elif filetype == 'CAMx':
        record = fio.read_record(infile, endian, '40s240siiifif')
        ft, notes, itzone_out, numfld, bdate, btime, edate, etime = record
        byyyy = 2000+bdate//1000
        bjjj  = bdate % 1000 -1
        bhh = int(btime)
        bmin = int(60.0*(btime - bhh))
        bss = int(60.0*( 60.0 * (btime - bhh) - bmin  ))
        #if_datestimes = []
        if_bdatetime = datetime(byyyy, 1 ,1) + timedelta(days=int(bjjj), hours=bhh, minutes=bmin, seconds=bss  ) 

        eyyyy = 2000+edate//1000
        ejjj  = edate % 1000 - 1
        ehh = int(etime)
        emin = int(60.0*(etime - ehh))
        ess = int(60.0*( 60.0 * (etime - ehh) - emin  ))
        if_edatetime = datetime(eyyyy, 1 ,1) + timedelta(days=int(ejjj), hours=ehh, minutes=emin, seconds=ess  ) 
    elif filetype == 'ALADIN':
        
        msg = infile.message(1)
        validityDate = msg.validityDate
        validityTime = msg.validityTime
        yyyy = validityDate//10000
        mmdd = validityDate % 10000
        mm = mmdd//100
        dd = mmdd % 100
        hh = validityTime//100
        mmin = validityTime % 100
        if_bdatetime = datetime(yyyy, mm, dd, hh, mmin)
        #print(yyyy,mm,dd, hh,mmin)
#        if_bdatetime = validdate
        if_edatetime = if_bdatetime                
    elif filetype == 'RegCM':
        from netCDF4 import num2date, date2num
        nt = len(infile.dimensions["time"])
        datetimetmp  = infile.variables["time"]
        datetimedata = datetimetmp[:]
        if_bdatetime = num2date(datetimedata[0], units=datetimetmp.units,calendar=datetimetmp.calendar) 
        if_edatetime = num2date(datetimedata[-1],units=datetimetmp.units,calendar=datetimetmp.calendar)     

    else:
        print('EE: Error in ep_met.met_get_timesdates: supported file types are MCIP, WRF, CAMx')        
        raise ValueError

    if isinstance(infilename, str):
        infile.close()
    
    tzinfo = timezone(timedelta(hours = ep_cfg.input_params.met.met_itzone ))
    return(if_bdatetime.replace(tzinfo=tzinfo), if_edatetime.replace(tzinfo=tzinfo)) # return the beginning and end datetime        

