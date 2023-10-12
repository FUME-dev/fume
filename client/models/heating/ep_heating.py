"""
Description: Emissions from heating sector.

It is assumed that emissions for a particular day are given by:

E(T_dmean) = E_yr * F(T) * norm.

where
1) E_yr is the annual emission which is fetched from the database as activity data of category "cat_heat" (see below).
2) F(T_d) represents the temperature dependence - T_d is the daily average temperature:
	F(T_d) = T_din - T_d, if T_d <= T_d0
	F(T_d) = 0., if T_d > T_d0
	where T_din is the indoor temperature (21 C by default) and T_d0 is the maximum heating day temperature (13 C by default)
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


import numpy as np
from os import path, walk
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times, ep_projection_params, ep_rtcfg, ep_connection, ep_create_schema
from netCDF4 import Dataset, date2num
from  met.ep_met_netcdf import write_netcdf
from models.heating.met_interp import interp_weights, interpolate, met_interp
from  met.ep_met_data import ep_met_data

_required_met = [ 'tas']


def t_func(T,td_out_max,td_in):
    if np.isscalar(T):
        if T <= td_out_max:
            return(td_in-T)
        else:
            return(0.0)
    else:
        result = np.empty(T.shape)
        result[:] =  T[:]
        mask = ( T <= td_out_max )
        result[mask] = td_in-T[mask]
        result[~mask] = 0.0
        return(result)


def get_annual(cfg, dim=2):
    """ This reads annual daily mean data. If dim=2, it is assumed that the data are 2D (+1D the time axis). if dim=0, the are considered as one timeseries only for the entire domain"""
    if dim == 2:
        ncf_t = Dataset(cfg.metfile_annual, 'r')
        temp = ncf_t.variables[cfg.tname_annual][:].transpose(2,1,0)
    else:
        f_t = open(cfg.metfile_annual,'r')
        lines = f_t.readlines()
        temp = np.array(lines,dtype=float)
    return(temp)


def get_temp(cfg, dim=2):
    """ This reads the temperature field for the given day as a single value or a 2D array"""
    if dim == 2:
        ncf_t = Dataset(cfg.metfile, 'r')
        temp = ncf_t.variables[cfg.tname][:].squeeze()
        temp = temp.transpose(1,0)        
    else:
        f_t = open(cfg.tfile,'r')
        lines = f_t.readlines()
        temp = lines[0]
    return(temp)
    

def preproc(cfg):
    """ This produces a file with emission normalization factors - sums of t_func across the year, either as one value for the whole case grid, or 2D array written in both case in a netcdf"""
    ep_datetimes = ep_dates_times()
    numtimes = len(ep_datetimes)
    tzinfo = ep_datetimes[0].tzinfo
    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny

    # make the filelist, if the annual data is stored not in a common directory (typical for ALADIN)
    indir = cfg.metdir
    filelist = []
    for subdir, dirs, files in walk(indir):
        for f in files:
            bn = path.basename(f)
            import re
            if cfg.mettype == 'ALADIN':
                if not re.match(r"ALAD4camx_[0-9]{10}_06.grb", bn):
                    filelist.append(path.join(subdir, f))
            elif cfg.mettype == 'WRF':
                if re.match(r"wrfout_d01_[0-9]{4}-[0-9]{2}-[0-9]{2}_([0-9]{2}:){2}[0-9]{2}",bn):
                    filelist.append(path.join(subdir, f))

    filelist.sort()
######################  ALADIN ###############################
    if cfg.mettype == 'ALADIN':
        import pygrib
        from met.aladin.ep_aladin import grb2dt
        # get info from the first file
        grbf1 = pygrib.open(filelist[0])
        msg = grbf1.message(1)
        daildata = np.zeros((cfg.met_nx,cfg.met_ny), dtype=float)
        dailymeans = []
        dcount = 0
        actual_date = grb2dt(grbf1).date()

        grbf1.close()

        for f in filelist:
            grbf = pygrib.open(f) 

            dt0 = grb2dt(grbf)
            dt = dt0.date()
            infileindex = pygrib.index(f, 'indicatorOfParameter', 'typeOfLevel', 'level')
            data = (infileindex.select(indicatorOfParameter=11,typeOfLevel="heightAboveGround", level=2)[0].values).transpose()          
            if dt == actual_date:
                
                daildata += data
                dcount += 1
            else:
                dailymeans.append(daildata/dcount)
                daildata = data
                actual_date = dt
                dcount = 1
        dailymeans = np.array(dailymeans)
        numdays = dailymeans.shape[0]
######################## WRF ################################
    elif cfg.mettype == "WRF":
        pass
        from met.wrf.ep_wrf import wrfdate2dt
        ncf1 = Dataset(filelist[0])
        daildata = np.zeros((cfg.met_nx,cfg.met_ny), dtype=float)
        dailymeans = []
        dcount = 0

        times_tmp = ncf1.variables['Times'][:]
        ntimes = times_tmp.shape[0]
        numchars = times_tmp.shape[1]
        timestr = ''.join(times_tmp[0,j].decode('utf-8') for j in range(numchars))
        actual_date = wrfdate2dt(timestr).date()
        ncf1.close()
        for f in filelist:
            ncf = Dataset(f)
            times_tmp = ncf.variables['Times'][:]
            ntimes = times_tmp.shape[0]
            numchars = times_tmp.shape[1]

            for i in range(ntimes):
                timestr = ''.join(times_tmp[i,j].decode('utf-8') for j in range(numchars))
                dt = wrfdate2dt(timestr).date()
                data = ncf.variables['T2'][i,...].squeeze().transpose()
                if dt == actual_date:
                    daildata += data
                    dcount += 1
                else:
                    dailymeans.append(daildata/dcount)
                    daildata = data
                    actual_date = dt
                    dcount = 1
        dailymeans = np.array(dailymeans)
        numdays = dailymeans.shape[0]

    suma = np.zeros((cfg.met_nx, cfg.met_ny), dtype=float)
    for i in range(cfg.met_nx):
        for j in range(cfg.met_ny):
            for d in range(numdays):
                suma[i,j] += t_func(dailymeans[d,i,j], cfg.td0,cfg.tdin)

    data = ep_met_data('sum', suma)
    write_netcdf([[data]], [ep_datetimes[0]], 'sum_orig.nc')

    if cfg.met_interp:
        suma =  met_interp(data, cfg).data

    ncfo = Dataset(path.join(cfg.workdir, cfg.metfile_tempsum),'w',format='NETCDF3_CLASSIC')
    ncfo.createDimension('time',1)
    ncfo.createDimension('ny',ny)
    ncfo.createDimension('nx',nx)
    ncfo_time = ncfo.createVariable('time','i4',('time'))
    ncfo_time.units = "years since "+str((ep_datetimes[0].replace(tzinfo=None)).year)+"-01-01"
    ncfo_time.calendar = "gregorian"
    ncfo_time[:] = 0
    ncfo_temp = ncfo.createVariable('sum','f4',('time','ny','nx'))
    ncfo_temp.units = 'K * days'
    ncfo_temp[0,:,:]  = suma.transpose(1,0)
    ncfo.close() 
   

def run(cfg, ext_mod_id):
    cat_heat = cfg.category
    ep_datetimes = ep_dates_times()
    numtimes = len(ep_datetimes)
    tzinfo = ep_datetimes[0].tzinfo
    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny

    houryear = 8765.76  # 365.24*24

    case_schema = ep_cfg.db_connection.case_schema
    
    #1) get gridded values of annual emissions
    with ep_connection.cursor() as cur:
        # get the species names
        q = str.format('SELECT DISTINCT ms.spec_id,  ms.name from  '
                    '"{case_schema}".ep_mod_spec_factors_all sf '
                    'JOIN '
                    '"{case_schema}".ep_out_species ms ON ms.spec_id = sf.spec_mod_id '
                    'WHERE cat_id IN %s ORDER BY spec_id;', case_schema=case_schema)

        cur.execute(q, (tuple([int(c) for c in cat_heat]),))
        heat_species = np.array(cur.fetchall())
        
        species = list(heat_species[:,1])
        spec_ids = list(heat_species[:,0])
        numspec = heat_species.shape[0]
        # get the annual heating emissions sum
        heat_emission = np.zeros((nx,ny,numspec), dtype=float) 
        q = str.format('SELECT tz.i, tz.j, spec_id, SUM({houryear}*emis.emiss) '
                    'FROM "{case_schema}".ep_sg_emissions_spec emis '
                    'JOIN "{case_schema}".ep_sources_grid USING(sg_id) '
                    'JOIN "{case_schema}".ep_grid_tz tz   USING(grid_id) '
                    'WHERE emis.cat_id IN %s GROUP BY tz.i, tz.j, spec_id;', houryear=houryear, case_schema=case_schema)
        cur.execute(q, (tuple([int(c) for c in cat_heat]),))
        for row in cur.fetchall():
            heat_emission[row[0]-1, row[1]-1,spec_ids.index(str(row[2]))-1] = row[3]

    #2) get the actual temperatures
    temp = np.empty((nx, ny, numtimes))
    for t in range(numtimes):
        for d in ep_rtcfg['met'][t]:
            if d.name == 'tas': # we need just a subset of all the meteorology
                temp[..., t] =  d.data

    td = t_func(np.mean(temp,axis = 2),  cfg.td0,cfg.tdin)


    #3) get the daily heating emissions from annual ones
    ncf = Dataset(path.join(cfg.workdir, cfg.metfile_tempsum),'r')
    suma = ncf.variables['sum'][0,:,:].transpose(1,0)
    
    for s in range(numspec):
        heat_emission[:,:,s] = heat_emission[:,:,s]/suma*td

    #4) calculate actual heating emissions for the FUME timesteps
    # get the timezone information for each of the gridboxes
    # TODO - need to revise work with time zones and time shifts!!! Current state is messy here
    tz_offset = np.zeros((nx, ny), dtype=int)
    with ep_connection.cursor() as cur:
        q = str.format(' SELECT g.i,g.j, EXTRACT(timezone_hour FROM ts.time_out) as tz_offset ' 
                       ' FROM "{case_schema}".ep_grid_tz g '
                       ' JOIN "{case_schema}".ep_timezones tz USING(tz_id) '
                       ' JOIN "{case_schema}".ep_time_zone_shifts ts USING(ts_id);', case_schema=case_schema)
        cur.execute(q)
        for row in cur.fetchall():
            tz_offset[row[0]-1, row[1]-1] = row[2] - ep_cfg.run_params.time_params.itzone_out 
    
    heatem = np.zeros((nx,ny,numtimes,numspec),dtype=float) 
    # apply timezone offsets
    # TODO - need to revise work with time zones and time shifts!!! Current state is messy here
    for t,dt in enumerate(ep_datetimes):
        hour = dt.hour
        for i in range(nx):
            for j in range(ny):
               nh = (hour + tz_offset[i,j]) % 24
               heatem[i,j,t,:] = heat_emission[i,j,:] * float(cfg.profile[nh])/24.0
               
    if cfg.write_emis:
        ncfo = Dataset(path.join(cfg.workdir, cfg.output),'w',format='NETCDF3_CLASSIC')
        ncfo.createDimension('time',numtimes)
        ncfo.createDimension('ny',ny)
        ncfo.createDimension('nx',nx)
        ncfo_time = ncfo.createVariable('time','f4',('time'))
        ncfo_time.units = "hours since "+str(ep_datetimes[0].replace(tzinfo=None))
        ncfo_time.calendar = "gregorian"
        ncfo_time[:] = date2num([i.replace(tzinfo=None) for i in ep_datetimes] ,units=ncfo_time.units,calendar=ncfo_time.calendar)
        ncfo_emis = []
        for i,s in enumerate(species):
            ncfo_emis.append(ncfo.createVariable(s,'f4',('time','ny','nx')))
            ncfo_emis[-1].units = 'mol/s or g/s'
            ncfo_emis[-1][:] = heatem[:,:,:,i].transpose(2,1,0)

        ncfo.close()
        
    if cfg.merge_emis: # same emission into rt_cfg
        if 'external_model_data' not in ep_rtcfg.keys():
            ep_rtcfg['external_model_data'] = {}

        ep_rtcfg['external_model_data']['heating'] = { 'data' : heatem[:,:,np.newaxis,:, :], 'species' : list(species) }
