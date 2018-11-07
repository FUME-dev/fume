import numpy as np

from os import path
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times, ep_projection_params, ep_rtcfg, ep_connection, ep_create_schema, ep_debug 
from datetime import datetime, timedelta
from netCDF4 import Dataset, num2date,date2num



"""
Emissions from heating sector.

It is assumed that emissions for a particular day are given by:

E(T_dmean) = E_yr * F(T) * norm.

where 
1) E_yr is the annual emission which is fetched from the database as activity data of category "cat_heat" (see below).
2) F(T_d) represents the temperature dependence - T_d is the daily average temperature:
	F(T_d) = T_din - T_d, if T_d <= T_d0
	F(T_d) = 0., if T_d > T_d0
	where T_din is the indoor temperature (21 C by default) and T_d0 is the maximum heating day temperature (13 C by default)
"""
#_required_met = [ 'tas', 'wndspd10m']





def func(T,td_out_max,td_in):
    if T <= td_out_max:
        return(td_in-T)
    else:
        return(0.0)

def get_annual(cfg, dim=2):
    """ This read annual daily mean data. If dim=2, it is assumed that the data are 2D (+1D the time axis). if dim=0, the are considered as one timeseries only for the entire domain"""
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
    """ This produces a file with emission normalization factors - sums of func across the year, either as one value for the whole case grid, or 2D array written in both case in a netcdf"""
    # get annual daily mean data (2D or 0D)
    ep_datetimes = ep_dates_times()
    numtimes = len(ep_datetimes)
    tzinfo = ep_datetimes[0].tzinfo
    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny
    # make the integrals
    if  cfg.file_dim_annual == 2:
        temp = get_annual(cfg,2)
        numdays = temp.shape[2]
        nx,ny = temp.shape[0], temp.shape[1]
        suma = np.zeros((nx, ny), dtype=float)
        for i in range(nx):
            for j in range(ny):
                for d in range(numdays):    
            
                    suma[i,j] += func(temp[i,j,d], cfg.td0,cfg.tdin)
    else:
        temp = get_annual(cfg,0)
        numdays = len(temp)
        suma = 0.
        for d in range(numdays):
            suma += func(temp[d], cfg.td0,cfg.tdin)
 

    ncfo = Dataset(cfg.workdir+"/sum.nc",'w',format='NETCDF3_CLASSIC')
    ncfo.createDimension('time',1)
    ncfo.createDimension('ny',ny)
    ncfo.createDimension('nx',nx)
    ncfo_time = ncfo.createVariable('time','i4',('time'))
    ncfo_time.units = "years since "+str((ep_datetimes[0].replace(tzinfo=None)).year)+"-01-01"
    ncfo_time.calendar = "gregorian"
    ncfo_time[:] = 0
    ncfo_temp = ncfo.createVariable('sum','f4',('time','ny','nx'))
    ncfo_temp.units = 'K * days'
    if cfg.file_dim_annual == 2:
        ncfo_temp[0,:,:]  = suma.transpose(1,0)
    else:
        ncfo_temp[0,:,:]  = suma
    ncfo.close() 
   

def run(cfg, ext_mod_id):
    cat_heat = cfg.category
    ep_datetimes = ep_dates_times()
    numtimes = len(ep_datetimes)
    tzinfo = ep_datetimes[0].tzinfo
    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny

    secyear = 31556736 # 365.24*24*3600


    conf_schema = ep_cfg.db_connection.conf_schema
    case_schema = ep_cfg.db_connection.case_schema
    #1) get gridded values of annual emissions

    with ep_connection.cursor() as cur:
        # get the species names
        q = str.format('SELECT DISTINCT ms.spec_id,  ms.name from  '
                    '"{case_schema}".ep_mod_spec_factors_all sf '
                    'JOIN '
                    '"{case_schema}".ep_out_species ms ON ms.spec_id = sf.spec_mod_id '
                    'WHERE cat_id = 2000000 ORDER BY spec_id;', case_schema=case_schema, cat_heat=cat_heat)

        cur.execute(q)
        heat_species = np.array(cur.fetchall())
        
        species = list(heat_species[:,1])
        spec_ids = list(heat_species[:,0])
        numspec = heat_species.shape[0]
        
        # get the annual heating emissions sum
        heat_emission = np.zeros((nx,ny,numspec), dtype=float) 
        q = str.format('SELECT tz.i, tz.j, s.spec_mod_id, {secyear}*sum(act.act_intensity*s.split_factor) '
                    'FROM "{case_schema}".ep_sg_activity_data act '
                    'JOIN "{case_schema}".ep_sources_grid USING(sg_id) '
                    'JOIN "{case_schema}".ep_grid_tz tz   USING(grid_id) '
                    'JOIN "{case_schema}".ep_mod_spec_factors_all s ON  act.cat_id = s.cat_id and act.act_unit_id = s.spec_in_id '
                    'WHERE act.cat_id = {cat_heat} GROUP BY tz.i, tz.j, s.spec_mod_id;', secyear=secyear, case_schema=case_schema, conf_schema=conf_schema, cat_heat=cat_heat)
        cur.execute(q)
        for row in cur.fetchall():
            heat_emission[row[0]-1, row[1]-1,spec_ids.index(str(row[2]))-1] = row[3]

    #2) get daily mean temperature - this return
    td = get_temp(cfg, dim=cfg.file_dim)
    #3) get the daily heating emissions from annual ones
    ncf = Dataset(cfg.workdir+"/sum.nc",'r')
    suma = ncf.variables['sum'][0,:,:].transpose(1,0)
#    for i in range(nx):
#        for j in range(ny):
#            if cfg.file_dim == 2:
#                heat_emission[i,j,:] = heat_emission[i,j,:]*secyear/suma[i,j]*td[i,j]
#            else:
#                heat_emission[i,j,:] = heat_emission[i,j,:]*secyear/suma*td
    for s in range(numspec):
        heat_emission[:,:,s] = heat_emission[:,:,s]/suma*td
    #4) calculate actual heating emissions for the FUME timesteps
    # get the timezone information for each of the gridboxes
    tz_offset = np.zeros((nx, ny), dtype=int)
    with ep_connection.cursor() as cur:
        q = str.format(' SELECT g.i,g.j, EXTRACT(timezone_hour FROM tz.time_out) as tz_offset ' 
                    'FROM "{case_schema}".ep_grid_tz g JOIN "{case_schema}".ep_time_zone_shifts tz USING(tz_id);', case_schema=case_schema)
        cur.execute(q)
        for row in cur.fetchall():
            tz_offset[row[0]-1, row[1]-1] = row[2] - ep_cfg.run_params.time_params.itzone_out 
    
    heatem = np.zeros((nx,ny,numtimes,numspec),dtype=float) 
    # apply timezone offsets
    for t,dt in enumerate(ep_datetimes):
        hour = dt.hour
        for i in range(nx):
            for j in range(ny):
               nh = (hour + tz_offset[i,j]) % 24
               heatem[i,j,t,:] = heat_emission[i,j,:] * float(cfg.profile[nh])/24.0
    if cfg.write_emis:
        ncfo = Dataset(cfg.workdir+"/output.nc",'w',format='NETCDF3_CLASSIC')
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

