import numpy as np

from os import path
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times, ep_projection_params, ep_rtcfg, ep_connection, ep_create_schema, ep_debug 
from datetime import datetime, timedelta
from netCDF4 import Dataset, num2date,date2num

_required_met = [ 'tas', 'wndspd10m']


cat = [
'Emise dojnice stáj - bod', 
'Emise dojnice hnůj ulož - bod', 
'Emise dojnice hnůj do OP', 
'Emise skot nedojený na TP', 
'Emise skot nezařazený na TP', 
'Emise ovce na TP', 
'Emise kozy na TP', 
'Emise prasata stáj bod',
'Emise prasata hnůj ulož - bod', 
'Emise prasata hnůj do OP',
'Emise drůbež stáj bod', 
'Emise drůbež hnůj ulož - bod',  
'Emise drůbež hnůj do OP' ] 
# v databazy - cislovani od 1
#       0  1 | Skot dojený     
#       1  2 | Skot nedojený   
#       2  3 | Skot nezařazený 
#       3  4 | Ovce            
#       4  5 | Kozy                                                                   
#       5  6 | Prasata         
#       6  7 | Drůbež                     
#

cat_map_to_animals = [ # orna puda = 0, trava = 1, hnuj = 2
(0,2),
(0,2),
(0,0),
(1,1),
(2,1),
(3,1),
(4,1),
(5,2),
(5,2),
(5,0),
(6,2),
(6,2),
(6,0) ]
numcat = len(cat)


cat_emisfactors = [ # category emissionfactors - emissions/animal/h for standard T and W.
0.000803266,
0.000053011,
0.000164177,
0.000827364,
0.001061950,
0.000046396,
0.000046396,
0.000329926,
0.000028558,
0.000042412,
0.000009639,
0.000000143,
0.000001779
]

sigma = [None, None, None,None,None,None,None, 30.0/365.25,  30.0/365.25,  20.0/365.25,  15.0/365.25, None, None, None, None]  # distribuce emisi behem roku - dni

mu    = [None, None, None,None,None,None,None, 91         ,          121,          213,          288, None, None, None, None]  # stred distribuce (den v roce)


cat_emis = [ # emise kg/zvire/yr
10.0,
5.0,
12.0,
10.3,
10.3,
0.45,
0.45,
3.2,
4.0,
3.1,
0.12,
0.02,
0.13
]




def func(i,T,W,day=None):
    # i - the function ID (1..16)
    if i == 1:
        if T < 12.5:
            Tcorr = 18
        else:
            Tcorr = 18 + 0.77*(T-12.5)
        f = Tcorr**0.89 * W**0.26
    elif i == 2:
        if T < 1:
            Tcorr = 4
        else:
            Tcorr = T + 3
        f = Tcorr**0.89 * W**0.26
    elif i == 3:
        Tcorr = max(1.0,T)
        f = Tcorr**0.89 * W**0.26
    elif i in [4, 5, 6, 7, 12, 13, 14]:
        Tcorr = 1
        Wcorr = np.exp(0.0419 * W)
        f = Tcorr * Wcorr
    else: # i 8 9 10 11 
        Tcorr = np.exp(0.0223 * T)
        Wcorr = np.exp(0.0419 * W)
        f = Tcorr * Wcorr * gauss(i,day)

    return(f)
                
def gauss(i,t):
    if mu[i-1] <= t:
        min_t = mu[i-1]
        max_t = t
    else:
        min_t = t
        max_t = mu[i-1]
    delta_t   = min(max_t-min_t, min_t+365-max_t)/365.25 # normalized difference
    
    g = np.exp( -delta_t**2 / ( 2*sigma[i-1]**2)) / (sigma[i-1] * 2.506628)  # sqrt(2*pi) = 2.506628274595178
    return(g)

def get_factor(cat,T,W,day=None):
    if   cat == 1:
        fact = func(2,T,W)
    elif cat == 2:
        fact = func(3,T,W)
    elif cat == 3:
        fact = 0.384615 * func(8,T,W,day=day) + 0.384615 * func(9,T,W,day=day) + 0.0384615 * func(10,T,W,day=day) + 0.1923076 * func(11,T,W,day=day)
    elif cat in [4,5,6,7]:
        fact = func(14,T,W)
    elif cat == 8:
        fact = func(1,T,W)
    elif cat == 9:
        fact = func(3,T,W)
    elif cat == 10:
        fact = 0.384615 * func(8,T,W,day=day) + 0.384615 * func(9,T,W,day=day) + 0.0384615 * func(10,T,W,day=day) + 0.1923076 * func(11,T,W,day=day)
    elif cat == 11:
        fact = func(1,T,W)
    elif cat == 12:
        fact = func(3,T,W)    
    else:
        fact = 0.384615 * func(8,T,W,day=day) + 0.384615 * func(9,T,W,day=day) + 0.0384615 * func(10,T,W,day=day) + 0.1923076 * func(11,T,W,day=day)


    return(fact)        
    
     
def run_nh3agri(cfg):
    
    ep_datetimes = ep_dates_times()
    numtimes = len(ep_datetimes)
    tzinfo = ep_datetimes[0].tzinfo
    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny
    case_schema = ep_cfg.db_connection.case_schema
    #1) get gridded values of number of animals
     
    animals2d = np.zeros((nx,ny,7,3),dtype=float)

    with ep_connection.cursor() as cur:
        pass
        q = str.format('SELECT tz.i, tz.j, act.act_unit_id, act.cat_id, act.act_intensity  FROM "{case_schema}".ep_sg_activity_data act JOIN "{case_schema}".ep_sources_grid USING(sg_id) JOIN "{case_schema}".ep_grid_tz tz  USING(grid_id); ', case_schema=case_schema)
        cur.execute(q)
        for row in cur.fetchall():
            animals2d[row[0]-1, row[1]-1,row[2]-1,row[3]-10000001] += row[4]


    #2) get meteorology
    temp = np.zeros((nx,ny,numtimes),dtype=float)
    wind = np.zeros((nx,ny,numtimes),dtype=float)
    
    for t in range(len(ep_datetimes)):
        for d in ep_rtcfg['met'][t]:
            if d.name == 'tas': # we need just a subset of all the meteorology
                temp[:,:,t] = d.data[:,:,0]-273.15
            if d.name == 'wndspd10m':
                wind[:,:,t] = d.data[:,:,0]


#3) calculate the actual emissions values
    nh3mol = 0.017031 #kg/mol
    nh3emis =  np.zeros((nx,ny,numtimes),dtype=float)
    for t,dt in enumerate(ep_datetimes):
        jul = (dt - datetime(dt.year,1,1,0,0,0).replace(tzinfo=tzinfo) ).days+1
        for i in range(nx):
            for j in range(ny):
                for c in range(numcat):
                    emis = cat_emisfactors[c] * animals2d[i,j,cat_map_to_animals[c][0],cat_map_to_animals[c][1]] * get_factor(c,temp[i,j,t],wind[i,j,t],day=jul)
                    nh3emis[i,j,t] += emis

    #3) write simple netcdf output
    if cfg.write_emis:
        nh3ofile =  cfg.nh3agri_output

        nh3group = Dataset(nh3ofile,'w',format='NETCDF3_CLASSIC')
        nh3group.createDimension('time',numtimes)
        nh3group.createDimension('ny',ny)
        nh3group.createDimension('nx',nx)
        nh3group.createDimension('nz',3) # orna puda/trava/hnuj
        nh3time = nh3group.createVariable('time','i4',('time'))
        nh3time.units = "hours since "+str(ep_datetimes[0].replace(tzinfo=None))
        nh3time.calendar = "gregorian"
        nh3time[:] = date2num([i.replace(tzinfo=None) for i in ep_datetimes] ,units=nh3time.units,calendar=nh3time.calendar)
        nh3 = nh3group.createVariable('NH3','f4',('time','ny','nx'))
        nh3.units = 'mol/s'

        if cfg.write_met:
            nh3_met_tas     =  nh3group.createVariable('tas','f4',('time','ny','nx'))
            nh3_met_wind10m =  nh3group.createVariable('wind10m','f4',('time','ny','nx'))
            nh3_met_tas[:]  = temp[:,:,:].transpose(2,1,0)
            nh3_met_wind10m[:] = wind[:,:,:].transpose(2,1,0)


        nh3[:]  = nh3emis.transpose(2,1,0) / (3600. * nh3mol) # mol/s
        animals= ['Skot_doj','Skot_nedoj','Skot_nezar','Ovce','Kozy','Prasata', 'Drubez']
        animalgrp = []
        for i,a in enumerate(animals):
            grp = nh3group.createVariable(a, 'f4',('nz','ny','nx'))

            grp[:] = animals2d[:,:,i,:].transpose(2,1,0)
            animalgrp.append(grp)

    if cfg.merge_emis: # same emission into rt_cfg
        if 'external_model_data' not in ep_rtcfg.keys():
            ep_rtcfg['external_model_data'] = {}
#                                                                       i,j,k=1       ,t,s=1
        ep_rtcfg['external_model_data']['nh3agri'] = { 'data' : nh3emis[:,:,np.newaxis,:,np.newaxis] / (3600. * nh3mol), 'species' : ['NH3'] }







def run(cfg,ext_mod_id):
    run_nh3agri(cfg)
    pass



def annualintegral(cat,metf):
    
    data = np.genfromtxt(metf,dtype=float,missing_values='NA')
    numhours = data.shape[0]
    integral = 0.0
    for i in [0,1]: 
        A = data[:,i]
        ok = -np.isnan(A)
        xp = ok.ravel().nonzero()[0]
        fp = A[ok]
        x  = np.isnan(A).ravel().nonzero()[0]
        A[np.isnan(A)] = np.interp(x, xp, fp)
        
        data[:,i]= A


    for h in range(numhours):
        day = h//24+1
        T = data[h,0]
        W = data[h,1]
        integral += get_factor(cat,T,W,day=day)
    return(integral)    

                                                                                      
if __name__ == '__main__':
    import sys
    fname = str(sys.argv[1])
    print(fname)
    for i in range(len(cat_emis)):
        Ef = cat_emis[i]/annualintegral(i,fname)
        print(Ef)
