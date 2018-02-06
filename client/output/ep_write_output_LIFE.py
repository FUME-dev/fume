#!/usr/bin/env python3

# write_output.py: writes emission to model-ready output in one of the supported formats (CAMx/CMAQ)

__author__ = "Peter Huszar"
__license__ = "GPL"
__email__ = "huszarpet@gmail.com"

import sys
import struct
import numpy as np
import calendar
import time
import datetime
import psycopg2
import os
from netCDF4 import Dataset
import lib.ep_io_fortran as fio
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times, ep_ResultIter, ep_debug, ep_rtcfg
from lib.ep_geo_tools import projection_parameters_from_srid,\
        projection_parameters_from_proj4, create_projection
import lib.ep_io_fortran as mt
import pyproj
#from osgeo import osr, ogr

#@profile(stream=sys.stderr)
def write_emis(): # ep_cfg is a json configuration object(?), for now it is a temporary fortran namelist file

    endian =  'big' #sys.byteorder

    model = ep_cfg.run_params.output_params.model
    case_schema = ep_cfg.db_connection.case_schema
    source_schema = ep_cfg.db_connection.source_schema

    proj  = ep_rtcfg['projection_params']['proj']
    p_alp = ep_rtcfg['projection_params']['p_alp']
    p_bet = ep_rtcfg['projection_params']['p_bet']
    p_gam = ep_rtcfg['projection_params']['p_gam']
    XCENT = ep_rtcfg['projection_params']['lon_central']
    YCENT = ep_rtcfg['projection_params']['lat_central']
    proj4str = ep_rtcfg['projection_params']['proj4string']
    caseproj_obj = pyproj.Proj(proj4str)




    # domain parameters
    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny
    nz = ep_cfg.domain.nz
    
    

    delx = ep_cfg.domain.delx
    dely = ep_cfg.domain.dely
    xorg = ep_cfg.domain.xorg
    yorg = ep_cfg.domain.yorg
    

    # the S/W corner of the grid (as gridboxes)
#    if (caseproj_obj.is_latlong()):

    xorig = xorg - nx*delx/2.0
    yorig = yorg - ny*dely/2.0
    # timeparam
    itzone_out    = ep_cfg.run_params.time_params.itzone_out
#    bdatetime = ep_cfg.run_params.time_params.dt_init
    ntimeint  = ep_cfg.run_params.time_params.num_time_int
    tdelta    = ep_cfg.run_params.time_params.timestep

    # output
    iarea = ep_cfg.run_params.output_params.write_area
    areafile = os.path.join(ep_cfg.run_params.output_params.outpath,   ep_cfg.run_params.output_params.areafile)

    ipoint = ep_cfg.run_params.output_params.write_point
    pointfile = os.path.join(ep_cfg.run_params.output_params.outpath,  ep_cfg.run_params.output_params.pointfile)
    print(areafile,pointfile)

    # CMAQ params
    VGTYP  = ep_cfg.run_params.output_params.CMAQ_params.VGTYP
    VGTOP  = ep_cfg.run_params.output_params.CMAQ_params.VGTOP
    VGLVLS = ep_cfg.run_params.output_params.CMAQ_params.VGLVLS

    datestimes = ep_dates_times()
    
    edatetime  = datestimes[-1]
    bdatetime   = datestimes[0]
    datestimes.append(edatetime+datetime.timedelta(seconds=tdelta)) # add one more timeinterval
    ntimeint_p1 = ntimeint + 1
    zonehours = datetime.timedelta(hours=itzone_out)
    if model == 'CAMx':
        bdate = []
        btime = []
        for i in range(ntimeint_p1):
            #actual_date = actual_date + timedelta
            
            actual_date  = datestimes[i]
            byyyy = actual_date.year
            bmm   = actual_date.month
            bdd   = actual_date.day
            bhh   = actual_date.hour
            bmin  = actual_date.minute
            bss   = actual_date.second
            yearstart_date = datetime.datetime(byyyy, 1, 1,tzinfo=datetime.timezone(zonehours))
            bjjj = (actual_date - yearstart_date).days + 1
            bdate.append((1000*int(byyyy)+int(bjjj))%100000)
            btime.append(bhh+(bss/60.+bmin)/60.)
    elif model == 'CMAQ':
        bdate = []
        btime = []

        for i in range(ntimeint):
            actual_date = datestimes[i]
            byyyy = actual_date.year
            bmm   = actual_date.month
            bdd   = actual_date.day
            bhh   = actual_date.hour
            bmin  = actual_date.minute
            bss   = actual_date.second
            yearstart_date = datetime.datetime(byyyy, 1, 1,tzinfo=datetime.timezone(zonehours))
                        
            bjjj = (actual_date - yearstart_date).days + 1

            bdate.append(byyyy*1000+bjjj)
            btime.append(bhh*10000 + bmin * 100 + bss)

    #################################################################
    ### Projection flags for CAMx/CMAQ                          #####
    #################################################################

    # getting number based projection parameter for CAMx/CMAQ
    #PROJ4 CAMx CMAQ
    #lcc   LAMBERT-2 LAMGRD3=2(Lambert conformal conic)
    #merc MERCATOR-5 TRMGRD3=7(equatorial secant Mercator)
    #tmerc MERCATOR-5 TRMGRD3=8(transverse secant Mercator)
    #utm   UTM-1 UTMGRD3=5(UTM)
    #longlat LATLON-0 LATGRD3=1(Lat-Lon)
    #sterea RPOLAR-3 STEGRD3=4(general tangent stereographic)
    #stere  POLAR-4  POLGRD3=6(polar secant stereographic)

    if  proj == "LATLON":
        iproj = 0
        GDTYP = 1
    elif  proj == "UTM":
        iproj = 1
        GDTYP = 5
    elif  proj == "LAMBERT":
        iproj = 2
        GDTYP = 2
    elif  proj == "STEREO":
        iproj = 3
        GDTYP = 4
    elif  proj == "POLAR":
        iproj = 4
        GDTYP = 6
    elif  proj == "EMERCATOR":
        iproj = 5
        GDTYP = 7
    elif  proj == "MERCATOR":
        iproj = 5
        GDTYP = 3
    else:
        print( "EE: projection not known by CAMx and CMAQ. Exit.....")
        raise ValueError


    #######################################################################################
    #                                                                                     #
    #                 FETCHING DATA FROM THE DATABASE                                     #
    #                                                                                     #
    #######################################################################################
    maxspec = 200
    #TSTEP', 'LAY', 'ROW', 'COL')
    emis = np.zeros((nx, ny, nz, maxspec, ntimeint+1))

    try:
        from lib.ep_libutil import ep_connection # import the DB connection object
        emiscur = ep_connection.cursor()

        if iarea == 1:
            q = 'SELECT * from "{case_schema}".get_species'.format(case_schema=case_schema)
            emiscur.execute(q)
            s = np.array(emiscur.fetchall())
            numspec = s.shape[0]
            spec_ids = [ int(s[i,0])  for i in range(numspec) ]
            species  = [ s[i,1]  for i in range(numspec) ]

            print('Number of species written in area emission file: {numspec}'.format(numspec=numspec))
            print('Species: {species}'.format(species=species))
                     

        if ipoint == 1:
            q = 'SELECT * from "{case_schema}".get_species_point'.format(case_schema=case_schema)
            emiscur.execute(q)

            s = np.array(emiscur.fetchall())
            pnumspec = s.shape[0]
            pspec_ids = [ int(s[i,0])  for i in range(pnumspec) ]
            pspecies  = [ s[i,1]  for i in range(pnumspec) ]
            print(pspec_ids,pspecies)


            
            num_point_src = 0
            # get the pointsources params

            q = str.format('SELECT DISTINCT  '
                           'array[sg_id::float, lon, xstk, lat, ystk, height, diameter, temperature, velocity] '
                           'FROM "{case_schema}".ep_sg_emissions_spec em JOIN "{case_schema}".ep_sources_point psrc USING(sg_id)', case_schema=case_schema)
#                           csrid=ep_cfg.projection_params.projection_srid, ssrid=,case_schema=case_schema, source_schema=source_schema)
            emiscur.execute(q)
#            point_src_params = emiscur.fetchall()
            point_src_params = np.array(emiscur.fetchall(), dtype = np.float).squeeze(axis=1)
            numstk = point_src_params.shape[0]
            stacks_id = list(map(int,point_src_params[:,0]))
            
            ep_debug('Number of point sources: {}',format(numstk))

            # LIFE - asociate layer to each point source
            heights = make_heights()
            stacks_layer = np.zeros((numstk),dtype=int)
            for s in range(numstk):
                stacks_layer[s] = heights[int(point_src_params[s,5])]

            species = list(set(species) | set(pspecies))
            spec_ids = list(set(spec_ids) | set(pspec_ids))
            numspec = len(species)
            print(species)
            print(spec_ids)

            # add point emissions
            psrc_zyx = np.empty((numstk,3),dtype=int)
            for s in range(numstk):
                ix = int((point_src_params[s,2]-xorig)/delx) 
                jx = int((point_src_params[s,4]-yorig)/dely) 
                psrc_zyx[s,:] = [stacks_layer[s]-1,jx-1,ix-1]
            
    except psycopg2.DatabaseError as e:

        print ('Error %s',  e)
        raise

    finally:
        ep_debug('Finished reading sources parameters...')

        
        # if emisdata:
        #     emisdata.close()
    # get xstk, ystk, row and col numbers for each stack from lon/lat data
    #######################################################################################
    #                                                                                     #
    #                                      CAMx                                           #
    #                                                                                     #
    #######################################################################################
    pass
    #
    #######################################################################################
    #                                                                                     #
    #                                      CMAQ                                           #
    #                                                                                     #
    #######################################################################################

    # CMAQ area file?
    if (model == 'CMAQ' and iarea == 1):
            # some preparation
            # temporary values
        ep_debug('Writing CMAQ area emissions')

#        emisname = []
#        longemisname = []
#        for i in range(numspec): 
#            #emisname.append('')
#            longemisname.append('')
        
        longemisname = [ '{0:16s}'.format(species[i]) for i in range(numspec) ]

        cdt = datetime.datetime.now()
        
        cyyyy = cdt.year
        cmm   = cdt.month
        cdd   = cdt.day
        chh   = cdt.hour
        cmin  = cdt.minute
        css   = cdt.second
        yearstart_date = datetime.datetime(cyyyy, 1, 1)
        cjjj = (cdt - yearstart_date).days + 1
        cdate= cyyyy*1000+cjjj
        ctime= chh*10000 + cmin * 100 + css
        
        try:
            emisgrp = Dataset(areafile, 'w', format='NETCDF4')
            ep_debug('File {} opened'.format(areafile))
            emisgrp.createDimension('TSTEP', None)
            emisgrp.createDimension('DATE-TIME', 2)
            #emisgrp.createDimension('LAY', nz)
            #LIFE
            emisgrp.createDimension('LAY', 16)

            emisgrp.createDimension('VAR', numspec)
            emisgrp.createDimension('ROW', ny)
            emisgrp.createDimension('COL', nx)

            tflag = emisgrp.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
            tflag.units = '<YYYYDDD,HHMMSS>'
            tflag.long_name = 'FLAG            ' # length = 15
            tflag.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '
            emisvar = []
            #nx, ny, nz, maxspec, ntimeint
            #emis = emis.transpose(4,2,1,0,3)
            for i in range(numspec):
                emission = emisgrp.createVariable(species[i], 'f4', ('TSTEP', 'LAY', 'ROW', 'COL'))
                emission.long_name=longemisname[i]
                emission.units='moles/s for gases and  g/s for aerosols'
                emission.var_desc='Model species '+longemisname[i]
                emission[:] = 0.0
                emisvar.append(emission)
                #for t in range(ntimeint):
                #    emission[t,:,:,:] = emis[t,:,:,:,i]
            for t in range(ntimeint):
                tflag[t,:,0] = bdate[t]
                tflag[t,:,1] = btime[t]
                ep_debug('Fetching area emission data from DB for: {dt}'.format(dt=datestimes[t]))
                q = 'SELECT ep_emiss_time_series(%s,%s,%s,%s,%s::timestamp,%s::text,%s::boolean)'
                emiscur.execute(q, (nx ,ny, nz, spec_ids, datestimes[t], case_schema, ep_cfg.run_params.output_params.save_time_series_to_db))
                if ep_cfg.run_params.output_params.save_time_series_to_db:
                    ep_connection.commit()

                emis = np.array(emiscur.fetchone()[0]).transpose(2,1,0,3)
                # LIFE - read point emissions to write into 3D CMAQ emissions
                ep_debug('Fetching point emission data from DB for: {dt}'.format(dt=datestimes[t]))
                q = 'SELECT ep_pemiss_time_series(%s,%s,%s::timestamp,%s::text, %s::text)'
                emiscur.execute(q, (stacks_id, spec_ids, datestimes[t], source_schema, case_schema))
                pemis = np.array(emiscur.fetchone()[0])


                for i in range(numspec):
                    emisvar[i][t,0,:,:] = emis[0,:,:,i]
                    for s in range(numstk):
                        emisvar[i][t,psrc_zyx[s,0],psrc_zyx[s,1],psrc_zyx[s,2]] += pemis[s,i]

    # attributes
            emisgrp.EXEC_ID='????????????????'
            emisgrp.FTYPE=np.int32(1)
            emisgrp.SDATE=np.int32(bdate[0])
            emisgrp.STIME=np.int32(btime[0])
            emisgrp.CDATE=np.int32(cdate)
            emisgrp.CTIME=np.int32(ctime)
            emisgrp.WDATE=np.int32(cdate)
            emisgrp.WTIME=np.int32(ctime)            
            emisgrp.TSTEP=np.int32(tdelta/3600*10000)
            emisgrp.NTHIK = np.int32(1)
            emisgrp.NCOLS=np.int32(nx)
            emisgrp.NROWS=np.int32(ny)
            emisgrp.NLAYS=np.int32(nz)
            emisgrp.NVARS=np.int32(numspec)
            emisgrp.GDTYP=np.int32(GDTYP)
            emisgrp.VGTYP=np.int32(VGTYP)
            emisgrp.VGTOP=np.float32(VGTOP)
            emisgrp.VGLVLS =np.float32(VGLVLS)
            emisgrp.P_ALP=np.float64(p_alp)
            emisgrp.P_BET=np.float64(p_bet)
            emisgrp.P_GAM=np.float64(p_gam)
            emisgrp.XCENT=np.float64(XCENT)
            emisgrp.YCENT=np.float64(YCENT)
            emisgrp.XORIG=np.float64(xorig)
            emisgrp.YORIG=np.float64(yorig)
            emisgrp.XCELL=np.float64(delx)
            emisgrp.YCELL=np.float64(dely)
            emisgrp.GDNAM = ep_cfg.domain.grid_name
            emisgrp.UPNAM = '???'
            emisgrp.HISTORY = '???'
            setattr(emisgrp,'VAR-LIST', ''.join(longemisname))
            emisgrp.FILEDESC='emisieF'

        except IOError:
            print('EE: Error opening '+areafile+' netcdf file for writing. Check paths!')
            raise

        finally:
            emisgrp.close()


def make_heights():

    
    maxlevel = 20000
    mlevels = [0, 76, 152, 307, 544, 868, 1289, 1818, 2570, 3600, 4754, 6073, 7621, 9512, 11998, 16069]

    heights = np.zeros((maxlevel),dtype=int)
    for i in range(1,len(mlevels)):
        heights[mlevels[i-1]:mlevels[i]] = i

    return(heights)    


