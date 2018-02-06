#!/usr/bin/env python3


import sys
import struct
import numpy as np
import calendar
import time
import datetime
#import f90nml
import psycopg2
import os
#from osgeo import osr
from netCDF4 import Dataset
import lib.ep_io_fortran as fio
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times, ep_ResultIter, ep_debug, ep_rtcfg
from lib.ep_geo_tools import projection_parameters_from_srid,\
        projection_parameters_from_proj4, create_projection
import lib.ep_io_fortran as mt
import pyproj
#from memory_profiler import profile

#@profile(stream=sys.stderr)
def combine_2_spec(spec1,spec2): # function to combine emission species 
    return(list(set(spec1).union(spec2)))

def combine_2_emis(em1,spec1,em2,spec2): # function to combine emissions from ep_rtcfg['models'] 
    
    kx,ky,kz1 = em1.shape[0],em1.shape[1],em1.shape[2]
    kz2       = em2.shape[2]
    
    spec = combine_2_spec(spec1,spec2)
    numspec = len(spec)
    emisout = np.zeros((kx,ky,max(kz1,kz2),numspec),dtype=float)

    for s,i in enumerate(spec):

        if s in spec1:
            j = spec1.index(s)
            emisout[:,:,0:kz1,i] += em1[:,:,0:kz1,j]
        if s in spec2:
            j = spec2.index(s)
            emisout[:,:,0:kz2,i] += em2[:,:,0:kz2,j]
    return(emisout, spec)

def combine_model_emis(em,sp,t):
    try:
        emtmp,sptmp = em,sp
        for m in ep_rtcfg['external_model_data']:
            specmodel = ep_rtcfg['external_model_data'][m]['species']
            emismodel = ep_rtcfg['external_model_data'][m]['data'][:,:,:,t,:]
            emtmp,sptmp = combine_2_emis(emtmp,sptmp,emismodel,specmodel)
        return(emtmp,sptmp)
    except KeyError:
        return(em,sp)
  

def combine_model_spec(spec):
    # spec - spec list from FUME
    try:
        sptmp = spec
        for m in ep_rtcfg['external_model_data']:
            
            specmodel = ep_rtcfg['external_model_data'][m]['species']
            sptmp = combine_2_spec(sptmp,specmodel)
        return(sptmp)
    except KeyError:
        return(spec)
        
 
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
#                           csrid=ep_cfg.domain.srid, ssrid=4236,case_schema=case_schema, source_schema=source_schema)
            emiscur.execute(q)
#            point_src_params = emiscur.fetchall()
            point_src_params = np.array(emiscur.fetchall(), dtype = np.float).squeeze(axis=1)
            numstk = point_src_params.shape[0]
            stacks_id = list(map(int,point_src_params[:,0]))
            
            ep_debug('Number of point sources: {}',format(numstk))



    except psycopg2.DatabaseError as e:

        print ('Error %s',  e)
        raise

    finally:
        ep_debug('Finished reading point sources...')
        # if emisdata:
        #     emisdata.close()
    # get xstk, ystk, row and col numbers for each stack from lon/lat data
    ######################################################################################
    # get species from external models:
    ep_species = species[:]
    species = combine_model_spec(ep_species)
    numspec = len(species)
    ep_debug('Species from FUME: {}.'.format(','.join(ep_species)))
    ep_debug('Species from FUME and other models: {}.'.format(','.join(species)))
        #######################################################################################
    #                                                                                     #
    #                                      CAMx                                           #
    #                                                                                     #
    #######################################################################################

    # CAMx area file?
    if (model == 'CAMx' and iarea == 1):
        # projection parameters:

        # some preparation
        emisname = []
        longemisname = []
        for i in range(numspec): # for each species split the string into 4 chacater long substring, one for each letters (a CAMx requirement)
            emisname.append('')
            longemisname.append('')
        for i in range(numspec):
            emisname[i] = '{:10s}'.format(species[i])
            tmps = []
            for j in range(10):
                tmps.append(emisname[i][j]+'   ')
            longemisname[i] = tmps
    #   print longemisname

        # cut title string into characters with 3 trailing spaces (as CAMx requires)
        emiss = 'EMISSIONS '
        tmps = []
        for i in range(10):
            tmps.append(emiss[i]+'   ')
        emisslong = tmps

    # cut note string into characters with 3 trailing spaces (as CAMx requires)
        notes = 'CAMx area emissions created by EP'
        tmps = []
        if len(notes)>60:
                tmp = notes[0:60]
        else:
            tmp = '{:60s}'.format(notes)
        for i in range(60):
            tmps.append(tmp[i]+'   ')

        notesformatted = tmps


        istag = 0
#        emissions = np.arange(nx*ny*numvar*ntimeint).reshape((nx,ny,numvar,ntimeint),order='F')


    #   end of temporaty data
        # open output file
        af = open(areafile,mode='wb')

        ep_debug('Writing '+model+' area emissions...')
        ione = int(1)
        rdum = float(0.)
        mt.write_record (af, endian, '40s240siiifif', ''.join(emisslong).encode('utf-8'),''.join(notesformatted).encode('utf-8'),int(itzone_out),int(numspec),int(bdate[0]),btime[0],int(bdate[-1]),btime[-1] )
        mt.write_record (af, endian, 'ffiffffiiiiifff', XCENT, YCENT,int(p_alp),xorig,yorig,delx,dely,nx,ny,nz,int(iproj),int(istag),p_alp,p_bet,rdum)
        mt.write_record (af, endian, 'iiii', ione,ione,nx,ny)

        joinedstr = []
        for i in range(numspec):
            joinedstr.append(''.join(longemisname[i]))

        fmt_str = str(40*numspec)+'s'
        mt.write_record (af, endian, fmt_str, ''.join(joinedstr).encode('utf-8') )

        for t in range(ntimeint):
            mt.write_record(af, endian, 'ifif', bdate[t],btime[t],bdate[t+1],btime[t+1])

            ep_debug('Fetching area emission data from DB for: {dt}'.format(dt=datestimes[t]))

            q = 'SELECT ep_emiss_time_series(%s,%s,%s,%s,%s::timestamp,%s::text,%s::boolean)'
            emiscur.execute(q, (nx ,ny, nz, spec_ids, datestimes[t], case_schema, ep_cfg.run_params.output_params.save_time_series_to_db))
            if ep_cfg.run_params.output_params.save_time_series_to_db:
                ep_connection.commit()

            emis = np.array(emiscur.fetchone()[0])
            emis_ep = np.sum(emis, axis=2, keepdims=True) # in CAMx, we do not have elevated emissions (3D emissions), so sum up to the ground

            # combine with model emissions
            emisout, sp = combine_model_emis(emis_ep,ep_species,t)

           # if 'megan' in ep_cfg.run_params.models.models:
           #             
           #     emis_megan = np.zeros((nx,ny,numspec_megan),dtype=float)
           #     for i in range(numspec_megan):
           #         emis_megan[:,:,i] = m.variables[megan_species[i]][t,0,...].transpose(1,0)
           # 
           #     emisout = combine_emis(emis_ep,ep_species,emis_megan,megan_species)
           # else:
           #     emisout = emis_ep

            for i in range(numspec):
                fmt_str = 'i40s'+str(nx*ny)+'f'
                data = np.sum(emisout[:,:,:,i], axis=2).flatten('F')*tdelta # mol/timeinterval
                
                print('Writing species {}'.format(species[i]))
                mt.write_record(af, endian, fmt_str, ione, ''.join(longemisname[i]).encode('utf-8'), *data)
        af.close()
        print( model+' area file succesfully written')


    # CAMx point source file?
    if (model == 'CAMx' and ipoint == 1):
        ep_debug('Writing '+model+' point source emissions...')
        ione=int(1)
        rdum = 0.
        emisname = []
        longemisname = []
        for i in range(pnumspec): # for each species split the string into 4 chacater long substring, one for each letters (a CAMx requirement)
            emisname.append('')
            longemisname.append('')

        for i in range(pnumspec):
            emisname[i] = '{:10s}'.format(pspecies[i])
            tmps = []
            for j in range(10):
                tmps.append(emisname[i][j]+'   ')
            longemisname[i] = tmps

            # cut title string into characters with 3 trailing spaces (as CAMx requires)
        emiss = 'PTSOURCE  '
        tmps = []
        for i in range(10):
            tmps.append(emiss[i]+'   ')
        emisslong = tmps

    # cut note string into characters with 3 trailing spaces (as CAMx requires)
        notes = 'CAMx point source emissions created by EP'
        tmps = []
        if len(notes)>60:
            tmp = notes[0:60]
        else:
            tmp = '{:60s}'.format(notes)
        for i in range(60):
            tmps.append(tmp[i]+'   ')

        notesformatted = tmps


        istag = 0
        iutm = 0


        pf = open(pointfile,mode='wb')

        mt.write_record(pf, endian, '40s240siiifif', ''.join(emisslong).encode('utf-8'),''.join(notesformatted).encode('utf-8'),itzone_out,pnumspec,bdate[0],btime[0],bdate[-1],btime[-1] )
        mt.write_record(pf, endian, 'ffiffffiiiiifff', XCENT, YCENT,int(p_alp),xorig,yorig,delx,dely,nx,ny,nz,iproj,istag,p_alp,p_bet,rdum)
        mt.write_record(pf, endian, 'iiii', ione,ione,nx,ny)

        joinedstr = []
        for i in range(pnumspec):
            joinedstr.append(''.join(longemisname[i]))
        fmt_str = str(40*pnumspec)+'s'

        mt.write_record(pf, endian, fmt_str, ''.join(joinedstr).encode('utf-8') )

        mt.write_record(pf, endian, 'ii', ione, numstk)

        var_list = []
        for i in range(numstk):
#            if lpig[i]: # PiF flag on - dstk must be negative
#                dstk[i] = -abs(dstk[i])-0.01 # to make sure stack diameter will be < 0 for PiG calculation
# 'ST_x(ST_Transform(geom_def.geom,{ssrid})), ST_x(ST_Transform(geom_def.geom,{csrid})), ST_y(ST_Transform(geom_def.geom,{ssrid})), ST_y(ST_Transform(geom_def.geom,{csrid})), '
#                            'src.height, src.diameter, src.temperature, src.velocity
            #stk_list = [ float(xstk[i]), float(ystk[i]), float(hstk[i]), float(dstk[i]), float(tstk[i]), float(vstk[i]) ]
            stk_list = [ point_src_params[i,2], point_src_params[i,4],  point_src_params[i,5],  point_src_params[i,6],  point_src_params[i,7],  point_src_params[i,8] ]
            var_list.extend(stk_list)

        fmt_str = str(6*numstk)+'f'
        mt.write_record(pf, endian, fmt_str, *var_list)

        # end writing the time-invariant portion, starting time variant part
        for t in range(ntimeint):
            mt.write_record(pf, endian, 'ifif', bdate[t],btime[t],bdate[t+1],btime[t+1] )
            mt.write_record(pf, endian, 'ii', ione, numstk )
            var_list = []
            # create list to write
            for i in range(numstk):
                var_list.extend([ ione, ione, 1, 0.0, 0.0 ] )
            fmt_str = numstk*'iiiff'

            mt.write_record(pf, endian, fmt_str, *var_list )

            ep_debug('Fetching point emission data from DB for: {dt}'.format(dt=datestimes[t]))

            q = 'SELECT ep_pemiss_time_series(%s,%s,%s::timestamp,%s::text, %s::text)'
            emiscur.execute(q, (stacks_id, pspec_ids, datestimes[t], source_schema,  case_schema))
            pemis = np.array(emiscur.fetchone()[0])

            for i in range(pnumspec):
                data = pemis[:,i]*tdelta
                fmt_str = 'i40s'+str(numstk)+'f'
                mt.write_record(pf, endian, fmt_str, ione, ''.join(longemisname[i]).encode('utf-8') , *data)

        pf.close()
        print (model+' point file succesfully written')

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

        # CMAQ params
        VGTYP  = ep_cfg.run_params.output_params.CMAQ_params.VGTYP
        VGTOP  = ep_cfg.run_params.output_params.CMAQ_params.VGTOP
        VGLVLS = ep_cfg.run_params.output_params.CMAQ_params.VGLVLS


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
            emisgrp.createDimension('LAY', nz)
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

                emis_ep = np.array(emiscur.fetchone()[0])
                
                emisout, specout = combine_model_emis(emis_ep,ep_species,t)
                for i in range(numspec):
                    emisvar[i][t,:,:,:] = emisout[:,:,:,i].transpose(2,1,0)



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
    # CMAQ point files?
    if (model == 'CMAQ' and ipoint == 1):
        ep_debug('Writing CMAQ point emissions')

        longemisname = [ '{0:16s}'.format(pspecies[i]) for i in range(pnumspec) ]
        # CMAQ params
        VGTYP  = ep_cfg.run_params.output_params.CMAQ_params.VGTYP
        VGTOP  = ep_cfg.run_params.output_params.CMAQ_params.VGTOP
        VGLVLS = ep_cfg.run_params.output_params.CMAQ_params.VGLVLS


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

            pointfile_name, pointfile_extension= os.path.splitext(pointfile)
            pointfile_stacks = pointfile_name + '_stacks' + pointfile_extension
            stackgrp = Dataset(pointfile_stacks, 'w', format='NETCDF4')
            pemisgrp = Dataset(pointfile, 'w', format='NETCDF4')

            ep_debug('Files {} and {}  opened'.format(pointfile, pointfile_stacks))

            stackgrp.createDimension('TSTEP', None)
            stackgrp.createDimension('DATE-TIME', 2)
            stackgrp.createDimension('LAY', 1)
            stackgrp.createDimension('VAR', pnumspec)
            stackgrp.createDimension('ROW', numstk)
            stackgrp.createDimension('COL', 1)

            tflag = stackgrp.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
            tflag.units = '<YYYYDDD,HHMMSS>'
            tflag.long_name = 'FLAG            ' # length = 15
            tflag.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '
            tflag[0,:,0] = bdate[0]
            tflag[0,:,1] = btime[0]

# integert variables
            var_int=      ['ISTACK', 'STKCNT', 'ROW', 'COL', 'LMAJOR','LPING']
            var_int_units=['none'  , 'none'  , 'none','none','none',  'none' ]

            stkintvars = []
            for var in var_int:
                stkvar = stackgrp.createVariable(var, 'i4', ('TSTEP', 'LAY', 'ROW', 'COL'))
                stkvar.longname='{0:16s}'.format(var)
                unit   = var_int_units[var_int.index(var)]
                stkvar.unit = '{0:16s}'.format(unit)
                stkintvars.append(stkvar)

            #ISTACK
            stkintvars[0][0,0,:,0] = stacks_id
            #STKCNT
            stkintvars[1][0,0,:,0] = range(0,numstk)

            #ROW
            stkintvars[2][0,0,:,0] = (point_src_params[:,2]-xorig)/delx + 1
            #COL
            stkintvars[3][0,0,:,0] = (point_src_params[:,4]-yorig)/dely + 1
            #LMAJOR
            stkintvars[4][0,0,:,0] = 0
            #LPING
            stkintvars[5][0,0,:,0] = 0


# float variables
            var_float=      ['LATITUDE','LONGITUDE','STKDM','STKHT','STKTK',   'STKVE','STKFLW','XLOCA','YLOCA']
            var_float_units=['degrees' ,'degrees'  ,'m'    ,'m'    ,'degrees K','m/s' ,'m**3/s',''     ,''     ]

            stkfloatvars = []
            print(point_src_params.shape)
            for var in var_float:
                stkvar = stackgrp.createVariable(var, 'i4', ('TSTEP', 'LAY' , 'ROW', 'COL'))
                stkvar.longname='{0:16s}'.format(var)
                unit   = var_float_units[var_float.index(var)]
                stkvar.unit = '{0:16s}'.format(unit)
                stkfloatvars.append(stkvar)
            #LATITUDE
            stkfloatvars[0][0,0,:,0] = point_src_params[:,3]
            #LONGITUDE
            stkfloatvars[1][0,0,:,0] = point_src_params[:,1]
            #STKDM
            stkfloatvars[2][0,0,:,0] = point_src_params[:,6]
            #STKHT
            stkfloatvars[3][0,0,:,0] = point_src_params[:,5]
            #STKTK
            stkfloatvars[4][0,0,:,0] = point_src_params[:,7]
            #STKVE
            stkfloatvars[5][0,0,:,0] = point_src_params[:,8]
            #STKFLW
            stkfloatvars[6][0,0,:,0] = (0.5*point_src_params[:,6])**2 * np.pi * point_src_params[:,8]
            #XLOCA
            stkfloatvars[7][0,0,:,0] = point_src_params[:,2]
            #YLOCA
            stkfloatvars[8][0,0,:,0] = point_src_params[:,4]

    # attributes
            stackgrp.EXEC_ID='?????????????????????'

            stackgrp.FTYPE=np.int32(1)
            stackgrp.SDATE=np.int32(bdate[0])
            stackgrp.STIME=np.int32(btime[0])
            stackgrp.CDATE=np.int32(cdate)
            stackgrp.CTIME=np.int32(ctime)
            stackgrp.WDATE=np.int32(cdate)
            stackgrp.WTIME=np.int32(ctime)
                                                            
            stackgrp.TSTEP=np.int32(tdelta/3600*10000)
            stackgrp.NCOLS=np.int32(nx)
            stackgrp.NROWS=np.int32(ny)
            stackgrp.NLAYS=np.int32(nz)
            stackgrp.NVARS=np.int32(pnumspec)

            stackgrp.GDTYP=np.int32(GDTYP)
            stackgrp.VGTYP=np.int32(VGTYP)
            stackgrp.VGTOP=np.float32(VGTOP)
            stackgrp.VGLVLS =np.float32(VGLVLS)
            stackgrp.P_ALP=np.float32(p_alp)
            stackgrp.P_BET=np.float32(p_bet)
            stackgrp.P_GAM=np.float32(p_gam)
            stackgrp.XCENT=np.float32(XCENT)
            stackgrp.YCENT=np.float32(YCENT)
            stackgrp.YCENT=np.float64(YCENT)
            stackgrp.XORIG=np.float64(xorig)
            stackgrp.YORIG=np.float64(yorig)
            stackgrp.XCELL=np.float64(delx)
            stackgrp.YCELL=np.float64(dely)
            stackgrp.GDNAM = ep_cfg.domain.grid_name
            stackgrp.UPNAM = '???'
            stackgrp.HISTORY = '???'
            var_joined = ''.join(['{0:16s}'.format(var) for var in var_int + var_float ] )
            setattr(stackgrp,'VAR-LIST', var_joined)
            stackgrp.FILEDESC='stack params'




# write point emissione file
            pemisgrp.createDimension('TSTEP', None)
            pemisgrp.createDimension('DATE-TIME', 2)
            pemisgrp.createDimension('LAY', 1)
            pemisgrp.createDimension('VAR', pnumspec)
            pemisgrp.createDimension('ROW', numstk)
            pemisgrp.createDimension('COL', 1)

            tflag = pemisgrp.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
            tflag.units = '<YYYYDDD,HHMMSS>'
            tflag.long_name = 'FLAG            ' # length = 15
            tflag.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '
            for t in range(ntimeint):
                tflag[t,:,0] = bdate[t]
                tflag[t,:,1] = btime[t]

            emisvar = []
            for i in range(pnumspec):
                emission = pemisgrp.createVariable(pspecies[i], 'f4', ('TSTEP', 'LAY', 'ROW', 'COL'))
                emission.long_name=longemisname[i]
                emission.units='moles/s for gases and  g/s for aerosols'
                emission.var_desc='Model species '+longemisname[i]
                emisvar.append(emission)
#                for t in range(ntimeint):
#                    ep_debug('Writing emission at t={}, i={}'.format(t,i))
                    
            for t in range(ntimeint):
                ep_debug('Fetching point emission data from DB for: {dt}'.format(dt=datestimes[t]))
                q = 'SELECT ep_pemiss_time_series(%s,%s,%s::timestamp,%s::text, %s::text)'
                emiscur.execute(q, (stacks_id, pspec_ids, datestimes[t], source_schema, case_schema))
                pemis = np.array(emiscur.fetchone()[0])
                for i in range(pnumspec):
                    emisvar[i][t,0,:,0] = pemis[:,i]
                                                        
    # attributes
            pemisgrp.EXEC_ID='????????????????'
            pemisgrp.FTYPE=np.int32(1)
            pemisgrp.SDATE=np.int32(bdate[0])
            pemisgrp.STIME=np.int32(btime[0])
            pemisgrp.CDATE=np.int32(cdate)
            pemisgrp.CTIME=np.int32(ctime)
            pemisgrp.WDATE=np.int32(cdate)
            pemisgrp.WTIME=np.int32(ctime)            
            pemisgrp.TSTEP=np.int32(tdelta/3600*10000)
            pemisgrp.NTHIK = np.int32(1)
            pemisgrp.NCOLS=np.int32(nx)
            pemisgrp.NROWS=np.int32(ny)
            pemisgrp.NLAYS=np.int32(nz)
            pemisgrp.NVARS=np.int32(pnumspec)

            pemisgrp.GDTYP=np.int32(GDTYP)
            pemisgrp.VGTYP=np.int32(VGTYP)
            pemisgrp.VGTOP=np.float32(VGTOP)
            pemisgrp.VGLVLS =np.float32(VGLVLS)
            pemisgrp.P_ALP=np.float64(p_alp)
            pemisgrp.P_BET=np.float64(p_bet)
            pemisgrp.P_GAM=np.float64(p_gam)
            pemisgrp.XCENT=np.float64(XCENT)
            pemisgrp.YCENT=np.float64(YCENT)
            pemisgrp.XORIG=np.float64(xorig)
            pemisgrp.YORIG=np.float64(yorig)
            pemisgrp.XCELL=np.float64(delx)
            pemisgrp.YCELL=np.float64(dely)
            pemisgrp.GDNAM = ep_cfg.domain.grid_name
            pemisgrp.UPNAM = '???'
            pemisgrp.HISTORY = '???'
            setattr(pemisgrp,'VAR-LIST', ''.join(longemisname))
            pemisgrp.FILEDESC='point source emissions'






        except IOError:

            print('EE: Error while writing point source netcdf file for CMAQ. Check paths and permissions!')
            raise

        finally:
            stackgrp.close()
            pemisgrp.close()


    #######################################################################################
    #                                                                                     #
    #                                      RegCM                                          #
    #                                                                                     #
    #######################################################################################

    # RegCM area file
    if (model == 'RegCM' and iarea == 1):
            # some preparation
            # temporary values
        ep_debug('Writing RegCM area emissions')

        try:
            emisgrp = Dataset(areafile, 'w', format='NETCDF4')
            ep_debug('File {} opened'.format(areafile))
            emisgrp.createDimension('time', None)
            emisgrp.createDimension('lev', nz)
            emisgrp.createDimension('y', ny)
            emisgrp.createDimension('x', nx)

            time = emisgrp.createVariable('time', 'f8', ('time'))
            time.units = 'hours since {}-{}-{} {}:00:00'.format(datestimes[0].year, datestimes[0].month, datestimes[0].day, datestimes[0].hour)
            time.long_name = 'time' 
            time.calendar = 'standard'


            emisvar = []
            nx, ny, nz, maxspec, ntimeint
            emis = emis.transpose(4,2,1,0,3)
            for i in range(numspec):
                emission = emisgrp.createVariable(species[i]+'_flux', 'f4', ('time', 'lev', 'y', 'x'))
                emission.long_name='Flux of '+species[i]
                emission.units='Kg/m-2/s-1'
                emisvar.append(emission)
                for t in range(ntimeint):
                    emission[t,:,:,:] = emis[t,:,:,:,i]
            for t in range(ntimeint):
                tflag[t,:,0] = bdate[t]
                tflag[t,:,1] = btime[t]

        except IOError:

            print('EE: Error while writing area source netcdf file for RegCM. Check paths and permissions!')
            raise


    # attributes

            emisgrp.FTYPE=np.int32(1)
            emisgrp.SDATE=np.int32(bdate[0])
            emisgrp.STIME=np.int32(btime[0])
            emisgrp.TSTEP=np.int32(tdelta/3600*10000)
            emisgrp.NCOLS=np.int32(nx)
            emisgrp.NROWS=np.int32(ny)
            emisgrp.NLAYS=np.int32(nz)
            emisgrp.NVARS=np.int32(numspec)



