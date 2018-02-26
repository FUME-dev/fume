#!/usr/bin/env python3

__author__ = "Peter Huszar"
__license__ = "GPL"
__email__ = "huszarpet@gmail.com"


import os
import numpy as np
import pyproj
from netCDF4 import Dataset, num2date
from datetime import *
from subprocess import call

from lib.ep_config import ep_cfg

from lib.ep_libutil import ep_dates_times, ep_projection_params, ep_rtcfg, ep_connection, ep_create_schema, ep_debug, ep_dates_times
from cdo import *
from input.ep_read_sources import ep_read_raw_netcdf, ep_get_source_file_id, ep_register_source_file, ep_get_eset_id

_required_met = [ 'soim1', 'soit1', 'tas', 'ps', 'qas', 'wndspd10m', 'pr24', 'par']

def preproc(cfg):

    # alternative interpolation routine - done in python on the client side (not using Postgis)

    megan_input_dir = cfg.megan_input_dir
    megan_temp_dir = cfg.megan_temp_dir
    megan_out_dir = cfg.megan_out_dir
    file_lai = cfg.megan_lai
    file_pft = cfg.megan_pft
    files_ef = cfg.megan_ef
    file_sltyp = cfg.megan_sltyp

    cdt = datetime.now()

    cyyyy = cdt.year
    cmm   = cdt.month
    cdd   = cdt.day
    chh   = cdt.hour
    cmin  = cdt.minute
    css   = cdt.second
    yearstart_date = datetime(cyyyy, 1, 1)
    cjjj = (cdt - yearstart_date).days + 1
    cdate= cyyyy*1000+cjjj
    ctime= chh*10000 + cmin * 100 + css


    from lib.ep_geo_tools import grid_desc
    gdf = megan_temp_dir+'/cdo_griddesc_case'
    m3gdf = megan_temp_dir+'/GRIDDESC'
    case_lons,case_lats = grid_desc(ep_cfg,cdo_griddesc_file=gdf,m3_griddesc_file=m3gdf)

    try:
        pftgrp = Dataset(megan_input_dir+'/'+file_pft,'r+')
    except IOError:
        print('EE: Error opening megan input dataset. Check path {}!'.format(megan_input_dir))
        raise IOError


#   pft file has unconventional naming for lat, lon and for attributes. CDO is unable to determine the grid.
#   should be:
#   LON -> lon, LAT -> lat   
#   lat:units -> degrees_north
#   lat:long_name -> "coordinate latitude
#   same for lon

    try:
        pftgrp.renameVariable('LON','lon')
        pftgrp.renameVariable('LAT','lat')
    except KeyError:
        pass

    try:
        pftgrp.variables['lon'].units='degrees_east'
        pftgrp.variables['lat'].units='degrees_north'
        pftgrp.variables['lon'].long_name='coordinate longitude'
        pftgrp.variables['lat'].long_name='coordinate latitude'
    except: # FIXME
        pass

    pftgrp.close()

    EF_vars = [ 'apin22', 'bpin22','care22','isop22','limo22','mbo22','myrc22','nox22', 'ocim22', 'sabi22' ]

    # Now everything is prepared for interpolation with CDO
    method = 'remapnn'
    files = files_ef
    files.append(file_lai)
    files.append(file_pft) 
    files.append(file_sltyp)

    meganivars = EF_vars
    meganivars.append('lai')
    meganivars.append('PCT_PFT')
    meganivars.append('SLTYP')
    remapped = []
    
    ep_debug('II: remapping megan input to the case grid')
    cdo = Cdo()
    for f,v in zip(files, meganivars):
        fp = os.path.join(megan_input_dir,f)
        tmpremapped = cdo.remapbil(gdf,input = fp, output = None, returnArray = v)
        remapped.append(tmpremapped)
        
 
       
########### take the interpolated data and write to MEGAN IO/API files

       
    proj  = ep_rtcfg['projection_params']['proj']
    p_alp = ep_rtcfg['projection_params']['p_alp']
    p_bet = ep_rtcfg['projection_params']['p_bet']
    p_gam = ep_rtcfg['projection_params']['p_gam']
    XCENT = ep_rtcfg['projection_params']['lon_central']
    YCENT = ep_rtcfg['projection_params']['lat_central']

    if proj == "LATLON":
        iproj = 0
        GDTYP = 1
    elif proj == "UTM":
        iproj = 1
        GDTYP = 5
    elif proj == "LAMBERT":
        iproj = 2
        GDTYP = 2
    elif proj == "STEREO":
        iproj = 3
        GDTYP = 4
    elif proj == "POLAR":
        iproj = 4
        GDTYP = 6
    elif proj == "EMERCATOR":
        iproj = 5
        GDTYP = 7
    elif proj == "MERCATOR":
        iproj = 5
        GDTYP = 3
    else:
        print("EE: projection not known. Exit.....")
        raise ValueError

    # domain parameters
    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny
    nz = ep_cfg.domain.nz

    delx = ep_cfg.domain.delx
    dely = ep_cfg.domain.dely
    xorg = ep_cfg.domain.xorg
    yorg = ep_cfg.domain.yorg

    grid_name = ep_cfg.domain.grid_name

    xorg = xorg - nx*delx/2.
    yorg = yorg - ny*dely/2.

    # open output files (LAI, PFT, EFs)
    try:
        megan_lai = Dataset(megan_temp_dir+'/'+'megan_in_lai.nc','w',format='NETCDF3_CLASSIC')
        megan_pft = Dataset(megan_temp_dir+'/'+'megan_in_pft.nc','w',format='NETCDF3_CLASSIC')
        megan_EF =  Dataset(megan_temp_dir+'/'+'megan_in_EF.nc','w',format='NETCDF3_CLASSIC')
        megan_SLTYP = Dataset(megan_temp_dir+'/'+'megan_in_SLTYP.nc','w',format='NETCDF3_CLASSIC')
    except IOError:
        print('EE: Error while opening megan input files. Check paths: {}!'.format(megan_temp_dir))
        raise

    # creating dimensions

    megan_lai.createDimension('TSTEP', 46)
    megan_lai.createDimension('DATE-TIME', 2)
    megan_lai.createDimension('LAY', 1)
    megan_lai.createDimension('VAR', 1)
    megan_lai.createDimension('ROW', ny)
    megan_lai.createDimension('COL', nx)

    megan_EF.createDimension('TSTEP', 1)
    megan_EF.createDimension('DATE-TIME', 2)
    megan_EF.createDimension('LAY', 1)
    megan_EF.createDimension('VAR', 24)
    megan_EF.createDimension('ROW', ny)
    megan_EF.createDimension('COL', nx)

    megan_pft.createDimension('TSTEP', 16)
    megan_pft.createDimension('DATE-TIME', 2)
    megan_pft.createDimension('LAY', 1)
    megan_pft.createDimension('VAR', 1)
    megan_pft.createDimension('ROW', ny)
    megan_pft.createDimension('COL', nx)

    megan_SLTYP.createDimension('ROW', ny)
    megan_SLTYP.createDimension('COL', nx)



    # time specific data
    tflag_lai = megan_lai.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
    tflag_lai.units = '<YYYYDDD,HHMMSS>'
    tflag_lai.long_name = 'FLAG            '  # length = 15
    tflag_lai.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '

    tflag_pft = megan_pft.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
    tflag_pft.units = '<YYYYDDD,HHMMSS>'
    tflag_pft.long_name = 'FLAG            '  # length = 15
    tflag_pft.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '

    tflag_EF = megan_EF.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
    tflag_EF.units = '<YYYYDDD,HHMMSS>'
    tflag_EF.long_name = 'FLAG            '  # length = 15
    tflag_EF.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '


    datestimes = ep_dates_times()
    ntimeint = len(datestimes)
    tzone = datestimes[0].tzinfo
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

        yearstart_date = datetime(byyyy, 1, 1, tzinfo = tzone)
        bjjj = (actual_date - yearstart_date).days + 1
        bdate.append(byyyy*1000+bjjj)
        btime.append(bhh*10000 + bmin * 100 + bss)

    for t in range(24):
        tflag_lai[t, :, 0] = int(0) # byyyy*1000+1+8*t  # bdate[t]
        tflag_lai[t, :, 1] = int(t*10000)
    for t in range(24, 46):
        tflag_lai[t, :, 0] = int(1) # np.int32(1)  # bdate[t]
        tflag_lai[t, :, 1] = (t-24)*10000



    tflag_pft[:,0,0] = np.int32(0)
    tflag_pft[:,0,1] = np.arange(0,16)*10000 #btime[0]

    tflag_EF[0,:,0] = np.int32(0) #bdate[0]
    tflag_EF[0,:,1] = np.int32(0) #btime[0]

    # creating and feeding fields
    fields_lai = megan_lai.createVariable('LAIS', 'f4', ('TSTEP', 'LAY', 'ROW', 'COL'))
    fields_lai.long_name = "LAIS            "
    fields_lai.units = "nondimension    "
    fields_lai.var_desc = ""

    fields_pft = megan_pft.createVariable('PFTS', 'f4', ('TSTEP', 'LAY', 'ROW', 'COL'))
    fields_pft.long_name = "PFTS            "
    fields_pft.units = "nondimension    "
    fields_pft.var_desc = ""


    fields_SLTYP = megan_SLTYP.createVariable('SLTYP','f4', ('ROW','COL'))

    EF_fields = ['LAT','LONG','DTEMP','DSRAD','EF_ISOP','EF_MYRC','EF_SABI','EF_LIMO','EF_A_3CAR','EF_OCIM','EF_BPIN','EF_APIN','EF_OMTP','EF_FARN','EF_BCAR','EF_OSQT','EF_MBO','EF_MEOH','EF_ACTO','EF_CO','EF_NO','EF_BIDER','EF_STRESS','EF_OTHER']


    EF_fields_o = np.empty((len(EF_fields)), dtype=object)
    for i in range(len(EF_fields)):
        EF_fields_o[i] = megan_EF.createVariable(EF_fields[i], 'f4', ('TSTEP', 'LAY', 'ROW', 'COL'))
        EF_fields_o[i].long_name = EF_fields[i].ljust(16)
        EF_fields_o[i].units = ""
        EF_fields_o[i].var_desc = ""



    EF_fields_o[0].units = "DEGREE          " 
    EF_fields_o[1].units = "DEGREE          "

    # first lai

    fields_lai[:,0,:,:] = remapped[-3][:,:,:]

    # feed EF file with values
    # LAT
    EF_fields_o[0][0, 0, :, :] = case_lats.transpose(1, 0)
    # LONG
    EF_fields_o[1][0, 0, :, :] = case_lons.transpose(1, 0)

    EF_fields_o[EF_fields.index('EF_ISOP')][0, 0, :, :] = remapped[EF_vars.index('isop22')]
    EF_fields_o[EF_fields.index('EF_MYRC')][0, 0, :, :] = remapped[EF_vars.index('myrc22')]
    EF_fields_o[EF_fields.index('EF_SABI')][0, 0, :, :] = remapped[EF_vars.index('sabi22')]
    EF_fields_o[EF_fields.index('EF_LIMO')][0, 0, :, :] = remapped[EF_vars.index('limo22')]
    EF_fields_o[EF_fields.index('EF_A_3CAR')][0, 0, :, :] = remapped[EF_vars.index('care22')]
    EF_fields_o[EF_fields.index('EF_OCIM')][0, 0, :, :] = remapped[EF_vars.index('ocim22')]
    EF_fields_o[EF_fields.index('EF_BPIN')][0, 0, :, :] = remapped[EF_vars.index('bpin22')]
    EF_fields_o[EF_fields.index('EF_APIN')][0, 0, :, :] = remapped[EF_vars.index('apin22')]
    EF_fields_o[EF_fields.index('EF_NO')][0, 0, :, :] = remapped[EF_vars.index('nox22')]
    EF_fields_o[EF_fields.index('EF_MBO')][0, 0, :, :] = remapped[EF_vars.index('mbo22')]

    for f in ['EF_OMTP', 'EF_FARN', 'EF_BCAR', 'EF_OSQT', 'EF_MEOH', 'EF_ACTO', 'EF_CO', 'EF_BIDER', 'EF_STRESS', 'EF_OTHER']:
        EF_fields_o[EF_fields.index(f)][0, 0, :, :] = np.full((ny,nx), 1.0, dtype=float)


    # feed PFT file with values
    
    fields_pft[0, 0, :, :] = remapped[-2][1, :, :]  # NT_EG_TEMP
    fields_pft[1, 0, :, :] = remapped[-2][3, :, :]  # NT_DC_BORL
    fields_pft[2, 0, :, :] = remapped[-2][2, :, :]  # NT_EG_BORL
    fields_pft[3, 0, :, :] = remapped[-2][4, :, :]  # BT_EG_TROP
    fields_pft[4, 0, :, :] = remapped[-2][5, :, :]  # BT_EG_TEMP
    fields_pft[5, 0, :, :] = remapped[-2][6, :, :]  # BT_DC_TROP
    fields_pft[6, 0, :, :] = remapped[-2][7, :, :]  # BT_DC_TEMP
    fields_pft[7, 0, :, :] = remapped[-2][8, :, :]  # BT_DC_BORL
    fields_pft[8, 0, :, :] = remapped[-2][9, :, :]  # SB_EG_TEMP
    fields_pft[9, 0, :, :] = remapped[-2][10,:, :]  # SB_DC_TEMP
    fields_pft[10, 0, :, :] = remapped[-2][11, :, :]  # SB_DC_BORL
    fields_pft[11, 0, :, :] = remapped[-2][12, :, :]  # GS_C3_COLD
    fields_pft[12, 0, :, :] = remapped[-2][13, :, :]  # GS_C3_COOL
    fields_pft[13, 0, :, :] = remapped[-2][14, :, :]  # GS_C3_WARM
    fields_pft[14, 0, :, :] = remapped[-2][15, :, :]  # CROP
    fields_pft[15, 0, :, :] = remapped[-2][16, :, :]  # CORN


    fields_SLTYP[:] = remapped[-1].transpose(1,0)
    megan_SLTYP.close()
    # write global attributes
    # first the attributes common to each megan input file

    for grp in megan_lai, megan_pft, megan_EF:
        grp.IOAPI_VERSION = ""
        grp.EXEC_ID = "????????????????                                                                "
        grp.FTYPE=np.int32(1)
        grp.SDATE=np.int32(bdate[0])
        grp.STIME=np.int32(btime[0])


        dt  =  ep_cfg.run_params.time_params.timestep
        dt_hh = dt // 3600
        dt_tmp = dt % 3600
        dt_min = dt_tmp // 60
        dt_ss  = dt_tmp % 60
        tstep  = str(dt_hh*10000+dt_min * 100 + dt_ss)
        grp.TSTEP=np.int32(tstep)

        grp.CDATE = np.int32(cdate)
        grp.CTIME = np.int32(ctime)
        grp.WDATE = np.int32(cdate)
        grp.WTIME = np.int32(ctime)

        grp.NCOLS = np.int32(nx)
        grp.NROWS = np.int32(ny)
        grp.NLAYS = np.int32(1)
      
        grp.GDTYP = np.int32(GDTYP)
        grp.VGTYP = np.int32(0)
        grp.VGTOP = np.float32(5.)
        grp.VGLVLS = np.float32([0.,0.])
        grp.P_ALP = np.float32(p_alp)
        grp.P_BET = np.float32(p_bet)
        grp.P_GAM = np.float32(p_gam)
        grp.XCENT = np.float32(XCENT)
        grp.YCENT = np.float32(YCENT)
        grp.XORIG = np.float32(xorg-nx*delx/2.0)
        grp.YORIG = np.float32(yorg-ny*dely/2.0)
        grp.XCELL = np.float32(delx)
        grp.YCELL = np.float32(dely)
        grp.GDNAM = '{0:16s}'.format(grid_name)
        grp.NTHIK = np.int32(1)
        grp.HISTORY = ""
        grp.FILEDESC = "Created by EP                                                                                                                                                   "

###############
    megan_EF.TSTEP = np.int32(0)
    megan_EF.STIME = np.int32(1)

    megan_pft.TSTEP = np.int32(10000)
    megan_pft.STIME = np.int32(0)
    megan_pft.SDATE = np.int32(-635)

    megan_lai.STIME = np.int32(0)
    megan_lai.SDATE = np.int32(-635)
###############

    megan_lai.NVARS = np.int32(1)
    megan_pft.NVARS = np.int32(1)
    megan_EF.NVARS = np.int32(24)

    megan_lai.UPNAM = "CNVT_LAI        "
    megan_pft.UPNAM = "CNVT_PFT        "
    megan_EF.UPNAM  = "CNVT_EFS        " 

    setattr(megan_lai,'VAR-LIST', "LAIS            ")
    setattr(megan_pft,'VAR-LIST', "PFTS            ")

    longfldname = [ '{0:16s}'.format(f) for f in EF_fields ]

    setattr(megan_EF,'VAR-LIST', ''.join(longfldname))

    


#############################################################################
# write MEGAN met input header - this does not write the data itself
#############################################################################
def met_write_megan_met(cfg):

    megan_temp_dir = cfg.megan_temp_dir

    megan_met_file = os.path.join(megan_temp_dir,cfg.megan_met)
    try:
        megangrp = Dataset(megan_met_file,'w',format='NETCDF3_CLASSIC')
        ep_rtcfg['megangrp'] = megangrp
    except IOError:
        print('EE: Error while opening {}. Check paths!'.format(megan_met_file))
        raise

    proj  = ep_rtcfg['projection_params']['proj']
    p_alp = ep_rtcfg['projection_params']['p_alp']
    p_bet = ep_rtcfg['projection_params']['p_bet']
    p_gam = ep_rtcfg['projection_params']['p_gam']
    XCENT = ep_rtcfg['projection_params']['lon_central']
    YCENT = ep_rtcfg['projection_params']['lat_central']

    if proj == "LATLON":
        iproj = 0
        GDTYP = 1
    elif proj == "UTM":
        iproj = 1
        GDTYP = 5
    elif proj == "LAMBERT":
        iproj = 2
        GDTYP = 2
    elif proj == "STEREO":
        iproj = 3
        GDTYP = 4
    elif proj == "POLAR":
        iproj = 4
        GDTYP = 6
    elif proj == "EMERCATOR":
        iproj = 5
        GDTYP = 7
    elif proj == "MERCATOR":
        iproj = 5
        GDTYP = 3
    else:
        print("EE: projection not known. Exit.....")
        raise ValueError

    # domain parameters
    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny
    nz = ep_cfg.domain.nz
                                  
    delx = ep_cfg.domain.delx
    dely = ep_cfg.domain.dely
    xorg = ep_cfg.domain.xorg
    yorg = ep_cfg.domain.yorg
    grid_name = ep_cfg.domain.grid_name

    # timeparam

    itzone_out = ep_cfg.run_params.time_params.itzone_out
    tdelta = ep_cfg.run_params.time_params.timestep

    VGTYP  = 1   #ep_cfg.run_params.output_params.CMAQ_params.VGTYP
    VGTOP  = 0.0 #ep_cfg.run_params.output_params.CMAQ_params.VGTOP
    VGLVLS = 0.0,0.0


    bdate = []
    btime = []
    datestimes = ep_dates_times()
    ntimeint = len(datestimes)
    tzone = datestimes[0].tzinfo
    for i in range(ntimeint):
        actual_date = datestimes[i]
        byyyy = actual_date.year
        bmm   = actual_date.month
        bdd   = actual_date.day
        bhh   = actual_date.hour
        bmin  = actual_date.minute
        bss   = actual_date.second

        yearstart_date = datetime(byyyy, 1, 1,tzinfo = tzone)
        bjjj = (actual_date - yearstart_date).days + 1
        bdate.append(byyyy*1000+bjjj)
        btime.append(bhh*10000 + bmin * 100 + bss)

    cdt = datetime.now()

    cyyyy = cdt.year
    cmm   = cdt.month
    cdd   = cdt.day
    chh   = cdt.hour
    cmin  = cdt.minute
    css   = cdt.second
    yearstart_date = datetime(cyyyy, 1, 1)
    cjjj = (cdt - yearstart_date).days + 1
    cdate= cyyyy*1000+cjjj
    ctime= chh*10000 + cmin * 100 + css


    megan_fields = ['SOIM1','SOIT1','SLTYP','TEMP2','PRES','QV','WINDSPD','RAIN_ACC24','PREC_ADJ','PAR']
    megan_mapping = {
    'soim1': 'SOIM1',
    'soit1': 'SOIT1',
    'tas': 'TEMP2',
    'ps': 'PRES',
    'qas': 'QV',
    'wndspd10m' :  'WINDSPD',
    'pr24': 'RAIN_ACC24',  # FIXME
    'par' : 'PAR'
    }
    numfld = len(megan_fields)
    # field units and description
    megan_units = []
    megan_desc  = []

    megan_units.append('M**3/M**3       ')
    megan_desc.append('volumetric soil moisture in top cm                                              ')

    megan_units.append('K               ')
    megan_desc.append('soil temperature in top cm                                                      ')

    megan_units.append('CATEGORY        ')
    megan_desc.append('soil texture type by USDA category                                              ')

    megan_units.append('K               ')
    megan_desc.append('temperature at 2 m                                                              ')

    megan_units.append('Pa              ')
    megan_desc.append('pressure                                                                        ')

    megan_units.append('KG/KG           ')
    megan_desc.append('water vapor mixing ratio                                                        ')

    megan_units.append('m/s             ')
    megan_desc.append('Cell centered Windspeed                                                         ')

    megan_units.append('cm              ')
    megan_desc.append('24-hour accumulated rain                                                        ')

    megan_units.append('No dimension    ')
    megan_desc.append('Precip adjustment factor                                                        ')

    megan_units.append('WATTS/M**2      ')
    megan_desc.append('Photosynthetically Active Radiation                                             ')

    longfldname = [ '{0:16s}'.format(megan_fields[f]) for f in range(numfld) ]



    megangrp.createDimension('TSTEP', None)
    megangrp.createDimension('DATE-TIME', 2)
    megangrp.createDimension('LAY', 1)
    megangrp.createDimension('VAR', numfld)
    megangrp.createDimension('ROW', ny)
    megangrp.createDimension('COL', nx)

    tflag = megangrp.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
    tflag.units = '<YYYYDDD,HHMMSS>'
    tflag.long_name = 'FLAG            ' # length = 15
    tflag.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '


    for t in range(ntimeint):
        tflag[t,:,0] = bdate[t]
        tflag[t,:,1] = btime[t]


    fields = list(megan_mapping.keys())

    megan_met_var = []
    for f in range(numfld):
        field = megangrp.createVariable(megan_fields[f], 'f4', ('TSTEP', 'LAY', 'ROW', 'COL'))
        field.long_name = longfldname[f]
        field.units = megan_units[f]
        field.var_desc = megan_desc[f]
        megan_met_var.append(field)


        
    for t in range(len(datestimes)):
        for d in ep_rtcfg['met'][t]:
            if d.name in _required_met: # we need just a subset of all the meteorology
                iv = megan_fields.index(megan_mapping[d.name])
                if d.name != 'pr24':
                    megan_met_var[iv][t, 0, :, :] = d.data.transpose(1,0)
                else:
                    megan_met_var[iv][t, 0, :, :] = d.data.transpose(1,0)/10. #.reshape((ny,nx))/10.0 # units are cm not mm
        

    # fill in precip adjustment factor, for now, value 1

    iv = megan_fields.index('PREC_ADJ')
    megan_met_var[iv][:] = 1.


    megan_SLTYP = Dataset(megan_temp_dir+'/'+'megan_in_SLTYP.nc','r')

    iv = megan_fields.index('SLTYP')
    for t in range(len(datestimes)):
        megan_met_var[iv][t,0,:,:] = megan_SLTYP.variables['SLTYP'][:]
    
    megan_SLTYP.close()
    # attributes

    megangrp.FTYPE=np.int32(1)
    megangrp.SDATE=np.int32(bdate[0])
    megangrp.STIME=np.int32(btime[0])
    megangrp.CDATE=np.int32(cdate)
    megangrp.CTIME=np.int32(ctime)
    megangrp.WDATE=np.int32(cdate)
    megangrp.WTIME=np.int32(ctime)

    dt  =  ep_cfg.run_params.time_params.timestep
    dt_hh = dt // 3600
    dt_tmp = dt % 3600
    dt_min = dt_tmp // 60
    dt_ss  = dt_tmp % 60
    tstep  = str(dt_hh*10000+dt_min * 100 + dt_ss)

    megangrp.TSTEP = np.int32(tstep)
    megangrp.NCOLS = np.int32(nx)
    megangrp.NROWS = np.int32(ny)
    megangrp.NLAYS = np.int32(1)
    megangrp.NVARS = np.int32(numfld)
    megangrp.GDNAM = '{0:16s}'.format(grid_name)

    megangrp.GDTYP = np.int32(GDTYP)
    megangrp.VGTYP = np.int32(VGTYP)
    megangrp.VGTOP = np.float32(VGTOP)
    megangrp.VGLVLS = np.float32(VGLVLS)
    megangrp.P_ALP = np.float32(p_alp)
    megangrp.P_BET = np.float32(p_bet)
    megangrp.P_GAM = np.float32(p_gam)
    megangrp.XCENT = np.float32(XCENT)
    megangrp.YCENT = np.float32(YCENT)
    megangrp.XORIG = np.float32(xorg-nx*delx/2.0)
    megangrp.YORIG = np.float32(yorg-ny*dely/2.0)
    megangrp.XCELL = np.float64(delx)
    megangrp.YCELL = np.float64(dely)
    megangrp.NTHIK = np.int32(1)
    megangrp.EXEC_ID = ""
    megangrp.UPNAM = ""
    megangrp.FILEDESC = ""
    megangrp.UPDSC = ""
    setattr(megangrp, 'VAR-LIST', ''.join(longfldname))
    megangrp.HISTORY = ""


def megan_run(cfg):
    
    # set enviromental variables
    mgnhome = cfg.megan_base_dir
    megan_temp_dir = cfg.megan_temp_dir
    megan_out_dir  = cfg.megan_out_dir 

    os.environ["MGNHOME"] = mgnhome
    mgnsrc = os.path.join(mgnhome, 'src')
    mgnlib = os.path.join(mgnhome, 'lib')
    mgnexe = os.path.join(mgnhome, 'bin')
    mgnrun = os.path.join(mgnhome, 'work')
    mgninp = os.path.join(mgnhome, 'Input')
    mgnout = os.path.join(mgnhome, 'Output')
    mgnint = os.path.join(mgnhome, 'Output/INT')
    mgnlog = os.path.join(mgnhome, 'work/logdir')

    os.environ["MGNSRC"] = mgnsrc
    os.environ["MGNLIB"] = mgnlib
    os.environ["MGNEXE"] = mgnexe
    os.environ["MGNRUN"] = mgnrun
    os.environ["MGNINP"] = mgninp
    os.environ["MGNOUT"] = mgnout
    os.environ["MGNINT"] = mgnint
    os.environ["MGNLOG"] = mgnlog



    if not os.path.isdir(mgninp):
        os.mkdir(os.path.join(mgninp, 'MAP'))
        os.mkdir(os.path.join(mgninp, 'MGNMET'))
        os.mkdir(os.path.join(mgninp, 'PAR'))

    if not os.path.isdir(mgnint):
        os.mkdir(mgnint)
    if not os.path.isdir(mgnlog):
        os.mkdir(mgnlog)

    # emisproc
    # set emisproc evnironmental variables
    os.environ["PRJ"]  = ep_cfg.casename
    os.environ["PROMPTFLAG"]  = 'Y'
    exe = os.path.join(mgnexe, 'emproc')
    os.environ["INPDIR"]  = os.path.join(mgninp,'MAP')
    os.environ["METDIR"]  = os.path.join(mgninp,'MGNMET')
    os.environ["INTDIR"]  = mgnint
    os.environ["OUTDIR"]  = mgnout

    datestimes = ep_dates_times()
    byyyy = datestimes[0].year
    bmm   = datestimes[0].month
    bdd   = datestimes[0].day
    
    yearstart_date = datetime(byyyy, 1, 1, tzinfo = datestimes[0].tzinfo)
    bjjj = (datestimes[0] - yearstart_date).days + 1
    

    sdate = str(byyyy*1000 + bjjj)

    os.environ["SDATE"]  =  sdate
    bhh = datestimes[0].hour
    bmin = datestimes[0].minute
    bss = datestimes[0].second    
    stime = str(bhh*10000 + bmin * 100 + bss)
    os.environ["STIME"] = stime


    ntimeint = len(datestimes)
    dtl = ntimeint * ep_cfg.run_params.time_params.timestep
    dtl_hh = dtl // 3600
    dtl_tmp = dtl % 3600
    dtl_min = dtl_tmp // 60
    dtl_ss  = dtl_tmp % 60
    rleng  = str(dtl_hh*10000+dtl_min * 100 + dtl_ss)


    dt = ep_cfg.run_params.time_params.timestep
    dt_hh = dt // 3600
    dt_tmp = dt % 3600
    dt_min = dt_tmp // 60
    dt_ss  = dt_tmp % 60
    tstep  = str(dt_hh*10000+dt_min * 100 + dt_ss)   
    os.environ["RLENG"] = rleng 
    os.environ["TSTEP"] = tstep


    os.environ["RUN_MEGAN"] = 'Y'
    os.environ["ONLN_DT"] = 'Y'
    os.environ["ONLN_DS"] = 'Y'
    os.environ["GRIDDESC"] = os.path.join(megan_temp_dir,'GRIDDESC')
    os.environ["GDNAM3D"] = ep_cfg.domain.grid_name
    os.environ["PROMPTFLAG"] = "Y"

    os.environ["EFMAPS"] = os.path.join(megan_temp_dir,'megan_in_EF.nc')
    os.environ["PFTS16"] = os.path.join(megan_temp_dir,'megan_in_pft.nc')
    os.environ["LAIS46"] = os.path.join(megan_temp_dir,'megan_in_lai.nc')

    os.environ["MGNMET"] = os.path.join(megan_temp_dir,cfg.megan_met)


    os.environ["MGNERS"] = os.path.join(megan_temp_dir,'megan_er.nc')
    megan_out_file =  os.path.join(megan_out_dir,cfg.megan_out)

    os.environ["MGNOUT"] = megan_out_file
    ep_debug('Executing MEGAN emproc')    
    call(['time',exe])


    # Run speciation and mechanism conversion
       
    os.environ["MECHANISM"] = cfg.megan_chem_mech

    os.environ["RUN_CONVERSION"] = "Y"
    os.environ["SPCTONHR"] = "N"

    exe = os.path.join(mgnexe, 'mgn2mech')
    ep_debug('Executing MEGAN mgn2mech')

    call(['time', exe])


    if cfg.merge_emis: # save emission into rt_cfg
        if 'external_model_data' not in ep_rtcfg.keys():
            ep_rtcfg['external_model_data'] = {}

        m = Dataset(megan_out_file,'r')
        megan_species = getattr(m,'VAR-LIST').split()
        megan_species.remove('GDAY')
        numspec_megan = len(megan_species)
        nx = ep_cfg.domain.nx
        ny = ep_cfg.domain.ny
        emis_megan = np.zeros((nx,ny,1,ntimeint, numspec_megan),dtype=float)
        for i in range(numspec_megan):
            emis_megan[:,:,0,:,i] = m.variables[megan_species[i]][:,0,:,:].transpose(2,1,0)

        ep_rtcfg['external_model_data']['megan'] = { 'data' : emis_megan, 'species' : megan_species }

def ep_megan_data_import(cfg, ext_mod_id):

    case_schema = ep_cfg.db_connection.case_schema

    meg_table = 'ep_raw_megan_emissions'

    meg_file_path = os.path.join(cfg.megan_temp_dir, cfg.megan_out)

    ep_read_raw_netcdf(ep_connection, meg_file_path, case_schema, meg_table,
                       ['TSTEP', 'LAY', 'ROW', 'COL'],
                       [('TSTEP', 'TFLAG', 'DATE-TIME')])

    grid_name = ep_cfg.domain.grid_name
    megan_cat = cfg.megan_category

    with ep_connection.cursor() as cur:
        # TODO should we do this? should we do it here?
        cur.execute('DELETE FROM "{case_schema}".ep_sg_out_emissions '
                    'WHERE sg_id IN (SELECT sg_id FROM "{case_schema}".ep_sources_grid WHERE ext_mod_id=%s)'
                    .format(case_schema=case_schema), (ext_mod_id, ))
        cur.execute('DELETE FROM "{case_schema}".ep_sources_grid WHERE ext_mod_id=%s'
                    .format(case_schema=case_schema), (ext_mod_id, ))
        ep_connection.commit()

        # import MEGAN data to ep_sources_grid
        cur.execute('CREATE TEMP TABLE sid_map ON COMMIT DROP AS WITH s AS'
                    '(INSERT INTO "{case_schema}".ep_sources_grid (source_type, ext_mod_id, grid_id, k, sg_factor)'
                    'SELECT DISTINCT \'A\', %s, grid_id, 1, 1 '
                    '   FROM "{case_schema}"."{megan}" '
                    '   JOIN "{case_schema}".ep_grid_tz ON ("dim_ROW" = j AND "dim_COL" = i) RETURNING sg_id, grid_id)'
                    'SELECT sg_id, grid_id FROM s'
                    .format(case_schema=case_schema, megan=meg_table), (ext_mod_id, ))

        # import MEGAN emissions to ep_sg_out_emissions
        # first get names of columns with known emission species
        cur.execute('SELECT name FROM "{schema}".ep_out_species '
                    'JOIN ('
                    '   SELECT column_name AS spec_name '
                    '   FROM information_schema.columns '
                    '   WHERE table_name = %s) AS m '
                    'ON m.spec_name = name'
                    .format(schema=case_schema), (meg_table, ))
        spec_names = cur.fetchall()
        cols_ap = ', '.join(["'" + c[0] + "'" for c in spec_names])
        cols_qu = ', '.join(['"' + c[0] + '"' for c in spec_names])

        # prepare time string
        time_start = ep_cfg.run_params.time_params.dt_init
        timestep = ep_cfg.run_params.time_params.timestep
        tstring = "timestamp '" + str(time_start) + "'" + ' + ("dim_TSTEP" - 1) * interval ' + "'" + str(timestep) + "'"

        print(cur.mogrify('INSERT INTO "{schema}".ep_sg_out_emissions (sg_id, spec_id, cat_id, time_out, emiss) '
                    'SELECT sg_id, spec_id, %s, {time_string}, emiss '
                    '  FROM ('
                    '    SELECT unnest(array[{cols_ap}]) AS spec_name, unnest(ARRAY[{cols_qu}]) AS emiss, "dim_TSTEP", grid_id '
                    '      FROM "{schema}".{megan} '
                    '      JOIN "{schema}".ep_grid_tz ON ("dim_ROW" = j AND "dim_COL" = i)'
                    '    ) AS m '
                    '    JOIN "{schema}".ep_out_species ON name = m.spec_name '
                    '    JOIN sid_map USING (grid_id)'
                    '    WHERE emiss > 0'
                    .format(schema=case_schema,
                            time_string=tstring,
                            cols_ap=cols_ap,
                            cols_qu=cols_qu,
                            megan=meg_table),
                    (megan_cat, )))
        cur.execute('INSERT INTO "{schema}".ep_sg_out_emissions (sg_id, spec_id, cat_id, time_out, emiss) '
                    'SELECT sg_id, spec_id, %s, {time_string}, emiss '
                    '  FROM ('
                    '    SELECT unnest(array[{cols_ap}]) AS spec_name, unnest(ARRAY[{cols_qu}]) AS emiss, "dim_TSTEP", grid_id '
                    '      FROM "{schema}".{megan} '
                    '      JOIN "{schema}".ep_grid_tz ON ("dim_ROW" = j AND "dim_COL" = i)'
                    '    ) AS m '
                    '    JOIN "{schema}".ep_out_species ON name = m.spec_name '
                    '    JOIN sid_map USING (grid_id)'
                    '    WHERE emiss > 0'
                    .format(schema=case_schema,
                            time_string=tstring,
                            cols_ap=cols_ap,
                            cols_qu=cols_qu,
                            megan=meg_table),
                    (megan_cat, ))

        ep_connection.commit()


def run(cfg, ext_mod_id):
    met_write_megan_met(cfg)
    megan_run(cfg)
    if ep_cfg.run_params.output_params.save_time_series_to_db:
        ep_megan_data_import(cfg, ext_mod_id)


