"""
Description: CMAQ output postprocessor

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

from datetime import datetime
import numpy as np
from netCDF4 import Dataset
from postproc.receiver import DataReceiver
from postproc.netcdf import NetCDFAreaTimeDisaggregator
import os


# CMAQ-specific projection IDs
gdtyp_mapping = {
        'LATLON': 1,
        'UTM': 5,
        'LAMBERT': 2,
        'STEREO': 4,
        'POLAR': 6,
        'EMERCATOR': 7,
        'MERCATOR': 3
}


def cmaq_date_time(dt):
    return (dt.year*1000 + dt.timetuple().tm_yday,
            dt.hour*10000 + dt.minute*100 + dt.second)


def long_object_name(s):
    return '{0:16s}'.format(s)


class CMAQAreaTimeWriter(NetCDFAreaTimeDisaggregator):
    """
    Postprocessor writing time disaggregated area emissions.
    *Time-optimized version: time disaggregation performed with NetCDF
    files. Requires a prior run of NetCDFTotalAreaWriter!*
    """

    def setup(self):
        """
        Run NetCDF setup with CMAQ-specific overrides:
        Dimensions names are x=COL, Y=ROW, Z=LAY, T=TSTEP
        Do not let the parent class create a time variable,
        projection attributes and close the file during finalize.
        """
        super().setup(filename=self.cfg.postproc.cmaqareawriter.outfile,
                      no_create_t_var=True, no_create_projection_attrs=True,
                      create_v_dim=True, v_dim='VAR',
                      x_dim='COL', y_dim='ROW', z_dim='LAY', t_dim='TSTEP',
                      no_close_outfile=True)

        self.outfile.createDimension('DATE-TIME', 2)

    def finalize(self):
        """
        Finalization steps
        ------------------

         - create the CMAQ-specific time variable TFLAG
         - run parent finalize (save output data and generic NetCDF attributes)
         - save time data
         - change the long_name attribute of output variables to CMAQ format
         - save CMAQ-specific attributes
         - close the file

        """
        self.timevar = self.outfile.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
        self.timevar.units = '<YYYYDDD,HHMMSS>'
        self.timevar.long_name = 'FLAG           '
        self.timevar.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '

        super().finalize()

        for timestep, datetime in enumerate(self.rt_cfg['run']['datestimes']):
            date, time = cmaq_date_time(datetime)
            self.timevar[timestep,:,0] = date
            self.timevar[timestep,:,1] = time

        for outvar in self.outvars:
            outvar.long_name = long_object_name(outvar.long_name)

        setattr(self.outfile, 'VAR-LIST', ''.join([long_object_name(spec[1])
                                                   for spec in self.species]))
        self.outfile.FILEDESC='CMAQ area emissions created by FUME ' + self.cfg.run_params.output_params.output_description
        self.outfile.FTYPE = np.int32(1)
        self.outfile.EXEC_ID = '????????????????'
        self.outfile.NTHIK = np.int32(1)
        self.outfile.NCOLS = np.int32(self.cfg.domain.nx)
        self.outfile.NROWS = np.int32(self.cfg.domain.ny)
        self.outfile.NLAYS = np.int32(self.cfg.domain.nz)
        self.outfile.NVARS = np.int32(len(self.species))
        start_date, start_time = cmaq_date_time(self.rt_cfg['run']['datestimes'][0])
        cur_date, cur_time = cmaq_date_time(datetime.now())
        self.outfile.SDATE = np.int32(start_date)
        self.outfile.STIME = np.int32(start_time)
        self.outfile.CDATE = np.int32(cur_date)
        self.outfile.CTIME = np.int32(cur_time)
        self.outfile.WDATE = np.int32(cur_date)
        self.outfile.WTIME = np.int32(cur_time)
        self.outfile.TSTEP = np.int32(self.cfg.run_params.time_params.timestep/3600*10000)
        self.outfile.GDTYP = np.int32(gdtyp_mapping[self.rt_cfg['projection_params']['proj']])
        self.outfile.VGTYP = np.int32(self.cfg.postproc.cmaqareawriter.vgtyp)
        self.outfile.VGTOP = np.float32(self.cfg.postproc.cmaqareawriter.vgtop)
        self.outfile.VGLVLS = np.float32(self.cfg.postproc.cmaqareawriter.vglvls)
        self.outfile.P_ALP = np.float64(self.rt_cfg['projection_params']['p_alp'])
        self.outfile.P_BET = np.float64(self.rt_cfg['projection_params']['p_bet'])
        self.outfile.P_GAM = np.float64(self.rt_cfg['projection_params']['p_gam'])
        self.outfile.XCENT = np.float64(self.rt_cfg['projection_params']['lon_central'])
        self.outfile.YCENT = np.float64(self.rt_cfg['projection_params']['lat_central'])
        xorig = self.cfg.domain.xorg - self.cfg.domain.nx*self.cfg.domain.delx/2.0
        yorig = self.cfg.domain.yorg - self.cfg.domain.ny*self.cfg.domain.dely/2.0
        self.outfile.XORIG = np.float64(xorig)
        self.outfile.YORIG = np.float64(yorig)
        self.outfile.XCELL = np.float64(self.cfg.domain.delx)
        self.outfile.YCELL = np.float64(self.cfg.domain.dely)
        self.outfile.GDNAM = self.cfg.domain.grid_name
        self.outfile.UPNAM = '???'
        self.outfile.HISTORY = '???'

        self.outfile.close()


class CMAQWriter(DataReceiver):
    """
    Postprocessor class for writing CMAQ emission input files.
    *Non time-optimized version*
    This is the base class for the common CMAQ format settings and should not be
    instantiated by itself. Descendant classes CMAQAreaWriter or CMAQPointWriter
    should be used instead.

    Implements only common setup (create output netcdf file, time variable) and
    finalize (set common global attributes of the netcdf file and close the
    file).
    """

    def setup(self, filename):
        filepath=os.path.dirname(os.path.abspath(filename))
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        self.outfile = Dataset(filename, 'w', format='NETCDF4')
        self.outfile.createDimension('TSTEP', None)
        self.outfile.createDimension('DATE-TIME', 2)

    def finalize(self):
        start_date, start_time = cmaq_date_time(self.rt_cfg['run']['datestimes'][0])
        cur_date, cur_time = cmaq_date_time(datetime.now())
        self.outfile.EXEC_ID = '????????????????'
        self.outfile.FTYPE = np.int32(1)
        self.outfile.SDATE = np.int32(start_date)
        self.outfile.STIME = np.int32(start_time)
        self.outfile.CDATE = np.int32(cur_date)
        self.outfile.CTIME = np.int32(cur_time)
        self.outfile.WDATE = np.int32(cur_date)
        self.outfile.WTIME = np.int32(cur_time)
        self.outfile.TSTEP = np.int32(self.cfg.run_params.time_params.timestep/3600*10000)
        self.outfile.GDTYP = np.int32(gdtyp_mapping[self.rt_cfg['projection_params']['proj']])
        self.outfile.VGTYP = np.int32(self.cfg.postproc.cmaqareawriter.vgtyp)
        self.outfile.VGTOP = np.float32(self.cfg.postproc.cmaqareawriter.vgtop)
        self.outfile.VGLVLS = np.float32(self.cfg.postproc.cmaqareawriter.vglvls)
        self.outfile.P_ALP = np.float64(self.rt_cfg['projection_params']['p_alp'])
        self.outfile.P_BET = np.float64(self.rt_cfg['projection_params']['p_bet'])
        self.outfile.P_GAM = np.float64(self.rt_cfg['projection_params']['p_gam'])
        self.outfile.XCENT = np.float64(self.rt_cfg['projection_params']['lon_central'])
        self.outfile.YCENT = np.float64(self.rt_cfg['projection_params']['lat_central'])
        xorig = self.cfg.domain.xorg - self.cfg.domain.nx*self.cfg.domain.delx/2.0
        yorig = self.cfg.domain.yorg - self.cfg.domain.ny*self.cfg.domain.dely/2.0
        self.outfile.XORIG = np.float64(xorig)
        self.outfile.YORIG = np.float64(yorig)
        self.outfile.XCELL = np.float64(self.cfg.domain.delx)
        self.outfile.YCELL = np.float64(self.cfg.domain.dely)
        self.outfile.GDNAM = self.cfg.domain.grid_name
        self.outfile.UPNAM = '???'
        self.outfile.HISTORY = '???'

        self.outfile.close()


class CMAQAreaWriter(CMAQWriter):
    """
    Postprocessor class for writing CMAQ area emission file.
    *Non time-optimized version: time disaggregation performed by the
    database backend!*
    """

    def setup(self):
        super().setup(self.cfg.postproc.cmaqareawriter.outfile)

        self.outfile.createDimension('LAY', self.cfg.domain.nz)
        self.outfile.createDimension('ROW', self.cfg.domain.ny)
        self.outfile.createDimension('COL', self.cfg.domain.nx)
        self.outvars = []

    def receive_area_emiss(self, timestep, data):
        date, time = cmaq_date_time(self.rt_cfg['run']['datestimes'][timestep])
        self.timevar[timestep,:,0] = date
        self.timevar[timestep,:,1] = time
        for i, spectuple in enumerate(self.species):
            self.outvars[i][timestep,:,:,:] = data.transpose(2,1,0,3)[:,:,:,i]

    def receive_species(self, species):
        self.species = species

        # Need to wait for the list of species to create these
        self.outfile.createDimension('VAR', len(self.species))
        self.timevar = self.outfile.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
        self.timevar.units = '<YYYYDDD,HHMMSS>'
        self.timevar.long_name = 'FLAG           '
        self.timevar.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '

        for  specid, specname in self.species:
            emisvar = self.outfile.createVariable(specname, 'f4', ('TSTEP', 'LAY', 'ROW', 'COL'))
            emisvar.long_name = long_object_name(specname)
            emisvar.units = 'moles/s for gases and  g/s for aerosols'
            emisvar.var_desc = 'Model species ' + long_object_name(specname)
            self.outvars.append(emisvar)

    def finalize(self):
        self.outfile.NTHIK = np.int32(1)
        self.outfile.NCOLS = np.int32(self.cfg.domain.nx)
        self.outfile.NROWS = np.int32(self.cfg.domain.ny)
        self.outfile.NLAYS = np.int32(self.cfg.domain.nz)
        self.outfile.NVARS = np.int32(len(self.species))
        setattr(self.outfile, 'VAR-LIST', ''.join([long_object_name(spec[1])
                                                   for spec in self.species]))
        self.outfile.FILEDESC='CMAQ area emissions created by FUME ' + self.cfg.run_params.output_params.output_description

        super().finalize()


class CMAQPointWriter(CMAQWriter):
    """
    Postprocessor class for writing CMAQ point emission file.
    """

    def setup(self):
        pointfile_name, pointfile_extension= os.path.splitext(self.cfg.postproc.cmaqpointwriter.outfile)
        pointfile_stacks = pointfile_name + '_stacks' + pointfile_extension

        # point emission file
        super().setup(self.cfg.postproc.cmaqpointwriter.outfile)

        self.outfile.createDimension('LAY', 1)
        self.outfile.createDimension('COL', 1)
        self.outvars = []

        # setup the point source param file 
        self.outfilestk = Dataset(pointfile_stacks, 'w',format='NETCDF4')
        self.outfilestk.createDimension('TSTEP', None)
        self.outfilestk.createDimension('DATE-TIME', 2)

        self.outfilestk.createDimension('LAY', 1)
        self.outfilestk.createDimension('COL', 1)

        self.outfilestk.createDimension('VAR', 15)
        self.timevarstk = self.outfilestk.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
        self.timevarstk.units = '<YYYYDDD,HHMMSS>'
        self.timevarstk.long_name = 'TFLAG          '
        self.timevarstk.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS'

    def receive_point_emiss(self, timestep, data):
        # At the first time step create the NetCDF variables.
        # We need to wait till here to make sure we know all dimensions

        if not ('VAR' in self.outfile.dimensions.keys()):
            self.outfile.createDimension('VAR', len(self.species))

        #if len(self.outvars) == 0:
        if not ('TFLAG' in self.outfile.variables.keys()):
            self.timevar = self.outfile.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
            self.timevar.units = '<YYYYDDD,HHMMSS>'
            self.timevar.long_name = 'FLAG           '
            self.timevar.var_desc = 'Timestep-valid flags:  (1) YYYYDDD or (2) HHMMSS                                '

        for specid, specname in self.species:
            if not (specname in self.outfile.variables.keys()):
                emisvar = self.outfile.createVariable(specname, 'f4', ('TSTEP', 'LAY', 'ROW', 'COL'))
                emisvar.long_name = long_object_name(specname)
                emisvar.units = 'moles/s for gases and  g/s for aerosols'
                emisvar.var_desc = 'Model species ' + long_object_name(specname)
                self.outvars.append(emisvar)

        date, time = cmaq_date_time(self.rt_cfg['run']['datestimes'][timestep])
        self.timevar[timestep,:,0] = date
        self.timevar[timestep,:,1] = time

        self.timevarstk[timestep,:,0] = date
        self.timevarstk[timestep,:,0] = time

        for i, spec1 in enumerate(self.species):
            self.outvars[i][timestep,0,:,0] = data[:,i]

    def receive_stack_params(self, stacks):
        self.point_src_params = stacks
        self.numstk = self.point_src_params.shape[0]
        if self.numstk == 0:
            return

        self.stacks_id = list(map(int,self.point_src_params[:,0]))

        numtimes = len(self.rt_cfg['run']['datestimes'])

        self.outfile.createDimension('ROW', self.numstk)
        self.outfilestk.createDimension('ROW', self.numstk)

        # int variables
        var_int =      ['ISTACK', 'STKCNT', 'ROW', 'COL', 'LMAJOR', 'LPING']
        var_int_units =['none', 'none', 'none', 'none', 'none',  'none']
        stkintvars = []
        xorig = self.cfg.domain.xorg - self.cfg.domain.nx*self.cfg.domain.delx/2.0
        yorig = self.cfg.domain.yorg - self.cfg.domain.ny*self.cfg.domain.dely/2.0

        for var in var_int:
            stkvar = self.outfilestk.createVariable(var, 'i4', ('TSTEP', 'LAY', 'ROW', 'COL'))
            stkvar.longname = long_object_name(var)
            unit = var_int_units[var_int.index(var)]
            stkvar.unit = long_object_name(unit)
            stkintvars.append(stkvar)
        
        stkintvars[0][:,0,:,0] = np.array([self.stacks_id] * numtimes)
        stkintvars[1][:,0,:,0] = np.array([range(0, self.numstk)] * numtimes)
        stkintvars[2][:,0,:,0] = np.array([ ((self.point_src_params[:,2]-xorig)/self.cfg.domain.delx).astype('int') + 1] * numtimes)
        stkintvars[3][:,0,:,0] = np.array([ ((self.point_src_params[:,4]-yorig)/self.cfg.domain.dely).astype('int') + 1] * numtimes)
        stkintvars[4][:,0,:,0] = np.zeros((numtimes,self.numstk),dtype=int)
        stkintvars[5][:,0,:,0] = np.zeros((numtimes,self.numstk),dtype=int)


        # float variables
        var_float = ['LATITUDE', 'LONGITUDE', 'STKDM', 'STKHT', 'STKTK', 'STKVE', 'STKFLW', 'XLOCA', 'YLOCA']
        var_float_units = ['degrees', 'degrees', 'm', 'm', 'degrees K', 'm/s', 'm**3/s', '', '']

        stkfloatvars = []
        for var in var_float:
            stkvar = self.outfilestk.createVariable(var, 'f4', ('TSTEP', 'LAY' , 'ROW', 'COL'))
            stkvar.longname = long_object_name(var)
            unit = var_float_units[var_float.index(var)]
            stkvar.unit = long_object_name(unit)
            stkfloatvars.append(stkvar)

        #LATITUDE
        stkfloatvars[0][:,0,:,0] = np.array([self.point_src_params[:,3]] * numtimes)
        #LONGITUDE
        stkfloatvars[1][:,0,:,0] = np.array([self.point_src_params[:,1] ] * numtimes)
        #STKDM
        stkfloatvars[2][:,0,:,0] = np.array([self.point_src_params[:,6] ] * numtimes)

        #STKHT
        stkfloatvars[3][:,0,:,0] = np.array([ self.point_src_params[:,5] ] * numtimes)

        #STKTK
        stkfloatvars[4][:,0,:,0] = np.array([self.point_src_params[:,7] ] * numtimes)

        #STKVE
        stkfloatvars[5][:,0,:,0] = np.array([self.point_src_params[:,8] ] * numtimes)

        #STKFLW
        stkfloatvars[6][:,0,:,0] = np.array([(0.5*self.point_src_params[:,6])**2 * 3.1415 * self.point_src_params[:,8] ] * numtimes)

        #XLOCA
        stkfloatvars[7][:,0,:,0] = np.array([self.point_src_params[:,2] ] * numtimes)

        #YLOCA
        stkfloatvars[8][:,0,:,0] = np.array([self.point_src_params[:,4]  ] * numtimes)


        var_joined = ''.join([long_object_name(var) for var in var_int + var_float ] )
        setattr(self.outfilestk,'VAR-LIST', var_joined)

    def receive_point_species(self, species):
        self.species = species

    def finalize(self):
        self.outfile.NTHIK = np.int32(1)
        self.outfile.NCOLS = np.int32(self.cfg.domain.nx)
        self.outfile.NROWS = np.int32(self.cfg.domain.ny)
        self.outfile.NLAYS = np.int32(self.cfg.domain.nz)
        self.outfile.NVARS = np.int32(len(self.species))
        setattr(self.outfile, 'VAR-LIST', ''.join([long_object_name(spec[1])
                                                   for spec in self.species]))
        self.outfile.FILEDESC='CMAQ point emissions'


        self.outfilestk.NTHIK = np.int32(1)
        self.outfilestk.NCOLS = np.int32(1)
        self.outfilestk.NROWS = np.int32(self.numstk)
        self.outfilestk.NLAYS = np.int32(1)
        self.outfilestk.NVARS = np.int32(15)

        self.outfilestk.FILEDESC='CMAQ point emissions stack param file created by FUME ' + self.cfg.run_params.output_params.output_description

        start_date, start_time = cmaq_date_time(self.rt_cfg['run']['datestimes'][0])
        cur_date, cur_time = cmaq_date_time(datetime.now())
        self.outfilestk.EXEC_ID = '????????????????'
        self.outfilestk.FTYPE = np.int32(1)
        self.outfilestk.SDATE = np.int32(start_date)
        self.outfilestk.STIME = np.int32(start_time)
        self.outfilestk.CDATE = np.int32(cur_date)
        self.outfilestk.CTIME = np.int32(cur_time)
        self.outfilestk.WDATE = np.int32(cur_date)
        self.outfilestk.WTIME = np.int32(cur_time)
        self.outfilestk.TSTEP = np.int32(self.cfg.run_params.time_params.timestep/3600*10000)
        self.outfilestk.GDTYP = np.int32(gdtyp_mapping[self.rt_cfg['projection_params']['proj']])
        self.outfilestk.VGTYP = np.int32(self.cfg.postproc.cmaqareawriter.vgtyp)
        self.outfilestk.VGTOP = np.float32(self.cfg.postproc.cmaqareawriter.vgtop)
        self.outfilestk.VGLVLS = np.float32(self.cfg.postproc.cmaqareawriter.vglvls)
        self.outfilestk.P_ALP = np.float64(self.rt_cfg['projection_params']['p_alp'])
        self.outfilestk.P_BET = np.float64(self.rt_cfg['projection_params']['p_bet'])
        self.outfilestk.P_GAM = np.float64(self.rt_cfg['projection_params']['p_gam'])
        self.outfilestk.XCENT = np.float64(self.rt_cfg['projection_params']['lon_central'])
        self.outfilestk.YCENT = np.float64(self.rt_cfg['projection_params']['lat_central'])
        xorig = self.cfg.domain.xorg - self.cfg.domain.nx*self.cfg.domain.delx/2.0
        yorig = self.cfg.domain.yorg - self.cfg.domain.ny*self.cfg.domain.dely/2.0
        self.outfilestk.XORIG = np.float64(xorig)
        self.outfilestk.YORIG = np.float64(yorig)
        self.outfilestk.XCELL = np.float64(self.cfg.domain.delx)
        self.outfilestk.YCELL = np.float64(self.cfg.domain.dely)
        self.outfilestk.GDNAM = self.cfg.domain.grid_name
        self.outfilestk.UPNAM = '???'
        self.outfilestk.HISTORY = '???'
        self.outfilestk.close()

        super().finalize()
