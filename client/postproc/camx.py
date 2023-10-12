"""
Description: CAMx output postprocessor

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

from datetime import timedelta, datetime
import numpy as np
import lib.ep_io_fortran as mt
from netCDF4 import Dataset
from postproc.receiver import DataReceiver, requires
from postproc.netcdf import NetCDFAreaTimeDisaggregator
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)


# CAMx specific projection mapping IDs
gdtyp_mapping = {
        'LATLON': 0,
        'UTM': 1,
        'LAMBERT': 2,
        'STEREO': 3,
        'POLAR': 4,
        'EMERCATOR': 5,
        'MERCATOR': 5
}

cproj_mapping = {
        'LATLON': 0,
        'LAMBERT': 2,
        'RPOLAR': 3,
        'POLAR': 4,
        'MERCATOR': 5,
        'EMERCATOR': 5,
        'UTM': 1
}

ione = int(1)
rdum = float(0.)


def camx_date_time(dt, cal = False):
    if cal:
        return(dt.year*10000 + dt.month*100 + dt.day ,
           dt.hour + (dt.minute + dt.second/60.0)/60.0)
    else:
        return (dt.year*1000 + dt.timetuple().tm_yday,
            dt.hour + (dt.minute + dt.second/60.0)/60.0)


def cmaq_date_time(dt):
    return (dt.year*1000 + dt.timetuple().tm_yday,
            dt.hour*10000 + dt.minute*100 + dt.second)


def long_object_name(s):
    return '{0:16s}'.format(s)


class CAMxWriter(DataReceiver):
    """
    Postprocessor class for writing CAMx emission input files.
    This is the base class for the common CAMx format settings and should not be
    instantiated by itself. Descendant classes CAMxAreaWriter or CAMxPointWriter
    should be used instead.

    Implements only common setup (create output netcdf file, time variable) and
    finalize (set common global attributes of the netcdf file and close the
    file).
    """

    def setup(self, filename):
        self.outfile = open(filename, mode='wb')

        # datetime data needed when writing both file header and data
        # for both CAMx point and area sources
        bdate, btime = [], []
        for dt in self.rt_cfg['run']['datestimes']:
            dt1, dt2 = camx_date_time(dt)
            bdate.append(dt1)
            btime.append(dt2)

        # for CAMx, additional timestep is needed
        edatetime = self.rt_cfg['run']['datestimes'][-1]
        dt_p1 = edatetime + timedelta(
            seconds=self.cfg.run_params.time_params.timestep)

        dt1, dt2 = camx_date_time(dt_p1)
        bdate.append(dt1)
        btime.append(dt2)

        self.bdate = bdate
        self.btime = btime

    def finalize(self):
        self.outfile.close()


class CAMxAreaWriterBase(CAMxWriter):
    """
    Postprocessor class for writing CAMx area emission file.
    """

    def setup(self):
        super().setup(self.cfg.postproc.camxareawriter.outfile)

    def receive_species(self, species):
        self.species = species
        self.numspec = len(species)

        emiss = 'EMISSIONS '
        emisslong = [emiss[i]+'   ' for i in range(10)]

        notes = ('CAMx area emissions created by FUME ' +
                 self.cfg.run_params.output_params.output_description)
        if len(notes) > 60:
            tmp = notes[0:60]
        else:
            tmp = '{:60s}'.format(notes)

        notesformatted = [tmp[i] + '   ' for i in range(60)]
        emisname = ['{:10s}'.format(species[i][1]) for i in range(self.numspec)]
        longemisname = ['' for i in range(self.numspec)]
        for i in range(self.numspec):
            longemisname[i] = [emisname[i][j] + '   ' for j in range(10)]

        self.longemisname = longemisname

        istag = 0

        proj = self.rt_cfg['projection_params']['proj']
        p_alp = self.rt_cfg['projection_params']['p_alp']
        p_bet = self.rt_cfg['projection_params']['p_bet']
        XCENT = self.rt_cfg['projection_params']['lon_central']
        YCENT = self.rt_cfg['projection_params']['lat_central']

        # domain parameters
        nx = self.cfg.domain.nx
        ny = self.cfg.domain.ny
        nz = self.cfg.domain.nz
        delx = self.cfg.domain.delx
        dely = self.cfg.domain.dely
        xorg = self.cfg.domain.xorg
        yorg = self.cfg.domain.yorg
        # the S/W corner of the grid (as gridboxes)
        xorig = xorg - nx*delx/2.0
        yorig = yorg - ny*dely/2.0
        endian = self.cfg.run_params.output_params.endian

        mt.write_record(self.outfile, endian, '40s240siiifif',
                        ''.join(emisslong).encode('utf-8'),
                        ''.join(notesformatted).encode('utf-8'),
                        int(self.cfg.run_params.time_params.itzone_out),
                        int(self.numspec), self.bdate[0], self.btime[0],
                        self.bdate[-1], self.btime[-1])
        mt.write_record(self.outfile, self.cfg.run_params.output_params.endian,
                        'ffiffffiiiiifff', XCENT, YCENT, int(p_alp),
                        xorig, yorig, delx, dely, nx, ny, nz,
                        int(gdtyp_mapping[proj]), istag, p_alp, p_bet, rdum)
        mt.write_record(self.outfile, endian, 'iiii', ione, ione, nx, ny)

        joinedstr = [''.join(longemisname[i]) for i in range(self.numspec)]

        fmt_str = str(40*self.numspec)+'s'
        mt.write_record(self.outfile, endian, fmt_str,
                        ''.join(joinedstr).encode('utf-8'))

    def finalize(self):
        super().finalize()


class CAMxAreaTimeWriter(CAMxAreaWriterBase):
    def setup(self):
        super().setup()

    def receive_categories(self, categories):
        self.categories = categories

    def receive_time_factors(self, factors):
        self.time_factors = factors

    def receive_time_shifts(self, time_shifts):
        self.time_shifts = time_shifts
        self.ts = []
        for ts_id in time_shifts:
            if ts_id[0] not in self.ts:
                self.ts.append(ts_id[0])
        self.ts_lookup = {member: idx for idx, member in enumerate(self.ts)}

    @requires('time_shifts')
    def finalize(self):
        self.infile = Dataset(self.cfg.postproc.netcdfareawriter.totalfile)

        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
            log.debug('Time step ', time_idx, stepdt.replace(tzinfo=None))
            mt.write_record(self.outfile,
                            self.cfg.run_params.output_params.endian,
                            'ifif', self.bdate[time_idx], self.btime[time_idx],
                            self.bdate[time_idx+1], self.btime[time_idx+1])

            for spec_idx, (specid, specname) in enumerate(self.species):
                fmt_str = 'i40s'+str(self.cfg.domain.nx*self.cfg.domain.ny)+'f'
                # Create a data placeholder with the data from the first
                # category, then sum for the rest of the categories.
                # In CAMx, we do not have elevated emissions (3D emissions),
                # so sum up the vertical column too
                data = np.zeros(self.infile.variables[specname][0][0].shape, dtype='f')
                for ts_id in self.ts:
                    ts_idx = self.ts_lookup[ts_id]
                    for cat_idx, (catid, catname) in enumerate(self.categories):
                        dtutc = self.time_shifts[(ts_id, stepdt)]
                        tf = self.time_factors[dtutc]
                        try:
                            time_factor = float(tf[catid])
                        except KeyError:
                            continue

                        data += self.infile.variables[specname][ts_idx, cat_idx, :, :, :].filled(fill_value=0)*time_factor

                emis2d = np.sum(data, axis=0).transpose()
                data2d = emis2d.flatten('F')*self.cfg.run_params.time_params.timestep
                mt.write_record(self.outfile,
                                self.cfg.run_params.output_params.endian,
                                fmt_str, ione,
                                ''.join(self.longemisname[spec_idx]).encode('utf-8'),
                                *data2d)


class CAMxAreaWriter(CAMxAreaWriterBase):
    def receive_area_emiss(self, timestep, data):
        mt.write_record(self.outfile, self.cfg.run_params.output_params.endian,
                        'ifif', self.bdate[timestep], self.btime[timestep],
                        self.bdate[timestep+1], self.btime[timestep+1])

        for i in range(self.numspec):
            fmt_str = 'i40s'+str(self.cfg.domain.nx*self.cfg.domain.ny)+'f'
            # in CAMx, we do not have elevated emissions (3D emissions),
            # so sum up to the ground
            emis2d = np.sum(data, axis=2)
            data2d = emis2d[:, :, i].flatten('F')*self.cfg.run_params.time_params.timestep
            mt.write_record(self.outfile,
                            self.cfg.run_params.output_params.endian,
                            fmt_str, ione,
                            ''.join(self.longemisname[i]).encode('utf-8'),
                            *data2d)


class CAMxPointWriter(CAMxWriter):
    """
    Postprocessor class for writing CAMx point emission file.
    """

    def setup(self):
        super().setup(self.cfg.postproc.camxpointwriter.outfile)

    def receive_stack_params(self, stacks):
        self.point_src_params = np.array(stacks, dtype=np.float)
        self.numstk = self.point_src_params.shape[0]
        self.stacks_id = list(map(int, self.point_src_params[:, 0]))

    @requires('stack_params')
    def receive_point_species(self, pspecies):
        self.pspecies = pspecies
        self.numspec = len(pspecies)

        emiss = 'PTSOURCE  '
        emisslong = [emiss[i]+'   ' for i in range(10)]

        notes = ('CAMx point emissions created by FUME ' +
                 self.cfg.run_params.output_params.output_description)
        if len(notes) > 60:
            tmp = notes[0:60]
        else:
            tmp = '{:60s}'.format(notes)

        notesformatted = [tmp[i]+'   ' for i in range(60)]

        emisname = ['{:10s}'.format(self.pspecies[i][1])
                    for i in range(self.numspec)]

        longemisname = ['' for i in range(self.numspec)]

        for i in range(self.numspec):
            longemisname[i] = [emisname[i][j]+'   ' for j in range(10)]

        self.longemisname = longemisname

        istag = 0

        proj = self.rt_cfg['projection_params']['proj']
        p_alp = self.rt_cfg['projection_params']['p_alp']
        p_bet = self.rt_cfg['projection_params']['p_bet']
        XCENT = self.rt_cfg['projection_params']['lon_central']
        YCENT = self.rt_cfg['projection_params']['lat_central']
        nx = self.cfg.domain.nx
        ny = self.cfg.domain.ny
        nz = self.cfg.domain.nz
        delx = self.cfg.domain.delx
        dely = self.cfg.domain.dely
        xorg = self.cfg.domain.xorg
        yorg = self.cfg.domain.yorg
        # the S/W corner of the grid (as gridboxes)
        xorig = xorg - nx*delx/2.0
        yorig = yorg - ny*dely/2.0
        endian = self.cfg.run_params.output_params.endian

        mt.write_record(self.outfile, endian, '40s240siiifif',
                        ''.join(emisslong).encode('utf-8'),
                        ''.join(notesformatted).encode('utf-8'),
                        int(self.cfg.run_params.time_params.itzone_out),
                        int(self.numspec), self.bdate[0], self.btime[0],
                        self.bdate[-1], self.btime[-1])
        mt.write_record(self.outfile, self.cfg.run_params.output_params.endian,
                        'ffiffffiiiiifff', XCENT, YCENT, int(p_alp),
                        xorig, yorig, delx, dely, nx, ny, nz,
                        int(gdtyp_mapping[proj]), istag, p_alp, p_bet, rdum)
        mt.write_record(self.outfile, endian, 'iiii', ione, ione, nx, ny)

        joinedstr = [''.join(longemisname[i]) for i in range(self.numspec)]

        fmt_str = str(40*self.numspec)+'s'
        mt.write_record(self.outfile, endian, fmt_str,
                        ''.join(joinedstr).encode('utf-8'))

        mt.write_record(self.outfile, endian, 'ii', ione, self.numstk)

        var_list = []
        for i in range(self.numstk):
            stk_list = [self.point_src_params[i, j] for j in (2, 4, 5, 6, 7, 8)]
            var_list.extend(stk_list)

        fmt_str = str(6*self.numstk)+'f'
        mt.write_record(self.outfile, endian, fmt_str, *var_list)

    @requires('point_species')
    def receive_point_emiss(self, timestep, data):
        endian = self.cfg.run_params.output_params.endian
        mt.write_record(self.outfile, endian, 'ifif',
                        self.bdate[timestep], self.btime[timestep],
                        self.bdate[timestep+1], self.btime[timestep+1])
        mt.write_record(self.outfile, endian, 'ii', ione, self.numstk)
        var_list = []
        # create list to write

        for i in range(self.numstk):
            var_list.extend([ione, ione, ione, rdum, rdum])

        fmt_str = self.numstk*'iiiff'

        mt.write_record(self.outfile, endian, fmt_str, *var_list)
        pemis = np.array(data)
        for i in range(self.numspec):
            pemis1 = pemis[:, i]*self.cfg.run_params.time_params.timestep
            fmt_str = 'i40s'+str(self.numstk)+'f'
            mt.write_record(self.outfile, endian, fmt_str, ione,
                            ''.join(self.longemisname[i]).encode('utf-8'),
                            *pemis1)

    def finalize(self):
        super().finalize()


class CAMxNetCDFAreaTimeWriter(NetCDFAreaTimeDisaggregator):
    """
    Postprocessor writing time disaggregated area emissions for CAMx in netcdf format.
    *Time-optimized version: time disaggregation performed with NetCDF files. Requires
    a prior run of NetCDFTotalAreaWriter!*
    """

    def setup(self):
        """
        Run NetCDF setup with CAMx-specific overrides:
        Dimensions names are x=COL, Y=ROW, Z=LAY, T=TSTEP
        Do not let the parent class create a time variable,
        projection attributes and close the file during finalize.
        """
        super().setup(filename=self.cfg.postproc.camxareawriter.outfile,
                      no_create_t_var=True, no_create_projection_attrs=True,
                      create_v_dim=True, v_dim='VAR',
                      x_dim='COL', y_dim='ROW', z_dim='LAY', t_dim='TSTEP',
                      no_close_outfile=True, emission_units = 'mol/hr for gases, g/hr for aerosols', scale_factor = self.cfg.run_params.time_params.timestep)

        self.outfile.createDimension('DATE-TIME', 2)        

    def finalize(self):
        """
        Finalization steps
        ------------------

         - create the CAMx-specific time variable TFLAG
         - run parent finalize (save output data and generic NetCDF attributes)
         - save time data
         - add coordinate variables x,y,latitudes,longitudes
         - change the long_name attribute of output variables to CAMx format
         - save CAMx-specific attributes
         - close the file

        """
        self.timevar = self.outfile.createVariable('TFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
        self.timevar.units = '<YYYYJJJ,HHMMSS>'
        self.timevar.long_name = 'Start time flag'
        self.timevar.var_desc = 'Timestep start date and time'

        self.timevare = self.outfile.createVariable('ETFLAG', 'i4', ('TSTEP', 'VAR', 'DATE-TIME'))
        self.timevare.units = '<YYYYJJJ,HHMMSS>'
        self.timevare.long_name = 'End time flag'
        self.timevare.var_desc = 'Timestep end date and time'

        super().finalize()

        # datetime data needed when writing both file header and data
        numtimes = len(self.rt_cfg['run']['datestimes'])
        bdate, btime = [], []
        for dt in self.rt_cfg['run']['datestimes']:
            dt1, dt2 = cmaq_date_time(dt)
            bdate.append(dt1)
            btime.append(dt2)

        # for CAMx, additional timestep is needed to upper-bound the last interval
        edatetime = self.rt_cfg['run']['datestimes'][-1]
        dt_p1 = edatetime + timedelta(
            seconds=self.cfg.run_params.time_params.timestep)

        dt1, dt2 = cmaq_date_time(dt_p1)
        bdate.append(dt1)
        btime.append(dt2)
        for timestep in range(numtimes):
            # lower bound of time intervals
            self.timevar[timestep,:,0] = bdate[timestep]
            self.timevar[timestep,:,1] = btime[timestep]
            # upper bound of time intervals
            self.timevare[timestep,:,0] = bdate[timestep+1]
            self.timevare[timestep,:,1] = btime[timestep+1]

        proj = self.rt_cfg['projection_params']['proj']
        # add coordinate variables
        self.coord_x = self.outfile.createVariable('X', 'f8', ('COL'))
        if proj == 'LATLON':
            self.coord_x.units = 'lon'
            self.coord_x.long_name = 'X coordinate'
            self.coord_x.var_desc = 'longitude degrees east'
        else:
            self.coord_x.units = 'km'
            self.coord_x.long_name = 'X coordinate'
            self.coord_x.var_desc = 'X cartesian distance from projection origin'
        
        self.coord_y = self.outfile.createVariable('Y', 'f8', ('ROW'))
        if proj == 'LATLON':
            self.coord_y.units = 'lat'
            self.coord_y.long_name = 'Y coordinate'
            self.coord_y.var_desc = 'latitude degrees north'
        else:
            self.coord_y.units = 'km'
            self.coord_y.long_name = 'Y coordinate'
            self.coord_y.var_desc = 'Y cartesian distance from projection origin'


        self.lon = self.outfile.createVariable('longitude', 'f8', ('ROW','COL'))
        self.lon.units = "Degrees east"
        self.lon.long_name = "Longitude"
        self.lon.var_desc = "Longitude degrees east"
        self.lon.coordinates = "latitude longitude"

        self.lat = self.outfile.createVariable('latitude', 'f8', ('ROW','COL'))
        self.lat.units = "Degrees north"
        self.lat.long_name = "Latitude"
        self.lat.var_desc = "Latitude degrees north"
        self.lat.coordinates = "latitude longitude"

        # save the coordinate variables

        nx = self.cfg.domain.nx
        ny = self.cfg.domain.ny
        nz = self.cfg.domain.nz

        xorg = self.cfg.domain.xorg
        yorg = self.cfg.domain.yorg

        delx = self.cfg.domain.delx
        dely = self.cfg.domain.dely


        xorig = xorg - nx*delx/2.0
        yorig = yorg - ny*dely/2.0

        for i in range(nx):
            self.coord_x[i] = xorig + i*delx + delx/2.0      
        for j in range(ny):
            self.coord_y[j] = yorig + j*dely + dely/2.0

        import pyproj
        myproj = pyproj.Proj(self.cfg.projection_params.projection_proj4)
        for i in range(nx):
            for j in range(ny):
                self.lon[j,i], self.lat[j,i] = myproj(xorig + i*delx + delx/2.0,yorig + j*dely + dely/2.0,  inverse=True)

        # vertical layers
        self.lev = self.outfile.createVariable('layer', 'f8', ('LAY'))
        self.lev.units = "Layer index"
        self.lev.long_name = "Model layer"
        self.lev.var_desc = "Model layer"
        self.lev[:] = np.arange(nz)
        for outvar in self.outvars:
            outvar.var_desc  = outvar.long_name+' emissions'


        # // Global Attributes //
        start_date, start_time = camx_date_time(self.rt_cfg['run']['datestimes'][0])
        
        self.outfile.SDATE, self.outfile.STIME = np.int32(start_date), np.int32(start_time)
        
        start_datec, dummy = camx_date_time(self.rt_cfg['run']['datestimes'][0], cal = True)
        self.outfile.SDATEC = np.int32(start_datec)

        timestep = self.cfg.run_params.time_params.timestep
        HH =  int(timestep) / 3600
        MM =  (int(timestep) - HH*3600) / 60
        SS = int(timestep) - HH*3600 - MM*60      
        
        self.outfile.TSTEP =  np.int32(HH*10000 + MM*100 + SS)
        self.outfile.NSTEPS = np.int32(numtimes)
        self.outfile.NCOLS = np.int32(nx)
        self.outfile.NROWS = np.int32(ny)
        self.outfile.NLAYS = np.int32(nz)
        self.outfile.NVARS = np.int32(len(self.outvars))
        self.outfile.P_ALP = np.float64(self.rt_cfg['projection_params']['p_alp'])
        self.outfile.P_BET = np.float64(self.rt_cfg['projection_params']['p_bet'])
        self.outfile.P_GAM = np.float64(self.rt_cfg['projection_params']['p_gam'])
        self.outfile.XCENT = np.float64(self.rt_cfg['projection_params']['lon_central'])
        self.outfile.YCENT = np.float64(self.rt_cfg['projection_params']['lat_central'])
        self.outfile.XORIG = np.float64(xorig)
        self.outfile.YORIG = np.float64(yorig)
        self.outfile.IUTM = np.int32(1)
        self.outfile.ISTAG = np.int32(0)
        self.outfile.CPROJ = np.int32(cproj_mapping[self.rt_cfg['projection_params']['proj']])
        self.outfile.ITZON = np.int32(self.cfg.run_params.time_params.itzone_out)
        setattr(self.outfile, 'VAR-LIST', ''.join([long_object_name(outvar.long_name) for outvar in self.outvars]))
        self.outfile.CAMx_NAME = "EMISSIONS"
        self.outfile.FILEDESC = "EMISSIONS"
        self.outfile.NOTE = "CAMx v7+ area emission file generated by FUME"
        self.outfile.HISTORY = ""
        self.outfile.FTYPE = np.int32(1)
        cdate, ctime = camx_date_time(datetime.now())
        self.outfile.CDATE = np.int32(cdate)
        self.outfile.CTIME = np.int32(ctime)
        self.outfile.WDATE = np.int32(cdate)
        self.outfile.WTIME = np.int32(ctime)
        self.outfile.GDTYP = np.int32(gdtyp_mapping[self.rt_cfg['projection_params']['proj']])
        self.outfile.NTHIK = np.int32(1)
        self.outfile.VGTYP = np.int32(1)
        self.outfile.VGTOP = np.int32(1)
        self.outfile.VGLVLS = np.int32(0)
        self.outfile.GDNAM = self.cfg.domain.grid_name
        self.outfile.UPNAM = ""
        self.outfile.UPDSC = ""

        self.outfile.close()


######################################
# auxiliary postproc classes 
######################################

class CAMxAreaTimeWriterFromTotalFile(CAMxWriter):
    """
    Postprocessor writing time disaggregated area emissions for CAMx in UAM/NetCDF format.
    *Time-optimized version: time disaggregation performed with NetCDF files. Requires
    a prior run of NetCDFTotalAreaWriter!* Uses only the netcdf total file for all the species/category information.
    !!!! IMPORTANT !!!!
    In this class, these are not taken from the database but read in from the file. The only data coming from the database are the timefactors/time_shifts
    """

    def setup(self):
        super().setup(self.cfg.postproc.camxareawriter.outfile)


    def receive_time_factors(self, factors):
        self.time_factors = factors

    def receive_time_shifts(self, time_shifts):
        self.time_shifts = time_shifts
        self.ts = []
        for ts_id in time_shifts:
            if ts_id[0] not in self.ts:
                self.ts.append(ts_id[0])
        self.ts_lookup = {member: idx for idx, member in enumerate(self.ts)}


    @requires('time_shifts')
    def finalize(self):

        category_id_name = 'category_id'
        category_name_name = 'category_name'
        time_shift_name = 'ts_id'


        _ignorevars = [time_shift_name, category_id_name, category_name_name]
        self.infile = Dataset(self.cfg.postproc.netcdfareawriter.totalfile)

        # get the list of categories from netcdf
        category_ids   = self.infile.variables[category_id_name][:]
        category_names = self.infile.variables[category_name_name][:]

        categories = list(zip(category_ids, category_names) )# (list of (catid, catname) tuples )
        # get the list of species from netcdf

        species = [ s  for s in self.infile.variables if s not in _ignorevars] # list of species
        numspec = len(species)
        emiss = 'EMISSIONS '
        emisslong = [emiss[i]+'   ' for i in range(10)]

        notes = ('CAMx area emissions created by FUME ' +
                 self.cfg.run_params.output_params.output_description)
        if len(notes) > 60:
            tmp = notes[0:60]
        else:
            tmp = '{:60s}'.format(notes)

        notesformatted = [tmp[i] + '   ' for i in range(60)]
        emisname = ['{:10s}'.format(species[i]) for i in range(numspec)]
        longemisname = ['' for i in range(numspec)]
        for i in range(numspec):
            longemisname[i] = [emisname[i][j] + '   ' for j in range(10)]

        istag = 0

        proj = self.rt_cfg['projection_params']['proj']
        p_alp = self.rt_cfg['projection_params']['p_alp']
        p_bet = self.rt_cfg['projection_params']['p_bet']
        XCENT = self.rt_cfg['projection_params']['lon_central']
        YCENT = self.rt_cfg['projection_params']['lat_central']

        # domain parameters
        nx = self.cfg.domain.nx
        ny = self.cfg.domain.ny
        nz = self.cfg.domain.nz
        delx = self.cfg.domain.delx
        dely = self.cfg.domain.dely
        xorg = self.cfg.domain.xorg
        yorg = self.cfg.domain.yorg
        # the S/W corner of the grid (as gridboxes)
        xorig = xorg - nx*delx/2.0
        yorig = yorg - ny*dely/2.0
        endian = self.cfg.run_params.output_params.endian

        mt.write_record(self.outfile, endian, '40s240siiifif',
                        ''.join(emisslong).encode('utf-8'),
                        ''.join(notesformatted).encode('utf-8'),
                        int(self.cfg.run_params.time_params.itzone_out),
                        int(numspec), self.bdate[0], self.btime[0],
                        self.bdate[-1], self.btime[-1])
        mt.write_record(self.outfile, self.cfg.run_params.output_params.endian,
                        'ffiffffiiiiifff', XCENT, YCENT, int(p_alp),
                        xorig, yorig, delx, dely, nx, ny, nz,
                        int(gdtyp_mapping[proj]), istag, p_alp, p_bet, rdum)
        mt.write_record(self.outfile, endian, 'iiii', ione, ione, nx, ny)

        joinedstr = [''.join(longemisname[i]) for i in range(numspec)]

        fmt_str = str(40*numspec)+'s'
        mt.write_record(self.outfile, endian, fmt_str,
                        ''.join(joinedstr).encode('utf-8'))

        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
            log.debug('Time step ', time_idx, stepdt.replace(tzinfo=None))
            mt.write_record(self.outfile,
                            self.cfg.run_params.output_params.endian,
                            'ifif', self.bdate[time_idx], self.btime[time_idx],
                            self.bdate[time_idx+1], self.btime[time_idx+1])

            for spec_idx, specname in enumerate(species):
                fmt_str = 'i40s'+str(self.cfg.domain.nx*self.cfg.domain.ny)+'f'
                # Create a data placeholder with the data from the first
                # category, then sum for the rest of the categories.
                # In CAMx, we do not have elevated emissions (3D emissions),
                # so sum up the vertical column too
                data = np.zeros(self.infile.variables[specname][0][0].shape, dtype='f')
                for ts_id in self.ts:
                    ts_idx = self.ts_lookup[ts_id]
                    for cat_idx, (catid, catname) in enumerate(categories):
                        dtutc = self.time_shifts[(ts_id, stepdt)]
                        tf = self.time_factors[dtutc]
                        try:
                            time_factor = float(tf[catid])
                        except KeyError:
                            continue

                        data += self.infile.variables[specname][ts_idx, cat_idx, :, :, :].filled(fill_value=0)*time_factor

                emis2d = np.sum(data, axis=0).transpose()
                data2d = emis2d.flatten('F')*self.cfg.run_params.time_params.timestep
                mt.write_record(self.outfile,
                                self.cfg.run_params.output_params.endian,
                                fmt_str, ione,
                                ''.join(longemisname[spec_idx]).encode('utf-8'),
                                *data2d)


class CAMxPointTimeWriterFromTotalFile(CAMxWriter):
    """
    Postprocessor writing time disaggregated point emissions for CAMx in UAM format.
    *Time-optimized version: time disaggregation performed with NetCDF files. Requires
    a prior run of NetCDFTotalAreaWriter!* Uses only the netcdf total file for all the species/category information.
    !!!! IMPORTANT !!!!
    In this class, these are not taken from the database but read in from the file. The only data coming from the database are the timefactors/time_shifts
    """

    def setup(self):
        super().setup(self.cfg.postproc.camxpointwriter.outfile)


    def receive_time_factors(self, factors):
        self.time_factors = factors

    def receive_time_shifts(self, time_shifts):
        self.time_shifts = time_shifts
        self.ts = []
        for ts_id in time_shifts:
            if ts_id[0] not in self.ts:
                self.ts.append(ts_id[0])
        self.ts_lookup = {member: idx for idx, member in enumerate(self.ts)}


    @requires('time_shifts')
    def finalize(self):

        category_id_name = 'category_id'
        category_name_name = 'category_name'
        time_shift_name = 'ts_id'
        stack_param_names_int = ['ISTACK', 'STKCNT', 'ROW', 'COL', 'LMAJOR', 'LPING']
        stack_param_names_float = ['LATITUDE', 'LONGITUDE', 'STKDM', 'STKHT', 'STKTK', 'STKVE', 'STKFLW', 'XLOCA', 'YLOCA']

        _ignorevars = [time_shift_name, category_id_name, category_name_name] + stack_param_names_int + stack_param_names_float
        self.infile = Dataset(self.cfg.postproc.netcdfpointwriter.totalfile)

        # get the list of categories from netcdf
        category_ids   = self.infile.variables[category_id_name][:]
        category_names = self.infile.variables[category_name_name][:]
        categories = list(zip(category_ids, category_names) )# (list of (catid, catname) tuples )

        # get the list of species from netcdf
        species = [ s  for s in self.infile.variables if s not in _ignorevars] # list of species


        # get stack params
        stack_params_int = []
        for i,p in enumerate(stack_param_names_int):
            stack_params_int.append( self.infile.variables[p][:])

        stack_params_float = []
        for i,p in enumerate(stack_param_names_float):
            stack_params_float.append( self.infile.variables[p][:])
        numstk = len(stack_params_float[0])

        numspec = len(species)
        emiss = 'PTSOURCE  '
        emisslong = [emiss[i]+'   ' for i in range(10)]

        notes = ('CAMx point emissions created by FUME ' +
                 self.cfg.run_params.output_params.output_description)
        if len(notes) > 60:
            tmp = notes[0:60]
        else:
            tmp = '{:60s}'.format(notes)

        notesformatted = [tmp[i] + '   ' for i in range(60)]
        emisname = ['{:10s}'.format(species[i]) for i in range(numspec)]
        longemisname = ['' for i in range(numspec)]
        for i in range(numspec):
            longemisname[i] = [emisname[i][j] + '   ' for j in range(10)]

        istag = 0

        proj = self.rt_cfg['projection_params']['proj']
        p_alp = self.rt_cfg['projection_params']['p_alp']
        p_bet = self.rt_cfg['projection_params']['p_bet']
        XCENT = self.rt_cfg['projection_params']['lon_central']
        YCENT = self.rt_cfg['projection_params']['lat_central']

        # domain parameters
        nx = self.cfg.domain.nx
        ny = self.cfg.domain.ny
        nz = self.cfg.domain.nz
        delx = self.cfg.domain.delx
        dely = self.cfg.domain.dely
        xorg = self.cfg.domain.xorg
        yorg = self.cfg.domain.yorg
        # the S/W corner of the grid (as gridboxes)
        xorig = xorg - nx*delx/2.0
        yorig = yorg - ny*dely/2.0
        endian = self.cfg.run_params.output_params.endian

        mt.write_record(self.outfile, endian, '40s240siiifif',
                        ''.join(emisslong).encode('utf-8'),
                        ''.join(notesformatted).encode('utf-8'),
                        int(self.cfg.run_params.time_params.itzone_out),
                        int(numspec), self.bdate[0], self.btime[0],
                        self.bdate[-1], self.btime[-1])
        mt.write_record(self.outfile, self.cfg.run_params.output_params.endian,
                        'ffiffffiiiiifff', XCENT, YCENT, int(p_alp),
                        xorig, yorig, delx, dely, nx, ny, nz,
                        int(gdtyp_mapping[proj]), istag, p_alp, p_bet, rdum)
        mt.write_record(self.outfile, endian, 'iiii', ione, ione, nx, ny)

        joinedstr = [''.join(longemisname[i]) for i in range(numspec)]

        fmt_str = str(40*numspec)+'s'
        mt.write_record(self.outfile, endian, fmt_str,
                        ''.join(joinedstr).encode('utf-8'))

        # stack param record
        mt.write_record(self.outfile, endian, 'ii', ione, numstk)
        var_list = []
        for i in range(numstk):
            stk_list = [stack_params_float[7][i], stack_params_float[8][i], stack_params_float[3][i], stack_params_float[2][i], stack_params_float[4][i], stack_params_float[5][i] ]
            var_list.extend(stk_list)
        fmt_str = str(6*numstk)+'f'
        mt.write_record(self.outfile, endian, fmt_str, *var_list)

        # time var record
        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
            log.debug('Time step ', time_idx, stepdt.replace(tzinfo=None))
            mt.write_record(self.outfile,
                            self.cfg.run_params.output_params.endian,
                            'ifif', self.bdate[time_idx], self.btime[time_idx],
                            self.bdate[time_idx+1], self.btime[time_idx+1])
            mt.write_record(self.outfile, endian, 'ii', ione, numstk)

            var_list = []
            for i in range(numstk):
                var_list.extend([ione, ione, ione, rdum, rdum])

            fmt_str = numstk*'iiiff'
            mt.write_record(self.outfile, endian, fmt_str, *var_list)

            for spec_idx, specname in enumerate(species):
                fmt_str = 'i40s'+str(self.cfg.domain.nx*self.cfg.domain.ny)+'f'
                # Create a data placeholder with the data from the first
                # category, then sum for the rest of the categories.
                # In CAMx, we do not have elevated emissions (3D emissions),
                # so sum up the vertical column too
                data = np.zeros((numstk), dtype='f')
                for ts_id in self.ts:
                    ts_idx = self.ts_lookup[ts_id]
                    for cat_idx, (catid, catname) in enumerate(categories):
                        dtutc = self.time_shifts[(ts_id, stepdt)]
                        tf = self.time_factors[dtutc]
                        try:
                            time_factor = float(tf[catid])
                        except KeyError:
                            continue

                        data += self.infile.variables[specname][ts_idx, cat_idx, :].filled(fill_value=0)*time_factor

                fmt_str = 'i40s'+str(numstk)+'f'
                mt.write_record(self.outfile,
                                self.cfg.run_params.output_params.endian,
                                fmt_str, ione,
                                ''.join(longemisname[spec_idx]).encode('utf-8'),
                                *data)
