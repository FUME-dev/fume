"""
Description: FUME module to create emissions for PALM microscale model.
1) PalmTotalAreaWriter class creates total emission file.
2) PALMAreaTimeWriter class creates time disaggregated area 2d PALM emission file.
3) PALMVsrcTimeWriter class creates time disaggregated generic volume sources PALM emission file.
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
from netCDF4 import Dataset, date2num
from postproc.receiver import DataReceiver, requires
from postproc.netcdf import NetCDFAreaTimeDisaggregator, NetCDFTotalWriter
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)

class PalmTotalAreaWriter(NetCDFTotalWriter):
    """
    Postprocessor class for writing NetCDF area emission file.
    Outputs are: total emission data by species, category, and level.
    Overrides:
        z dim -> level
        z var -> level
    """
    def setup(self, *args, **kwargs):
        if 'filename' not in kwargs:
            if self.cfg.postproc.palmwriter.totalfile:
                kwargs['filename'] = self.cfg.postproc.palmwriter.totalfile
            else:
                log.error('Missing configuration parameter postproc.palmwriter.totalfile!')
        if 'undef' not in kwargs and self.cfg.postproc.palmwriter.undef:
            kwargs['undef'] = float(self.cfg.postproc.palmwriter.undef)
        kwargs['create_z_dim'] = False
        kwargs['create_z_var'] = False
        kwargs['z_dim'] = 'level'
        kwargs['z_var'] = 'level'
        super().setup(*args, **kwargs)

    @requires('categories', 'emiss_levels', 'species')
    def receive_area_emiss_by_species_category_and_level(self, data):
        self.z_var[:] = self.levels

        for row in data:
            spec_idx = self.species_lookup[row[3]]
            cat_idx = self.categories_lookup[row[4]]
            ts_idx = self.ts_lookup[row[5]]
            z_idx = self.levels.index(row[2])
            self.outvars[spec_idx][ts_idx, cat_idx, z_idx, row[1]-1, row[0]-1] =\
                (row[6] < 1e+36 and row[6] or self.undef)

    def receive_emiss_levels(self, levels):
        self.levels = levels
        log.debug('levels: ', self.levels)
        self.outfile.createDimension(self.names['z_dim'], len(self.levels))
        self.z_var = self.outfile.createVariable(self.names['z_var'], 'i4', (self.names['z_dim'], ))
        self.z_var[:] = self.levels

    def finalize(self):
        self.outfile.FILEDESC = 'PALM emissions created by FUME ' + self.cfg.run_params.output_params.output_description
        log.debug('PalmTotalAreaWriter finalize start')
        super().finalize()
        log.debug('PalmTotalAreaWriter finalize end')


class PALMAreaTimeWriter(NetCDFAreaTimeDisaggregator):
    """
    Postprocessor writing time disaggregated area emissions.
    Time-optimized version: time disaggregation performed with NetCDF
    files. Requires a prior run of NetCDFTotalAreaWriter!*
    """

    def setup(self, *args, **kwargs):
        """
        Run NetCDF setup with PALM-specific overrides:
        Dimensions names are X=x, Y=y, Z=z, T=time, nspecies, field_length
        projection attributes and close the file during finalize.
        """
        if 'filename' not in kwargs:
            if self.cfg.postproc.palmwriter.outfile_area:
                kwargs['filename'] = self.cfg.postproc.palmwriter.outfile_area
            else:
                log.error('Missing configuration parameter postproc.palmwriter.outfile_area!')

        if 'undef' not in kwargs and self.cfg.postproc.palmwriter.undef:
            kwargs['undef'] = float(self.cfg.postproc.palmwriter.undef)
            
        kwargs['no_create_t_dim'] = True
        kwargs['no_create_t_var'] = True
        kwargs['t_dim'] = 'time'
        kwargs['t_var'] = 'timestamp'
        kwargs['no_create_spec_vars'] = True
        kwargs['no_create_v_dim'] = True
        kwargs['v_dim'] = 'nspecies'
        kwargs['no_close_outfile'] =  True
        super().setup(*args, **kwargs)
        # PALM needs eission flows per m2, emission values are per grid
        # calculate conversion coefficient (1/gred_area)
        self.norm_coef = 1.0/(self.cfg.domain.delx*self.cfg.domain.dely)

        self.nchars_specname = 25

        # add E_UTM and N_UTM !!!

    def receive_molar_weight(self, molar_weight):
        self.molar_weight = molar_weight

    def receive_emiss_levels(self, levels):
        self.levels = levels

    @requires('categories')
    def receive_species(self, species):
        self.species = species
        self.species_lookup = {member[0]: idx for idx, member in enumerate(self.species)}
        self.create_emiss_file_struct()

    def receive_point_species(self, pspecies):
        self.pspecies = pspecies
        self.pspecies_lookup = {member[0]: idx for idx, member in enumerate(self.pspecies)}

    def receive_point_categories(self, pcategories):
        self.pcategories = pcategories
        self.pcategories_lookup = {member[0]: idx for idx, member in enumerate(self.pcategories)}

    def receive_stack_params(self, stacks):
        self.stack_params = stacks
        self.nstacks = self.stack_params.shape[0]
        if self.nstacks == 0:
            return
        self.stacks_id = list(map(int,self.stack_params[:,0]))

    @requires('categories','species','point_species','point_categories','time_shifts', 'molar_weight')
    def receive_point_emiss_ij(self, timestep, data):
        """
         - process point sources to gridded emission_values variable
           (PALM does not have implemented point sources so far)
         - all stack emission is put to level 0 as PALM ignores higher levels so far
         - TODO: very slow and inefficient, needs to rework in the future (when PALM point sources are available)
        """
        log.debug('data:', data.shape)
        if not np.any(data):
            return
        pemis = data.transpose(1, 0, 2, 3)
        time = self.rt_cfg['run']['datestimes'][timestep]
        log.debug('Time step point sources', timestep, time.replace(tzinfo=None))
        for pspec_idx, (spec_id, specname) in enumerate(self.pspecies):
            spec_idx = self.species_lookup[spec_id]
            for pcat_idx, (cat_id, catname) in enumerate(self.pcategories):
                # time shifts
                for ts_id in self.ts:
                    dtutc = self.time_shifts[(ts_id, time)]
                    tf = self.time_factors[dtutc]
                    try:
                        time_factor = float(tf[cat_id])
                    except KeyError:
                        continue
                    # write emisssion flux
                    # gas phase species needs to convert to weight units from molar for PALM!
                    # palm needs flux per m2 - normalize by 1/grid_area
                    self.emisvar[timestep, 0, :, :, spec_idx] += pemis[:, :, pcat_idx, pspec_idx] * time_factor * \
                                                                 self.molar_weight[(cat_id,spec_id)] * self.norm_coef

    def finalize(self):
        """
        Finalization steps
        ------------------
         - write temporary disagregated area emission from total file
         - close the file
        """
        log.debug('Fill out 2D area emission')
        # calculate and fill out temporarily disaggregated emission
        self.infile = Dataset(self.cfg.postproc.palmwriter.totalfile, "r")

        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
            log.debug('Time step area sources', time_idx, stepdt.replace(tzinfo=None))
            for spec_idx, (specid, specname) in enumerate(self.species):
                for ts_id in self.ts:
                    ts_idx = self.ts_lookup[ts_id]
                    for cat_idx, (catid, catname) in enumerate(self.categories):
                        dtutc = self.time_shifts[(ts_id, stepdt)]
                        tf = self.time_factors[dtutc]
                        try:
                            time_factor = float(tf[catid])
                        except KeyError:
                            continue

                        # write emisssion flux for base surface emissions (level = 0 -> z = 0)
                        # gas phase species needs to convert to weight units from molar for PALM!
                        # palm needs flux per m2 - normalize by 1/grid_area
                        # remark: self.infile.variables[specname] is indexed from 0 despite of dimension values!!
                        #         level index 0 thus means level value -1 (2d emission)
                        self.emisvar[time_idx, 0, :, :, spec_idx] += \
                            self.infile.variables[specname][ts_idx, cat_idx, 0, :, :].filled(fill_value=float(0.0)) * \
                            time_factor * self.molar_weight[(catid,specid)] * self.norm_coef

        # close files
        self.infile.close()
        self.outfile.close()

    def create_emiss_file_struct(self):
        """
         - create netcdf emission variables
         - save time data
         - save PALM-specific attributes
        """
        self.outfile.createDimension(self.names['t_dim'], len(self.rt_cfg['run']['datestimes']))
        self.outfile.createDimension('field_length', self.nchars_specname)
        self.outfile.createDimension('nspecies', len(self.species))

        self.timevar = self.outfile.createVariable(self.names['t_var'], 'c', (self.names['t_dim'],'field_length'), fill_value=chr(0))
        self.timevar.units = 'hours'
        self.timevar.long_name = 'hours since ' + self.rt_cfg['run']['datestimes'][0].strftime('%Y-%m-%d %H:%M')
        for dt in range(0, len(self.rt_cfg['run']['datestimes'])):
            # palm requires only 2-digit zone shift while python format %z is four digit
            ts = self.rt_cfg['run']['datestimes'][dt].strftime('%Y-%m-%d %H:%M:%S %z')[:-2]
            self.timevar[dt,:len(ts)] = ts

        self.emisid = self.outfile.createVariable('emission_index', 'i4', ('nspecies'))
        self.emisid.long_name = 'emission species index'
        self.emisid.standard_name = 'emission_index'
        self.emisid.units = ''
        self.emisid[:] = [x for x in self.species_lookup]

        self.emisname = self.outfile.createVariable('emission_name', 'c', ('nspecies', 'field_length'), fill_value=chr(0))
        self.emisname.long_name = 'emission species names'
        self.emisname.standard_name = 'emission_name'
        self.emisname.units = ''
        self.emisname[:,:] = chr(0)
        for specid in range(len(self.species)):
            self.emisname[specid, :len(self.species[specid][1])] = self.species[specid][1]  # TODO - enforce the correct order of the species!!!

        self.emisvar = self.outfile.createVariable('emission_values', 'f4', (self.names['t_dim'], 'z', 'y', 'x', 'nspecies'),
                            fill_value = self.cfg.postproc.palmwriter.undef)
        self.emisvar.missing_value = self.cfg.postproc.palmwriter.undef
        self.emisvar.lod = 2
        self.emisvar.units = 'g/m2/s'
        self.emisvar.long_name = 'emission values'
        self.emisvar.standard_name = 'emission_values'
        self.emisvar.coordinates = "E_UTM N_UTM lon lat"
        self.emisvar[:] = 0.0

        # Fill netcdf attributes according PIDS
        self.outfile.Conventions = "CF-1.7"
        self.outfile.origin_x = self.cfg.domain.xorg - self.cfg.domain.nx * self.cfg.domain.delx / 2.0
        self.outfile.origin_y = self.cfg.domain.yorg - self.cfg.domain.ny * self.cfg.domain.dely / 2.0
        #self.outfile.origin_z =
        # calculate lon,lat of the left bottom corner of the domain for output
        # IT DOES NOT WORK ON ARIEL (is it necessary?)
        #latlonproj = Proj(init='EPSG:4326')
        #outfileproj = Proj(init='EPGS:'+str(self.rt_cfg['projection_params']['srid']))
        #self.outfile.origin_lon, self.outfile.origin_lat = \
        #    transform(outfileproj, latlonproj, self.outfile.origin_x, self.outfile.origin_y)
        self.outfile.rotation_angle = 0.   # rotated grid not supported so far
        self.outfile.origin_time = self.cfg.run_params.time_params.dt_init.strftime("%Y-%m-%d %H:%M:%S")
        self.outfile.lod = 2               # lod = 2: pre-processed emission
        try:
            self.outfile.acronym = self.cfg.postproc.palmwriter.acronym
            self.outfile.author = self.cfg.postproc.palmwriter.author
            self.outfile.institution = self.cfg.postproc.palmwriter.institution
            self.outfile.palm_version = self.cfg.postproc.palmwriter.palm_version
        except Exception:
            log.debug('Some description attributes of the output file are missing.')
            log.debug('Check configuration parameters: acronym, author, institution, palm_version, and data_content')
        try:
            self.outfile.data_content = 'PALM emissions for case {} and domain {} created by FUME emission model'\
                                        .format(self.cfg.casename, self.cfg.domain.grid_name)
        except Exception as ex:
            log.debug('Check configuration parameters casename and grid_name.')
            log.debug(ex)

##########################################################


class PALMVsrcTimeWriter(NetCDFAreaTimeDisaggregator):
    """
    Postprocessor writing time disaggregated volume sources emissions.
    Time-optimized version: time disaggregation performed with NetCDF
    files. Requires a prior run of PalmTotalAreaWriter!*
    """

    def setup(self, *args, **kwargs):
        """
        Run NetCDF setup with PALM-specific overrides:
        Dimensions names are X=x, Y=y, Z=z, T=time, nspecies, field_length
        projection attributes and close the file during finalize.
        """
        if 'filename' not in kwargs:
            if self.cfg.postproc.palmwriter.outfile_vsrc:
                kwargs['filename'] = self.cfg.postproc.palmwriter.outfile_vsrc
            else:
                log.error('Missing configuration parameter postproc.palmwriter.outfile_vsrc!')

        if 'undef' not in kwargs and self.cfg.postproc.palmwriter.undef:
            kwargs['undef'] = float(self.cfg.postproc.palmwriter.undef)

        kwargs['no_create_t_dim'] = True
        kwargs['no_create_t_var'] = True
        kwargs['t_dim'] = 'ntime'
        kwargs['t_var'] = 'timestamp'
        kwargs['no_create_spec_vars'] = True
        kwargs['no_create_v_dim'] = True
        kwargs['v_dim'] = 'nspecies'
        kwargs['no_close_outfile'] =  True
        super().setup(*args, **kwargs)
        # PALM needs eission flows per m2, emission values are per grid
        # calculate conversion coefficient (1/gred_area)
        self.norm_coef = 1.0/(self.cfg.domain.delx*self.cfg.domain.dely*self.cfg.domain.delz)

        self.nchars_specname = 64

        # FIXME: add E_UTM and N_UTM !!!

    def receive_molar_weight(self, molar_weight):
        self.molar_weight = molar_weight

    def receive_emiss_levels(self, levels):
        self.levels = levels

    @requires('species','molar_weight')
    def receive_number_area_volume_sources(self, nvsrc):
        '''
        recieves number of volume sources from provider
        and creates volume sources netcdf structure (dimension, variables)
        '''
        self.nvsrc = nvsrc
        log.debug('nvsrc:', nvsrc)
        if nvsrc > 0:
            # create netcdf structure for 3d volume sources
            default_index = -1   # ijk index
            default_value = 0.0  # volume source value

            self.outfile.createDimension('nvsrc', self.nvsrc)
            self.vsrc_i = self.outfile.createVariable('vsrc_i', 'i4', ('nvsrc',), fill_value=default_index)
            self.vsrc_j = self.outfile.createVariable('vsrc_j', 'i4', ('nvsrc',), fill_value=default_index)
            self.vsrc_k = self.outfile.createVariable('vsrc_k', 'i4', ('nvsrc',), fill_value=default_index)
            self.vsrc_value = [None] * len(self.species_lookup)
            for ispec in range(len(self.species_lookup)):
                log.debug('species[ispec]', ispec, self.species[ispec])
                vname = 'vsrc_' + self.species[ispec][1]
                log.debug('createVariable: ' + vname)
                self.vsrc_value[ispec] = self.outfile.createVariable(vname, 'f4', (self.names['t_dim'], 'nvsrc', ), \
                                                                     fill_value=float(self.cfg.postproc.palmwriter.undef))
                self.vsrc_value[ispec].missing_value = float(self.cfg.postproc.palmwriter.undef)
                self.vsrc_value[ispec].lod = 2
                self.vsrc_value[ispec].units = 'kg/m3/s or mol/m3/s'
                self.vsrc_value[ispec].long_name = 'volume emission values ' + self.species[ispec][1]
                self.vsrc_value[ispec].standard_name = vname
                self.vsrc_value[ispec][:] = 0.0
                log.debug('Variable ' + vname + ' created')

    @requires('categories')
    def receive_species(self, species):
        self.species = species
        self.species_lookup = {member[0]: idx for idx, member in enumerate(self.species)}
        self.create_emiss_file_struct()

    def finalize(self):
        """
        Finalization steps
        ------------------
         - write temporary disagregated vsrc emission from total file
         - close the file
        """

        # if number of vsrc > 0, write emisssion flux for lower levels (level>0)
        # emission into 3d palm emiss structure
        if self.nvsrc > 0:
            # calculate and fill out temporarily disaggregated emission
            # open total file
            self.infile = Dataset(self.cfg.postproc.palmwriter.totalfile, "r")
            # open necessary palm static driver to obtain the vertical structure of the buildings
            self.static_driver = Dataset(self.cfg.postproc.palmwriter.static_driver, "r")
            # check dimensions
            if (self.infile.dimensions['x'].size != self.static_driver.dimensions['x'].size or
                self.infile.dimensions['y'].size != self.static_driver.dimensions['y'].size):
                log.error("Dimensions of emission domain and static driver domain dows not match. Exit.....")
                raise IOError

            # check needed variables
            if list(self.static_driver.variables.keys()).index('buildings_3d') < 0:
                log.error("Static driver does not contain variable buildings_3d. Exit.....")
                raise IOError

            if list(self.static_driver.variables.keys()).index('zt') < 0:
                log.error("Static driver does not contain variable zt. Exit.....")
                raise IOError

            # retrieve buildings_3d and zt data
            b3d = self.static_driver.variables['buildings_3d'][:].data
            #zt  = self.static_driver.variables['zt'][:].data
            # test delz config parameter
            if hasattr(self.cfg.domain, 'delz') and self.cfg.domain.delz != 0:
                self.delz = self.cfg.domain.delz
            else:  # it does not exist
                self.delz = self.cfg.domain.delx
            # get list of non-zero value locations
            dshape = list(self.infile.dimensions[dname].size for dname in 'level y x'.split())
            # eliminate level -1 used for 2D emission
            startlevel = 1  # level[0] has value -1. This represens 2D emission which needs to be eliminated here
            dshape[0] = dshape[0] - 1
            log.debug('startlevel, dshape: ', startlevel, dshape)
            has_data = np.ones(dshape, dtype=bool)
            for specid, specname in self.species:
                v = self.infile.variables[specname][:, :, startlevel:, :, :]
                vmask = v.mask.min(axis=(0,1))
                has_data &= vmask
            # invert values and transpose coordinates to x, y, level
            has_data = ~has_data
            # list coordinates of all filled values
            ivsrc = 0
            for level, y, x in zip(*has_data.nonzero()):
                self.vsrc_i[ivsrc] = x
                self.vsrc_j[ivsrc] = y
                # locate k index for given level
                # retrieve building_3d column
                b = b3d[:, y, x]
                # locate k index for level = 0
                ka = np.where(b == 1)[0]
                if ka.size == 0:
                    if level == 0:
                        k = 0
                    else:
                        log.fmt_error('No building found in x,y,level = {x}, {y}, {level}. Skipping.', x=x, y=y, level=level)
                        continue
                else:
                    k = ka.max()
                if level > 0:
                    # locate k for level>0
                    b = b[:k+1]
                    found = False
                    for l in range(1,level+1):  # levels from 1 to level
                        # next top of air
                        ka = np.where(b == 0)[0]
                        if ka.size == 0:
                            log.fmt_error('Only level {l} (building) found in x,y,level = {x}, {y}, {level}. Skipping.', l=l, x=x, y=y, level=level)
                            continue

                        k = ka.max()
                        b = b[:k+1]
                        # next top of building
                        ka = np.where(b == 1)[0]
                        if ka.size == 0:
                            if l < level:
                                log.fmt_error('Only level {l} (ground) found in x,y,level = {x}, {y}, {level}. Skipping.', l=l, x=x, y=y, level=level)
                            else:
                                # reached ground
                                k = 0
                                found = True
                        else:
                            # shrink to next top of the building
                            k = ka.max()
                            b = b[:k+1]
                    if not found:
                        # k for level not found, do not process point
                        continue

                # locate terrain top heigh and k dimension
                # !!! check with PALM procedure of terrain gridding !!!
                self.vsrc_k[ivsrc] = k + 1  # this will place volume source at the first grid above the ground in level
                level_idx = level + startlevel
                log.fmt_debug('Found vsrc_k = {vsrc_k} for x,y,level,ivsrc = {x}, {y}, {level}, {ivsrc}.', \
                              vsrc_k=self.vsrc_k[ivsrc], x=x, y=y, level=level, ivsrc=ivsrc)
                for spec_idx, (specid, specname) in enumerate(self.species):
                    invar = self.infile.variables[specname][:, :, level_idx, y, x]
                    for cat_idx, (catid, catname) in enumerate(self.categories):
                        # !!!HACK!!! PALM vsrc needs units in mol/m3/s for gases and kg/m3/s for PM
                        # transform PM from g to kg, species in mol units leave unchanged
                        # distinguish between gasses and PM by (molar weight == 1)
                        # In future, the model-mechanism units needs to be added
                        # into the mechanism configuration for every specie
                        # !!!HACK!!!
                        unit_fact = 1e-3 if (self.molar_weight[(catid,specid)]==1) else 1
                        for ts_id in self.ts:
                            ts_idx = self.ts_lookup[ts_id]
                            v = invar[ts_idx, cat_idx]
                            if np.ma.is_masked(v):
                                continue
                            for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
                                dtutc = self.time_shifts[(ts_id, stepdt)]
                                tf = self.time_factors[dtutc]
                                try:
                                    time_factor = float(tf[catid])
                                except KeyError:
                                    continue

                                self.vsrc_value[spec_idx][time_idx, ivsrc] += v * time_factor * self.norm_coef * unit_fact

                # increase vsrc counter
                ivsrc += 1
            # close infile
            self.infile.close()

        # close out file
        self.outfile.close()

    def create_emiss_file_struct(self):
        """
         - create netcdf emission variables
         - save time data
         - save PALM-specific attributes
        """
        self.outfile.createDimension(self.names['t_dim'], len(self.rt_cfg['run']['datestimes']))
        self.outfile.createDimension('field_length', self.nchars_specname)
        self.outfile.createDimension('nspecies', len(self.species))

        self.timevar = self.outfile.createVariable(self.names['t_var'], 'c', (self.names['t_dim'],'field_length'), fill_value=chr(0))
        self.timevar.units = 'hours'
        self.timevar.long_name = 'hours since ' + self.rt_cfg['run']['datestimes'][0].strftime('%Y-%m-%d %H:%M')
        for dt in range(0, len(self.rt_cfg['run']['datestimes'])):
            # palm requires only 2-digit zone shift while python format %z is four digit
            ts = self.rt_cfg['run']['datestimes'][dt].strftime('%Y-%m-%d %H:%M:%S %z')[:-2]
            self.timevar[dt,:len(ts)] = ts

        self.emisname = self.outfile.createVariable('species', 'c', ('nspecies', 'field_length'), fill_value=chr(0))
        self.emisname.long_name = 'emission species names'
        self.emisname.standard_name = 'species'
        self.emisname.units = ''
        self.emisname[:,:] = chr(0)
        for specid in range(len(self.species)):
            self.emisname[specid, :len(self.species[specid][1])] = self.species[specid][1]  # TODO - enforce the correct order of the species!!!

        # Fill netcdf attributes according PIDS
        self.outfile.Conventions = "CF-1.7"
        self.outfile.origin_x = self.cfg.domain.xorg - self.cfg.domain.nx * self.cfg.domain.delx / 2.0
        self.outfile.origin_y = self.cfg.domain.yorg - self.cfg.domain.ny * self.cfg.domain.dely / 2.0
        #self.outfile.origin_z =
        # calculate lon,lat of the left bottom corner of the domain for output
        # IT DOES NOT WORK ON ARIEL (is it necessary?)
        #latlonproj = Proj(init='EPSG:4326')
        #outfileproj = Proj(init='EPGS:'+str(self.rt_cfg['projection_params']['srid']))
        #self.outfile.origin_lon, self.outfile.origin_lat = \
        #    transform(outfileproj, latlonproj, self.outfile.origin_x, self.outfile.origin_y)
        self.outfile.rotation_angle = 0.   # rotated grid not supported so far
        self.outfile.origin_time = self.cfg.run_params.time_params.dt_init.strftime("%Y-%m-%d %H:%M:%S")
        self.outfile.lod = 2               # lod = 2: pre-processed emission
        try:
            self.outfile.acronym = self.cfg.postproc.palmwriter.acronym
            self.outfile.author = self.cfg.postproc.palmwriter.author
            self.outfile.institution = self.cfg.postproc.palmwriter.institution
            self.outfile.palm_version = self.cfg.postproc.palmwriter.palm_version
        except Exception:
            log.debug('Some description attributes of the output file are missing.')
            log.debug('Check configuration parameters: acronym, author, institution, palm_version, and data_content')
        try:
            self.outfile.data_content = 'PALM emissions for case {} and domain {} created by FUME emission model'\
                                        .format(self.cfg.casename, self.cfg.domain.grid_name)
        except Exception as ex:
            log.debug('Check configuration parameters casename and grid_name.')
            log.debug(ex)
