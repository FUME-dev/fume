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

Copyright 2014-2026 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
Copyright 2014-2026 Charles University, Faculty of Mathematics and Physics, Prague, Czech Republic
Copyright 2014-2026 Czech Hydrometeorological Institute, Prague, Czech Republic
Copyright 2014-2017 Czech Technical University in Prague, Czech Republic
"""

import math
import numpy as np
from collections import namedtuple
from netCDF4 import Dataset
from met.ep_plumerise import get_plume_frac
from  met.ep_met_netcdf import read_netcdf_timestep
from postproc.receiver import requires
from postproc.netcdf import NetCDFAreaTimeDisaggregator, NetCDFTotalWriter
from postproc.dissolving import dissolve, DissolvingOrder
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_rtcfg
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)

na_ = np.newaxis

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

    @requires('categories', 'emission_levels', 'species')
    def receive_area_emiss_by_species_category_and_level(self, data):
        self.z_var[:] = self.levels

        for row in data:
            spec_idx = self.species_lookup[row[3]]
            cat_idx = self.categories_lookup[row[4]]
            ts_idx = self.ts_lookup[row[5]]
            z_idx = self.levels.index(row[2])
            self.outvars[spec_idx][ts_idx, cat_idx, z_idx, row[1]-1, row[0]-1] =\
                (row[6] < 1e+36 and row[6] or self.undef)

    def receive_emission_levels(self, levels):
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
        kwargs['no_create_z_dim'] = True
        kwargs['no_close_outfile'] =  True
        super().setup(*args, **kwargs)
        # z dimension has to be present but is always 1
        self.outfile.createDimension(self.names['z_dim'], 1)
        # PALM needs eission flows per m2, emission values are per grid
        # calculate conversion coefficient (1/gred_area)
        self.norm_coef = 1.0/(self.rt_cfg['domain']['delx']*self.rt_cfg['domain']['dely'])
        self.nchars_specname = 25

        # add E_UTM and N_UTM !!!

    def receive_molar_weight(self, molar_weight):
        self.molar_weight = molar_weight

    def receive_emission_levels(self, levels):
        self.levels = levels

    @requires('categories')
    def receive_species(self, species):
        self.species = species
        self.species_lookup = {member[0]: idx for idx, member in enumerate(self.species)}
        self.create_2d_emiss_file_struct()

    '''
    def receive_point_species(self, pspecies):
        self.pspecies = pspecies

    def receive_point_categories(self, pcategories):
        self.pcategories = pcategories
        self.pcategories_lookup = {member[0]: idx for idx, member in enumerate(self.pcategories)}

    @requires('categories','species','point_species','point_categories','time_shifts', 'molar_weight')
    def receive_point_emiss_ij(self, timestep, cat_id, data):
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
            pcat_idx = self.pcategories_lookup[cat_id]
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
    '''

    def finalize(self):
        """
        Finalization steps
        ------------------
         - write temporary disagregated area emission from total file
         - close the file
        """
        log.debug('Fill out 2D area emission')
        # calculate and fill out temporarily disaggregated emission
        # for 2D emission (level=-1, level dimension in total file = 0)
        self.infile = Dataset(self.cfg.postproc.palmwriter.totalfile, "r")

        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
            log.debug('Time step area sources', time_idx, stepdt.replace(tzinfo=None))
            for spec_idx, (specid, specname) in enumerate(self.species):
                for ts_id in self.ts:
                    ts_idx = self.ts_lookup[ts_id]
                    for cat_idx, (catid, catname) in enumerate(self.categories):
                        try:
                            self.molar_weight[(catid, specid)]
                        except:
                            # combination cat and specie does not have emission source
                            # e.g. it does not have speciation in gspro
                            continue
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

    def create_2d_emiss_file_struct(self):
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
        # initialize by zero by individual timestes due to memory limits
        for it in range(len(self.rt_cfg['run']['datestimes'])):
            self.emisvar[it,:,:,:,:] = 0.0

        # Fill netcdf attributes according PIDS
        self.outfile.Conventions = "CF-1.7"
        self.outfile.origin_x = self.rt_cfg['domain']['xorg'] - self.rt_cfg['domain']['nx'] * self.rt_cfg['domain']['delx'] / 2.0
        self.outfile.origin_y = self.rt_cfg['domain']['yorg'] - self.rt_cfg['domain']['ny'] * self.rt_cfg['domain']['dely'] / 2.0
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

PointVsrcEmis = namedtuple('PointVsrcEmis', 'i j level spec_id cat_id ts_id height diameter '
        'temperature velocity emiss')

class VsrcMap:
    """Class for mapping between z,y,x coordinates and ivsrc for volume sources"""

    def __init__(self, i, j, k, datavars):
        self.nvsrc = 0
        self.xvar = i
        self.yvar = j
        self.zvar = k
        self.datavars = datavars

        # Prepare indexing
        self._vsrc_new_indices = []
        self._map = {}

    def _get(self, key):
        """Finds item from map or inserts a new key in one operation.

        Processing newly added records is postponed.
        """
        v = self._map.setdefault(key, self.nvsrc)
        if v == self.nvsrc:
            self._vsrc_new_indices.append(key)
            self.nvsrc += 1
        return v

    def assign_by_yxmask(self, yxmask, z):
        """Finds assignment indices for vsrc in 2D, expanding nvsrc where necessary."""

        assign_indices = [self._get((z[y,x],y,x))
                for y,x in np.argwhere(yxmask)]
        if self._vsrc_new_indices:
            self._process_new_coords()

        return np.array(assign_indices, dtype=int)

    def assign_by_zyxmask(self, zbase, ybase, xbase, zyxmask):
        """Finds assignment indices for vsrc in 3D, expanding nvsrc where necessary."""

        assign_indices = [self._get((zbase+z,ybase+y,xbase+x))
                for z,y,x in np.argwhere(zyxmask)]
        if self._vsrc_new_indices:
            self._process_new_coords()

        return np.array(assign_indices, dtype=int)

    def __getitem__(self, key):
        ivsrc = self._get(key)
        if self._vsrc_new_indices:
            self._process_new_coords()

        return ivsrc

    def _process_new_coords(self):
        """Fills variable values for newly created coords"""

        nnew = len(self._vsrc_new_indices)
        log.fmt_debug('Added {} new vsrc coordinates.', nnew)

        # Assign z, y, x coordinate variables
        ifrom = self.nvsrc - nnew
        zc, yc, xc = zip(*self._vsrc_new_indices)
        self.zvar[ifrom:self.nvsrc] = zc
        self.yvar[ifrom:self.nvsrc] = yc
        self.xvar[ifrom:self.nvsrc] = xc
        self._vsrc_new_indices[:] = []

        # Add zeros to data variables
        for dv in self.datavars:
            dv[:, ifrom:self.nvsrc] = 0.

class PALMVsrcTimeWriter(NetCDFAreaTimeDisaggregator):
    """
    Postprocessor writing time disaggregated volume sources emissions.
    Time-optimized version: time disaggregation performed with NetCDF
    files. Requires a prior run of PalmTotalAreaWriter!*
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.point_vsrc_emiss = []

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

        # PALM needs emission volume flows per m3, emission values are per grid
        # calculate conversion coefficient (1/grid_volume)
        self.norm_coef = 1.0/(self.rt_cfg['domain']['delx']*self.rt_cfg['domain']['dely']*self.cfg.domain.delz)

        # VSRC time writer needs detailed information from PALM static driver
        self.read_static_driver()

        self.nchars_specname = 64

        # FIXME: add E_UTM and N_UTM !!!

        # In case of dissolving, check if meteorology is available
        if ep_cfg.postproc.palmwriter.plumerise:
            if 'met_ts_mapping' not in ep_rtcfg:
                log.error('Plumerise requires meteorology - enable case.collect_meteorology in the workflow!')
                raise ValueError

        # Load dissolving order from config
        if ep_cfg.postproc.palmwriter.dissolving.area:
            self.dissolve_area = DissolvingOrder(ep_cfg.postproc.palmwriter.dissolving.area_order)
        else:
            self.dissolve_area = None

        if ep_cfg.postproc.palmwriter.dissolving.point:
            self.dissolve_point = DissolvingOrder(ep_cfg.postproc.palmwriter.dissolving.point_order)
        else:
            self.dissolve_point = None

    def read_static_driver(self):
        # check existence of the b3d to avoid to run method multiple time
        try:
            self.b3d
            return
        except:
            pass
        # open necessary palm static driver to obtain the vertical structure of the buildings
        self.static_driver = Dataset(self.cfg.postproc.palmwriter.static_driver, "r")
        try:
            # check dimensions of static driver and prescribed PALM domain
            if (ep_cfg.domain.nx != self.static_driver.dimensions['x'].size or
                ep_cfg.domain.ny != self.static_driver.dimensions['y'].size):
                log.error("Dimensions of emission domain and static driver domain dows not match. Exit.....")
                log.fmt_error("Domain x,y: {}, {}, static driver x,y: {}, {}",
                              ep_cfg.domain.nx, ep_cfg.domain.ny,
                              self.static_driver.dimensions['x'].size, self.static_driver.dimensions['y'].size)
                raise IOError

            # check needed variables
            if not ('buildings_2d' in self.static_driver.variables.keys() or \
                    'buildings_3d' in self.static_driver.variables.keys()):
                log.error("Static driver does not contain variable buildings_3d or buildings_2d. Exit.....")
                raise IOError

            if list(self.static_driver.variables.keys()).index('zt') < 0:
                log.error("Static driver does not contain variable zt. Exit.....")
                raise IOError

            # retrieve zt and nt data (terrain height in m and number of terrain layers)
            self.zt = self.static_driver.variables['zt'][:].data

            # retrieve buildings_3d
            try:
                self.b3d = self.static_driver.variables['buildings_3d'][:].filled(0).astype(bool)
            except:
                # build b3d from b2d
                b2d = self.static_driver.variables['buildings_2d'][:].filled(0)
                maxh = b2d.max()
                # PALM requires not stretched vertical levels inside the urban canopy
                # The b3d array can be calculated just from dz
                maxk = math.floor(maxh / ep_rtcfg.delz) + 1
                self.b3d = np.zeros((maxk, b2d.shape[0], b2d.shape[1]), dtype=bool)
                for i in range(np.shape(b2d)[1]):
                    for j in range(np.shape(b2d)[0]):
                        if b2d[j, i] >= ep_rtcfg.delz * 0.5:
                            self.b3d[0:math.floor(b2d[j, i] / ep_rtcfg.delz + 0.5) + 1, j, i] = True
        finally:
            self.static_driver.close()

    def receive_molar_weight(self, molar_weight):
        self.molar_weight = molar_weight
        log.debug('Molar_weight:', self.molar_weight)

    def receive_specie_type(self, specie_type):
        self.specie_type = specie_type
        log.debug('Specie_type:', self.specie_type)

    def receive_emission_levels(self, levels):
        self.levels = levels

    @requires('categories', 'specie_type')
    def receive_species(self, species):
        self.species = species
        self.species_lookup = {member[0]: idx for idx, member in enumerate(self.species)}

    @requires('categories', 'species', 'emission_levels', 'specie_type')
    def receive_point_vsrc_by_species_category_and_level(self, data):
        # receive point_vsrc_emiss
        log.debug('Receive point_vsrc_emiss rows')
        for row in data:
            # i, j, level, spec_id, cat_id, ts_id, height, diameter, temperature, velocity, emiss
            pvsrc = PointVsrcEmis(*row)
            self.point_vsrc_emiss.append(pvsrc)

    def yield_time_factors(self, ts_id, catid):
        """Yield (time_idx, time_factor) for each timestep."""

        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
            # calculate local time shift
            dtutc = self.time_shifts[(ts_id, stepdt)]
            tf = self.time_factors[dtutc]
            try:
                yield time_idx, float(tf[catid])
            except KeyError:
                continue

    def process_palm_emissions_from_totals(self, ftotal):
        """Reads total emissions from ftotal and writes them into vsrc"""

        # inicialize full 3d levels if not already done
        self.init_full3d_levels(ftotal)

        # Determine vertical extent of the surface area emissions
        if self.dissolve_area:
            zlev_start = self.level_kindex.min()
            zlev_stop = self.level_kindex.max()
            log.fmt_debug('Vertical extent of area emissions is between k={} and k={}.',
                          zlev_start, zlev_stop)
            zlev_stop += 1

            surf3d_shape = (len(self.rt_cfg['run']['datestimes']), zlev_stop-zlev_start,
                            ep_cfg.domain.ny, ep_cfg.domain.nx)
            surf3d_iy, surf3d_ix = np.mgrid[0:ep_cfg.domain.ny,0:ep_cfg.domain.nx]

            # Prepare hard mask based on buildings_3d
            hardmask = np.logical_not(self.b3d[zlev_start:zlev_stop,:,:])

        # Process emissions from the totals file
        for spec_idx, (specid, specname) in enumerate(self.species):
            invar = ftotal.variables[specname]
            outvar = self.vsrc_value[spec_idx]

            # PALM vsrc needs units in mol/m3/s for gases and kg/m3/s for PM
            # transform PM from g to kg, species in mol units leave unchanged
            # distinguish between gasses and PM by (molar weight == 1)
            # In future, the model-mechanism units needs to be added
            # into the mechanism configuration for every specie
            unit_fact = 1e-3 if (self.specie_type[specid]==1) else 1

            if self.dissolve_area:
                emis = np.zeros(surf3d_shape, dtype=invar.dtype)

            for ts_id in self.ts:
                ts_idx = self.ts_lookup[ts_id]

                for cat_idx, (catid, catname) in enumerate(self.categories):
                    for level in range(self.nlev):
                        # level[0] has value -1. This represens 2D emission which needs to be eliminated here
                        v = invar[ts_idx, cat_idx, level+1, :, :]
                        yxmask = ~v.mask
                        if not yxmask.any():
                            continue

                        # Locate z coordinates for the given level
                        z = self.level_kindex[level,:,:]
                        num_missing = (yxmask & self.missing_level[level,:,:]).sum()
                        if num_missing:
                            log.fmt_warning('Missing level {} has been replaced by higher level for {} points.',
                                    level, num_missing)

                        if self.dissolve_area:
                            # Just gather the emissions into the 3D array
                            z = z - zlev_start

                            # Process output timesteps
                            for time_idx, time_factor in self.yield_time_factors(ts_id, catid):
                                # Direct assignment is not supported with double indexing
                                newval = emis[time_idx, z, surf3d_iy, surf3d_ix]
                                newval[yxmask] += v[yxmask] * (time_factor * self.norm_coef * unit_fact)
                                emis[time_idx, z, surf3d_iy, surf3d_ix] = newval

                        else:
                            # Add emissions directly to the output file

                            # Find vsrc indices (potentially expanding)
                            ivsrc = self.vsrc_map.assign_by_yxmask(yxmask, z)
                            log.fmt_debug('Adding {} points for {}, ts={}, cat={}, lev={}.',
                                    len(ivsrc), specname, ts_id, catname, level)

                            # Process output timesteps
                            for time_idx, time_factor in self.yield_time_factors(ts_id, catid):
                                outvar[time_idx, ivsrc] += v[yxmask] * (time_factor * self.norm_coef * unit_fact)

            if self.dissolve_area:
                for time_idx in range(len(self.rt_cfg['run']['datestimes'])):
                    log.fmt_debug('Dissolving {} at time {}.', specname, time_idx)
                    for axis, nplus, nminus in self.dissolve_area.steps:
                        dissolve(emis[time_idx,:,:,:], hardmask, axis, nplus, nminus)

                emin = emis.min()
                if emin < 0.:
                    badpt = (emis<0.)
                    log.fmt_warning('Skipping {} cells with negative emissions of {}, min={}, sum={}.',
                                    badpt.sum(), specname, emin, emis[badpt].sum())

                has_emis = (emis > 0.).any(axis=0)
                if has_emis.any():
                    # Find vsrc indices (potentially expanding)
                    ivsrc = self.vsrc_map.assign_by_zyxmask(zlev_start, 0, 0, has_emis)
                    log.fmt_debug('Adding {} points for {}.', len(ivsrc), specname)

                    # Process output timesteps
                    for time_idx in range(len(self.rt_cfg['run']['datestimes'])):
                        outvar[time_idx, ivsrc] += emis[time_idx,:,:,:][has_emis]

    def process_palm_point_sources(self, ftotal):
        """Adds emissions from point sources to output file"""

        # inicialize full 3d levels if not already done
        self.init_full3d_levels(ftotal)

        # TODO: solve problem of the layer shift in case of stretching
        '''
        # transformation meteo values to over terrain levels
        is_above = ep_rtcfg.model_levels[:,na_,na_] > self.zt[na_,:,:]
        nt = np.argmax(is_above, axis=0)
        nlays = ep_cfg.domain.nz - nt
        nlays_max = nlays.max()
        kcoord = np.empty((nlays_max, ep_cfg.domain.ny, ep_cfg.domain.nx), dtype=int)
        kcoord[:,:,:] = np.arange(nlays_max)[:,na_,na_]
        kcoord[:,:,:] += nt[na_,:,:]
        kcoord = np.minimum(kcoord, ep_cfg.domain.nz-1)
        t_levels = ep_rtcfg.model_levels[:,na_,na_][kcoord,0,0]
        t_delz = ep_rtcfg.model_delz[:, na_, na_][kcoord, 0, 0]
        hghts = t_levels - ep_rtcfg.model_levels[nt] + t_delz * 0.5
        '''

        self.norm_coefs = 1.0 / (self.rt_cfg['domain']['delx'] * self.rt_cfg['domain']['dely'] * ep_rtcfg.model_delz)
        nlays = ep_cfg.domain.nz
        hghts = ep_rtcfg.model_levels_stag
        pvsrc_relocation = {}
        first = True
        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):

            log.fmt_debug('Processing point src at time step {} for time {}.',time_idx, stepdt)

            if ep_cfg.postproc.palmwriter.plumerise:
                met_data = read_netcdf_timestep(time_idx)
                if first:
                    # get mapping of the meteorological variables
                    metvar = {}
                    for i, v in enumerate(met_data):
                        metvar[v.name] = i

            for pvsrc in self.point_vsrc_emiss:
                # i,j start from 1 in database and from 0 in netcdf
                i = pvsrc.i-1
                j = pvsrc.j-1
                # calculate height of the stack from domain bottom
                height = pvsrc.height + self.zt[j,i]
                spec_idx = self.species_lookup[pvsrc.spec_id]
                unit_fact = 1e-3 if (self.specie_type[pvsrc.spec_id] == 1) else 1
                # kh - k coordinate of the source from domain bottom based on original source height
                kh = math.floor(height/self.cfg.domain.delz) + 1
                # kh - k coordinate of terrain from domain bottom
                kt = math.floor(self.zt[j,i] / self.cfg.domain.delz)
                # kht - k coordinate of the source from terrain
                kht = kh - kt

                log.fmt_debug('Processing point src at {},{} with height {} for specie {}.',
                               i, j, pvsrc.height, pvsrc.spec_id)

                if not ep_cfg.postproc.palmwriter.plumerise:
                    # locate k index for given level
                    k = self.level_kindex[pvsrc.level, j, i]
                    # calculate k according height of point source
                    # ensure it is higher than building
                    k = max(kh, k)

                    # Obtain vsrc index (expanding if necessary)
                    ivsrc = self.vsrc_map[k,j,i]

                # calculate local time shift
                dtutc = self.time_shifts[(pvsrc.ts_id, stepdt)]
                tf = self.time_factors[dtutc]
                time_factor = float(tf[pvsrc.cat_id])

                if ep_cfg.postproc.palmwriter.plumerise:
                    try:
                        newloc = pvsrc_relocation[kht,j,i]
                    except KeyError:
                        # Check whether the pvsrc postition is not blocked by
                        # a building
                        if self.b3d[kht,j,i]:
                            # blocked by a building, need to find the nearest free location.
                            newloc = get_nearest_free(self.b3d, (kht, j, i))
                            log.fmt_warning('Point source at {} blocked by a building, had to be moved to {}.',
                                    (kht, j, i), newloc)
                            pvsrc_relocation[kht,j,i] = newloc
                        else:
                            pvsrc_relocation[kht,j,i] = newloc = None

                    if newloc is not None:
                        kht, j, i = newloc
                        # Calculate terrain height in the new location
                        kt = math.floor(self.zt[j,i] / self.cfg.domain.delz)
                        kh_old = kh
                        kh = kht + kt
                        # When location is shifted, we may have to adjust height for the plumerise
                        if kh != kh_old:
                            height = (kh - 0.5) * self.cfg.domain.delz

                    # calculate plume: 'ta', 'pa', 'wndspd', 'zf'
                    k0, pfract = get_plume_frac(nlays,hghts,
                                    met_data[metvar['ta']].data[i,j,:],
                                    met_data[metvar['pa']].data[i,j,:],
                                    met_data[metvar['wndspd']].data[i,j,:],
                                    height,pvsrc.diameter,pvsrc.temperature,pvsrc.velocity)
                    k0 = max(k0, kt) # eliminate obscure cases where pvsrc.height==0 and k0 can be under the terrain
                    kn = k0 + len(pfract)
                    k0t = k0 - kt
                    knt = kn - kt

                    # Eliminate cells blocked by building
                    kn_b3d = min(knt, self.b3d[1:,j,i].shape[0])  # common top for plume and the b3d column (from buildings_3d array)
                    nfrac_b3d = max(0, kn_b3d - k0t)  # cells in the column that are within both the plume AND the b3d
                    if nfrac_b3d > 0:
                        pfract[:nfrac_b3d][self.b3d[k0t+1:kn_b3d+1,j,i]] = 0.   # b3d has significant fileds indexed from 1
                        # re-normalization of the pfrac array
                        pfrac_sum = pfract.sum()
                        if pfrac_sum == 0.:
                            # whole plume is blocked by a building, this should not happen thanks to relocation in the palm procedure
                            log.fmt_warning('Plume is fully blocked by buildings, avoiding plumrise.' )
                            log.fmt_warning('i,j,kht,k0t,knt: {}, {}, {}, {}, {}.',i,j,kht,k0t,knt )
                            log.fmt_warning('Pfract: {}.', pfract)
                            #raise RuntimeError('Plume is fully blocked by buildings')
                            # set plume to the original point
                            pfract = [1.0]
                            k0 = kh
                            k0t = kht
                            kn = k0 + 1
                            knt = k0t + 1
                        else:
                            pfract /= pfrac_sum

                    # Emission values in plume (using fractions)
                    emis = pvsrc.emiss * time_factor * unit_fact * self.norm_coefs[k0:kn] * pfract[:]
                    emis = emis[:,na_,na_]

                    if self.dissolve_point:
                        # Expand emission array for dissolving
                        dis = self.dissolve_point
                        emis = dis.expand(emis, (k0, j, i))

                        # Prepare expanded hardmask (by buildings)
                        hardmask = np.ones(emis.shape, dtype=bool)
                        # b3d may not reach high enough
                        bz0 = min(dis.z0, self.b3d.shape[0])
                        bz1 = min(dis.z1, self.b3d.shape[0])
                        nbz = bz1 - bz0
                        if nbz:
                            hardmask[-nbz:,:,:] &= np.logical_not(self.b3d[bz0:bz1,dis.y0:dis.y1,dis.x0:dis.x1])

                        # Perform dissolving
                        log.fmt_tracing('Dissolving point src at {} for {}, size {}, at time {}.',
                                (k0t, j, i), pvsrc.spec_id, emis.shape, time_idx)
                        for axis, nplus, nminus in self.dissolve_point.steps:
                            dissolve(emis, hardmask, axis, nplus, nminus)

                        ke, je, ie = dis.z0, dis.y0, dis.x0

                        emin = emis.min()
                        if emin < 0.:
                            # re-normalize positive values to allow eliminate the negative values
                            ec = emis.sum() / emis[emis>0.].sum()
                            emis *= ec
                            log.fmt_warning('Skipping {} cells with negative emissions, ' +
                                            're-normalize positive values to original emission with coeficient {}.',
                                            (emis<0.).sum(), ec)
                            #badpt = (emis<0.)
                            #log.fmt_warning('Skipping {} cells with negative emissions of {}, min={}, sum={}.',
                            #                badpt.sum(), pvsrc.spec_id, emin, emis[badpt].sum())

                    else:
                        # Postion of emission has not changed
                        ke, je, ie = k0, j, i

                    mask = emis > 0.

                    # Obtain vsrc indices (expanding file if necessary)
                    vsrc_idx = self.vsrc_map.assign_by_zyxmask(ke, je, ie, mask)
                    if vsrc_idx.size > 0:
                        # Add to emiss file
                        self.vsrc_value[spec_idx][time_idx,vsrc_idx] += emis[mask]

                else:
                    self.vsrc_value[spec_idx][time_idx,ivsrc] += pvsrc.emiss * time_factor * self.norm_coefs[k] * unit_fact

    def finalize(self):
        """
        Finalization steps
        ------------------
         - write temporary disagregated vsrc emission from total file
         - write vsrc point emission
         - close the file
        """

        # if number of vsrc > 0, write emisssion flux for vsrc levels (level>=0)
        # emission into 3d palm emiss structure
        try:
            log.info('Initializing PALM outfile structure and mapping')
            self.create_vsrc_emiss_file_struct()
            self.vsrc_map = VsrcMap(self.vsrc_i, self.vsrc_j, self.vsrc_k,
                                    self.vsrc_value)
            ftotal = None

            log.info('Adding PALM vsrc emissions from totals file')
            with Dataset(self.cfg.postproc.palmwriter.totalfile, 'r') as ftotal:
                self.process_palm_emissions_from_totals(ftotal)

            if self.point_vsrc_emiss:
                log.info('Adding PALM vsrc emissions from point sources')
                self.process_palm_point_sources(ftotal)

        finally:
            # close out file
            self.outfile.close()


    def create_vsrc_emiss_file_struct(self):
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
        self.outfile.origin_x = self.rt_cfg['domain']['xorg'] - self.rt_cfg['domain']['nx'] * self.rt_cfg['domain']['delx'] / 2.0
        self.outfile.origin_y = self.rt_cfg['domain']['yorg'] - self.rt_cfg['domain']['ny'] * self.rt_cfg['domain']['dely'] / 2.0
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

        # create netcdf dimensions and variables for 3d volume sources
        default_index = -1   # ijk index
        nvsrc_chunk = 256*1024 # 1 MB of f4, i4

        self.outfile.createDimension('nvsrc') # Unlimited dimension

        self.vsrc_i = self.outfile.createVariable('vsrc_i', 'i4', ('nvsrc',), fill_value=default_index, chunksizes=(nvsrc_chunk,))
        self.vsrc_j = self.outfile.createVariable('vsrc_j', 'i4', ('nvsrc',), fill_value=default_index, chunksizes=(nvsrc_chunk,))
        self.vsrc_k = self.outfile.createVariable('vsrc_k', 'i4', ('nvsrc',), fill_value=default_index, chunksizes=(nvsrc_chunk,))

        self.vsrc_value = [None] * len(self.species)
        for spec in self.species:
            #for ispec in range(len(self.species_lookup)):
            ispec = self.species_lookup[spec[0]]  # coordinate of species in netcdf (0..nspecies-1)
            log.debug('species[ispec]', ispec, self.species[ispec])
            vname = 'vsrc_' + self.species[ispec][1]
            log.debug('createVariable: ' + vname)
            self.vsrc_value[ispec] = self.outfile.createVariable(vname, 'f4', (self.names['t_dim'], 'nvsrc', ),
                                                                 fill_value=float(self.cfg.postproc.palmwriter.undef),
                                                                 chunksizes=(1,nvsrc_chunk))
            self.vsrc_value[ispec].missing_value = float(self.cfg.postproc.palmwriter.undef)
            self.vsrc_value[ispec].lod = 2
            if self.specie_type[spec[0]] == 0:
                # gas
                self.vsrc_value[ispec].units = 'mol/m3/s'
            else:
                # PM
                self.vsrc_value[ispec].units = 'kg/m3/s'
            self.vsrc_value[ispec].long_name = 'volume emission values ' + self.species[ispec][1]
            self.vsrc_value[ispec].standard_name = vname
            self.vsrc_value[ispec][:] = 0.0
            log.debug('Variable ' + vname + ' created')

    def init_full3d_levels(self, ftotal):
        """Finds levels 0-n in buildings_3d

        buildings_3d[k,j,i] = 1 where building and = 0 where air
        for each j, i:
            Level 0: highest k with air directly above building
            Level 1: second highest k with air directly above building
            etc.
        """
        if hasattr(self, 'level_kindex') and hasattr(self, 'missing_level') and hasattr(self, 'nlev'):
            # init_full3d_levels was already called
            return

        self.nlev = len(self.levels) - 1
        if ftotal is not None:
            self.nlev = max(self.nlev, ftotal.dimensions['level'].size - 1)
            log.fmt_debug('Using {} levels. FUME: {} levels ({}), totals file: {} levels ({}).',
                self.nlev, len(self.levels), self.levels, ftotal.dimensions['level'].size,
                ftotal.variables['level'][:])


        log.debug('Finding levels in PALM 3D building data.')
        nz, ny, nx = self.b3d.shape
        nxy = ny*nx

        # array for vertical comparison, has 1 extra level at bottom
        building_b = np.zeros((nz+1,ny,nx), dtype='i4')
        building_b[1:,:,:] = self.b3d

        # The logic taken from previous version is equivalent to b3d[0,:,:] being always filled with terrain
        building_b[0:2,:,:] = 1

        # find building-air boundary
        air_above_bld = (building_b[:-1,:,:] - building_b[1:,:,:]) == 1
        air_above_bld_rev = air_above_bld[::-1,:,:] #reversed z-coordinate for searching from above

        # find maximum number of levels in data
        nlev_b3d = air_above_bld.sum(axis=0).max()
        nlev = self.nlev #min(nlev_b3d, self.nlev)
        log.fmt_debug('Found {} levels in 3D building data. Preparing {} levels.', nlev_b3d, nlev)

        # Prepare open grid for indexing with a 2D layer of z-coordinates
        oy, ox = np.ogrid[0:ny,0:nx]

        self.level_kindex = level_kindex = np.empty((nlev, ny, nx), dtype='i4')
        self.missing_level = missing_level = np.empty((nlev, ny, nx), dtype=bool)
        lev = 0
        previous_level = np.array([[1]], dtype='i4')
        while 1:
            # Find highest boundary, argmax finds 1st occurrence along z in the reversed field
            highest = air_above_bld_rev.argmax(axis=0)

            # Columns with no boundary returned 0, at that z-coord it will be False
            no_boundary = ~(air_above_bld_rev[highest,oy,ox])
            nno_boundary = no_boundary.sum()
            log.fmt_debug('Level {}: found {} out of {} points', lev, nxy-nno_boundary, nxy)

            # Finalize level coordinate array
            level_kindex[lev,:,:] = np.where(no_boundary, previous_level, nz - 1 - highest) #flip back reversed z-coordinate
            missing_level[lev,:,:] = no_boundary

            previous_level = level_kindex[lev,:,:]
            lev += 1
            if lev >= nlev:
                break

            # clear highest so that we continue with the next highest
            air_above_bld_rev[highest,oy,ox] = 0

def get_nearest_free(mask, coord):
    """Find the point with mask==False nearest to coord"""

    # Find the selection of mask (coord +/- buffer)
    maxshift = ep_cfg.postproc.palmwriter.ptsrc_shift_max_points
    coord = np.asanyarray(coord)
    coord0 = np.maximum(0, coord - maxshift)
    coord1 = np.minimum(np.asanyarray(mask.shape), coord + (maxshift+1))
    newcoord = coord - coord0 #coord within selection

    sel = tuple(slice(c0, c1) for c0, c1 in zip(coord0, coord1))

    # Identify free points, calculate distance from coord
    free_pt = ~(mask[sel])
    if not free_pt.any():
        raise RuntimeError(f'No free points found near {coord} within {sel}.')
    free_pt_list = np.nonzero(free_pt)
    distance = sum(np.square(i-c) for i, c in zip(free_pt_list, newcoord))

    # Get nearest point, recalculate to original coords
    nearest_idx = np.argmin(distance) #index within list of free pts
    return tuple(fp[nearest_idx]+c0 for fp, c0 in zip(free_pt_list, coord0))
