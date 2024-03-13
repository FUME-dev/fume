"""
Description: Generic NetCDF dimension and variable names
Can be overriden in a NetCDFWriter-derived class
in the constructor arguments, e.g.:
    super().setup(x_dim='COL', t_dim='TSTEP')
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
import os
from postproc.receiver import DataReceiver, requires
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)

default_names = {
    'x_dim': 'x',
    'x_var': 'x',
    'y_dim': 'y',
    'y_var': 'y',
    'z_dim': 'z',
    'z_var': 'z',
    't_dim': 't',
    't_var': 'time',
    'v_dim': '',
    'category_dim': 'category',
    'category_id_var': 'category_id',
    'category_name_var': 'category_name',
    'ts_dim': 'ts',
    'ts_id_var': 'ts_id',
    'scale_factor' : 1,
    'emission_units' : 'moles/s for gases and g/s for aerosols',
}


"""
Use CF-compliant grid mapping parameters by default
"""

grid_mappings = {
    'LAMBERT': {'cf-name': 'lambert_conformal_conic',
                'cf-params': {'standard_parallel': ['p_alp', 'p_bet'],
                              'longitude_of_central_meridian': 'lon_central',
                              'latitude_of_projection_origin': 'lat_central'},
                },
    'LATLON': {'cf-name': 'latlon',
                 'cf-params': {'Latitude of natural origin': 'p_alp',
                               'Longitude of natural origin': 'p_bet',
                    },
                },
    'MERCATOR': {'cf-name': 'mercator',
                 'cf-params': {'Latitude of natural origin': 'p_alp',
                               'Longitude of natural origin': 'p_bet',
                               #'Scale factor at natural origin': 0.9996,
                               #'False easting': 500000,
                               #'False northing': 0
                              },
                 }
}

"""
Default action settings for the NetCDFWriter derived classes.
Can be overriden in child classes in the constructor arguments,
e.g. to prevent the parent class to create a time dimension+variable and
force creation of v dimension, use either of these:

    super().setup(no_create_t_dim=True, no_create_t_var=True, create_v_dim=True)
    super().setup(create_t_dim=False, create_t_var=False, no_create_v_dim=False)

"""

action_defaults = {
    'create_x_dim': True,
    'create_x_var': True,
    'create_y_dim': True,
    'create_y_var': True,
    'create_z_dim': True,
    'create_z_var': True,
    'create_t_dim': True,
    'create_t_var': True,
    'create_v_dim': False,
    'create_spec_vars': True,
    'create_projection_attrs': True,
    'close_outfile': True,
}

#undef = 0.

class NetCDFWriter(DataReceiver):
    """
    Postprocessor class for writing emission output files in NetCDF format.
    This is the base class for the common NetCDF format settings and should not
    be instantiated by itself. Descendant classes NetCDFAreaWriter should be
    used instead.

    Implements only common setup (create output netcdf file, time variable) and
    finalize (set common global attributes of the netcdf file and close the
    file).
    """

    def process_overrides(self, **kwargs):
        for k, v in kwargs.items():
            if k.startswith('no_'):
                self.actions[k[3:]] = not v
            elif k in self.actions:
                self.actions[k] = v
            elif k in self.names:
                self.names[k] = v

    def setup(self, *args, **kwargs):
        if 'undef' not in kwargs:
            if self.cfg.postproc.netcdfwriter.undef:
                kwargs['undef'] = float(self.cfg.postproc.netcdfwriter.undef)
            else:
                kwargs['undef'] = 0.0

        self.undef = kwargs['undef']
        if 'filename' not in kwargs and len(args) == 1:
            kwargs['filename'] = args[0]

        filepath = os.path.dirname(os.path.abspath(kwargs['filename']))
        if not os.path.exists(filepath):
            os.makedirs(filepath)

        log.debug('Filename:', kwargs['filename'])
        self.outfile = Dataset(kwargs['filename'], 'w', format='NETCDF4')

        self.names = default_names.copy()
        self.actions = action_defaults.copy()
        log.debug(kwargs)
        self.process_overrides(**kwargs)

        if self.actions['create_z_dim']:
            self.outfile.createDimension(self.names['z_dim'],
                                         self.cfg.domain.nz)
        if self.actions['create_y_dim']:
            self.outfile.createDimension(self.names['y_dim'],
                                         self.cfg.domain.ny)
        if self.actions['create_x_dim']:
            self.outfile.createDimension(self.names['x_dim'],
                                         self.cfg.domain.nx)
        if self.actions['create_t_dim']:
            self.outfile.createDimension(self.names['t_dim'], None)

    def finalize(self):
        self.outfile.history = ''

        if self.actions['create_projection_attrs']:
            grid_mapping = grid_mappings[self.rt_cfg['projection_params']['proj']]
            self.outfile.grid_mapping_name = grid_mapping['cf-name']
            for p_name, p_values in grid_mapping['cf-params'].items():
                if isinstance(p_values, list):
                    setattr(self.outfile, p_name,
                            [self.rt_cfg['projection_params'][n] for n in
                             p_values])
                else:
                    setattr(self.outfile, p_name,
                            self.rt_cfg['projection_params'][p_values])

        if self.actions['close_outfile']:
            log.debug('closing file: ', self.outfile.filepath())
            self.outfile.close()


class NetCDFTotalWriter(NetCDFWriter):
    """
    Postprocessor class for writing NetCDF area emission file.
    Outputs are: total emission data by species and category.
    """

    def setup(self, *args, **kwargs):
        if 'filename' not in kwargs:
            if self.cfg.postproc.netcdfareawriter.totalfile:
                kwargs['filename'] = self.cfg.postproc.netcdfareawriter.totalfile
            else:
                log.error('Missing configuration parameter postproc.netcdfareawriter.totalfile!')

        kwargs['no_create_t_dim'] = True
        kwargs['no_create_t_var'] = True
        super().setup(*args, **kwargs)
        if self.actions['create_z_var']:
            self.z_var = self.outfile.createVariable(self.names['z_var'], 'i4',
                                                     [self.names['z_dim']])

    @requires('categories','time_shifts')
    def receive_species(self, species):
        self.species = species
        self.species_lookup = {member[0]: idx
                               for idx, member in enumerate(self.species)}
        self.outvars = []

        for specid, specname in self.species:
            log.debug('receive_species:', self.names['ts_dim'], self.names['category_dim'], self.names['z_dim'], self.names['y_dim'],self.names['x_dim'])
            emisvar = self.outfile.createVariable(
                specname, 'f4', (self.names['ts_dim'],
                                 self.names['category_dim'],
                                 self.names['z_dim'],
                                 self.names['y_dim'],
                                 self.names['x_dim']),
                fill_value=self.undef)
            emisvar.long_name = specname
            emisvar.units = 'moles/s for gases and g/s for aerosols'
            emisvar.var_desc = 'Model species ' + specname
            emisvar.missing_value = self.undef
            self.outvars.append(emisvar)

    def receive_categories(self, categories):
        log.debug('receive_categories:', categories)
        self.categories = categories
        self.categories_lookup = {member[0]: idx
                                  for idx, member in enumerate(self.categories)}

        self.outfile.createDimension(self.names['category_dim'],
                                     len(self.categories))
        cat_id_var = self.outfile.createVariable(
            self.names['category_id_var'], 'i8', (self.names['category_dim'], ))
        cat_id_var[:] = [i[0] for i in self.categories]

        cat_name_var = self.outfile.createVariable(
            self.names['category_name_var'], 'str',
            (self.names['category_dim'], ))

        cat_name_var[:] = np.array([i[1] for i in self.categories], dtype='object')

    def receive_time_shifts(self, time_shifts):
        self.time_shifts = time_shifts
        self.ts = []
        for ts_id in time_shifts:
            if ts_id[0] not in self.ts:
                self.ts.append(ts_id[0])
        self.ts_lookup = {member: idx for idx, member in enumerate(self.ts)}

        self.outfile.createDimension(self.names['ts_dim'], len(self.ts))
        ts_id_var = self.outfile.createVariable(
            self.names['ts_id_var'], 'i8', (self.names['ts_dim'], ))
        ts_id_var[:] = [i for i in self.ts]

    def finalize(self):
        super().finalize()


class NetCDFTotalAreaWriter(NetCDFTotalWriter):
    """
    Postprocessor class for writing NetCDF area emission file.
    Outputs are: total emission data by species and category.
    """

    @requires('species')
    def receive_area_emiss_by_species_and_category(self, data):
        log.debug('area_emiss_by_species_and_category')
        for row in data:
            #log.debug('netcdf: receive_area_emiss_by_species_and_category: ', row)
            spec_idx = self.species_lookup[row[3]]
            cat_idx = self.categories_lookup[row[4]]
            ts_idx = self.ts_lookup[row[5]]
            self.z_var[row[2]-1] = row[2]
            self.outvars[spec_idx][ts_idx, cat_idx, row[2]-1, row[1]-1, row[0]-1] =\
                (row[6] < 1e+36 and row[6] or self.undef)

    def finalize(self):
        self.outfile.FILEDESC = 'Total area emissions created by FUME ' + self.cfg.run_params.output_params.output_description
        log.debug('NetCDFTotalAreaWriter finalize start')
        super().finalize()
        log.debug('NetCDFTotalAreaWriter finalize end')


class NetCDFAreaTimeDisaggregator(NetCDFWriter):
    """
    Postprocessor class for writing time series of area emissions into a generic
    NetCDF file.

    Assumes area totals were written previously by NetCDFTotalAreaWriter
    in the file named self.cfg.postproc.netcdfareawriter.totalfile
    """

    def setup(self, *args, **kwargs):
        if 'filename' not in kwargs:
            if self.cfg.postproc.netcdfareawriter.timedfile:
                kwargs['filename'] = self.cfg.postproc.netcdfareawriter.timedfile
            else:
                log.error('Missing configuration parameter postproc.netcdfareawriter.timedfile!')

        super().setup(*args, **kwargs)

    @requires('categories')
    def receive_species(self, species):
        self.species = species
        if self.actions['create_v_dim']:
            self.outfile.createDimension(self.names['v_dim'],
                                     len(self.species))
        if self.actions['create_spec_vars']:
            self.outvars = []
            for specid, specname in self.species:
                emisvar = self.outfile.createVariable(
                    specname, 'f4', (self.names['t_dim'],
                                     self.names['z_dim'],
                                     self.names['y_dim'],
                                     self.names['x_dim']),
                    fill_value=self.undef)
                emisvar.long_name = specname
                emisvar.units = self.names['emission_units']
                emisvar.var_desc = 'Model species ' + specname
                emisvar.missing_value = self.undef
                self.outvars.append(emisvar)

    def receive_categories(self, categories):
        self.categories = categories
        self.categories_lookup = {member[0]: idx for idx, member in enumerate(self.categories)}

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

        # intialize species specific scale factors
        species_scale_factors = { s[1]:1.0 for s in self.species }
        # override by self.names['scale_factor']
        if type(self.names['scale_factor']) is not dict:
            species_scale_factors = { s[1]: self.names['scale_factor'] for s in self.species }
        else:
        # self.names['scale_factor'] is a dictionary, overwrite species_scale_factors content
            for s in self.names['scale_factor']:
                species_scale_factors[s] = self.names['scale_factor'][s]

        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
            log.debug('Time step ', time_idx, stepdt, stepdt.replace(tzinfo=None))
            for spec_idx, (specid, specname) in enumerate(self.species):
                self.outvars[spec_idx][time_idx, :] = 0.
                for ts_id in self.ts:
                    ts_idx = self.ts_lookup[ts_id]
                    for cat_idx, (catid, catname) in enumerate(self.categories):
                        dtutc = self.time_shifts[(ts_id, stepdt)]
                        tf = self.time_factors[dtutc]
                        try:
                            time_factor = float(tf[catid])
                        except KeyError:
                            continue
                        self.outvars[spec_idx][time_idx, :, :, :] += \
                            self.infile.variables[specname][ts_idx, cat_idx, :, :, :].filled(fill_value=0)*time_factor* species_scale_factors[specname] #self.names['scale_factor']

        if self.actions['create_t_var']:
            self.timevar = self.outfile.createVariable(self.names['t_var'],
                                                       'f4',
                                                       (self.names['t_dim'], ))
            self.timevar.units = 'hours since ' + self.rt_cfg['run']['datestimes'][0].strftime('%Y-%m-%d %H:%M')
            self.timevar[:] = date2num([d.replace(tzinfo=None) for d in
                                        self.rt_cfg['run']['datestimes']],
                                       units=self.timevar.units,
                                       calendar='standard')

        self.outfile.FILEDESC = 'Area emissions created by FUME ' + self.cfg.run_params.output_params.output_description

        super().finalize()


class NetCDFTotalPointWriter(NetCDFWriter):
    """
    Postprocessor class for writing NetCDF point emission file.
    Outputs are: total point emission data by species and category.
    """

    def setup(self, *args, **kwargs):
        kwargs['no_create_x_dim'] = True
        kwargs['no_create_y_dim'] = True
        kwargs['no_create_z_dim'] = True
        if 'filename' not in kwargs:
            if self.cfg.postproc.netcdfpointwriter.totalfile:
                kwargs['filename'] = self.cfg.postproc.netcdfpointwriter.totalfile
            else:
                log.error('Missing configuration parameter postproc.netcdfareawriter.totalfile!')
        super().setup(*args, **kwargs)
# do this within the receive stack params, as it tells the size of the z-dimension (numstk)
#        self.outfile.createDimension(self.names['z_dim'], self.cfg.domain.nz)
        self.outfile.createDimension(self.names['y_dim'], 1)
        self.outfile.createDimension(self.names['x_dim'], 1)

    def receive_stack_params(self, stacks):
        self.point_src_params = stacks
        self.numstk = self.point_src_params.shape[0]
        
        self.outfile.createDimension(self.names['z_dim'], self.numstk)
        
        self.stacks_id = list(map(int,self.point_src_params[:,0]))
        self.stacks_lookup = {member: idx for idx, member in enumerate(self.stacks_id)}

        # int variables
        var_int =      ['ISTACK', 'STKCNT', 'ROW', 'COL', 'LMAJOR', 'LPING']
        var_int_units =['none', 'none', 'none', 'none', 'none',  'none']
        stkintvars = []
        xorig = self.cfg.domain.xorg - self.cfg.domain.nx*self.cfg.domain.delx/2.0
        yorig = self.cfg.domain.yorg - self.cfg.domain.ny*self.cfg.domain.dely/2.0
        for var in var_int:
            stkvar = self.outfile.createVariable(var, 'i4', (self.names['z_dim']))
            stkvar.longname = '{0:16s}'.format(var)
            unit = var_int_units[var_int.index(var)]
            stkvar.unit = '{0:16s}'.format(unit)
            stkintvars.append(stkvar)

        stkintvars[0][:] = np.array([self.stacks_id])
        stkintvars[1][:] = np.array([range(0, self.numstk)])
        stkintvars[3][:] = np.array([ ((self.point_src_params[:,2]-xorig)/self.cfg.domain.delx).astype('int') + 1])
        stkintvars[2][:] = np.array([ ((self.point_src_params[:,4]-yorig)/self.cfg.domain.dely).astype('int') + 1])
        stkintvars[4][:] = np.zeros((self.numstk),dtype=int)
        stkintvars[5][:] = np.zeros((self.numstk),dtype=int)
        

        # float variables
        var_float = ['LATITUDE', 'LONGITUDE', 'STKDM', 'STKHT', 'STKTK', 'STKVE', 'STKFLW', 'XLOCA', 'YLOCA']
        var_float_units = ['degrees', 'degrees', 'm', 'm', 'degrees K', 'm/s', 'm**3/s', '', '']

        stkfloatvars = []
        for var in var_float:
            stkvar = self.outfile.createVariable(var, 'f4', (self.names['z_dim']))
            stkvar.longname = '{0:16s}'.format(var)
            unit = var_float_units[var_float.index(var)]
            stkvar.unit = '{0:16s}'.format(unit)
            stkfloatvars.append(stkvar)

        #LATITUDE
        stkfloatvars[0][:] = np.array([self.point_src_params[:,3]] )
        #LONGITUDE
        stkfloatvars[1][:] = np.array([self.point_src_params[:,1] ] )
        #STKDM
        stkfloatvars[2][:] = np.array([self.point_src_params[:,6] ] )
        #STKHT
        stkfloatvars[3][:] = np.array([ self.point_src_params[:,5] ])
        #STKTK
        stkfloatvars[4][:] = np.array([self.point_src_params[:,7] ] )
        #STKVE
        stkfloatvars[5][:] = np.array([self.point_src_params[:,8] ] )
        #STKFLW
        stkfloatvars[6][:] = np.array([(0.5*self.point_src_params[:,6])**2 * 3.1415 * self.point_src_params[:,8] ])
        #XLOCA
        stkfloatvars[7][:] = np.array([self.point_src_params[:,2] ] )
        #YLOCA
        stkfloatvars[8][:] = np.array([self.point_src_params[:,4]  ] )

    @requires('stack_params','point_species','time_shifts')
    def receive_point_emiss_by_species_and_category(self, data):
        for row in data:
            spec_idx = self.pspecies_lookup[row[1]]
            cat_idx = self.pcategories_lookup[row[2]]
            ts_idx = self.ts_lookup[row[3]]
            stk_idx = self.stacks_lookup[row[0]]
            self.outvars[spec_idx][ts_idx, cat_idx, stk_idx] =\
                (row[4] < 1e+36 and row[4] or self.undef)

    @requires('point_categories','time_shifts')
    def receive_point_species(self, pspecies):
        self.pspecies = pspecies
        self.pspecies_lookup = {member[0]: idx
                               for idx, member in enumerate(self.pspecies)}
        self.outvars = []

        for specid, specname in self.pspecies:
            emisvar = self.outfile.createVariable(
                specname, 'f4', (self.names['ts_dim'],
                                 self.names['category_dim'],
                                 self.names['z_dim']),
                fill_value=self.undef)
            emisvar.long_name = specname
            emisvar.units = 'moles/s for gases and g/s for aerosols'
            emisvar.var_desc = 'Model species ' + specname
            emisvar.missing_value = self.undef
            self.outvars.append(emisvar)

    def receive_point_categories(self, pcategories):
        self.pcategories = pcategories
        self.pcategories_lookup = {member[0]: idx
                                  for idx, member in enumerate(self.pcategories)}

        self.outfile.createDimension(self.names['category_dim'],
                                     len(self.pcategories))
        cat_id_var = self.outfile.createVariable(
            self.names['category_id_var'], 'i8', (self.names['category_dim'], ))
        cat_id_var[:] = [i[0] for i in self.pcategories]

        cat_name_var = self.outfile.createVariable(
            self.names['category_name_var'], 'str',
            (self.names['category_dim'], ))

        cat_name_var[:] = np.array([i[1] for i in self.pcategories], dtype='object')


    def receive_time_shifts(self, time_shifts):
        self.time_shifts = time_shifts
        self.ts = []
        for ts_id in time_shifts:
            if ts_id[0] not in self.ts:
                self.ts.append(ts_id[0])
        self.ts_lookup = {member: idx for idx, member in enumerate(self.ts)}

        self.outfile.createDimension(self.names['ts_dim'], len(self.ts))
        ts_id_var = self.outfile.createVariable(
            self.names['ts_id_var'], 'i8', (self.names['ts_dim'], ))
        ts_id_var[:] = [i for i in self.ts]

    def finalize(self):
        self.outfile.FILEDESC = 'Native point emission file created by FUME '

        super().finalize()


class NetCDFTotal3DWriter(NetCDFWriter):
    """
    Postprocessor class for writing area + point total emissions into a generic 3D emission
    NetCDF file.
    """
    def setup(self, *args, **kwargs):
        kwargs['no_create_t_dim'] = True
        kwargs['no_create_t_var'] = True
        if 'filename' not in kwargs:
            if self.cfg.postproc.netcdf3dwriter.totalfile:
                kwargs['filename'] = self.cfg.postproc.netcdf3dwriter.totalfile
            else:
                log.error('Missing configuration parameter postproc.netcdf3dwriter.totalfile!')

    @requires('area_emiss_by_species_and_category', 'stack_params')
    def receive_point_emiss_by_species_and_category(self, data):
        for row in data:
            spec_idx = self.aspecies_lookup[row[1]]
            cat_idx = self.acategories_lookup[row[2]]
            ts_idx = self.ts_lookup[row[3]]
            stk_idx = self.stacks_lookup[row[0]]
            # combining with the surface data accoring to stack height
            self.outvars[spec_idx][ts_idx, cat_idx, self.stacks_layer[stk_idx], self.stacks_y[stk_idx], self.stacks_x[stk_idx]] += row[4]


    @requires('time_shifts')
    def receive_area_emiss_by_species_and_category(self, data):
        for row in data:
    #       filling up the 3D array with area emissions
            spec_idx = self.aspecies_lookup[row[3]]
            cat_idx = self.acategories_lookup[row[4]]
            ts_idx = self.ts_lookup[row[5]]
            self.outvars[spec_idx][ts_idx, cat_idx, row[2]-1, row[1]-1, row[0]-1] = row[6]

    def receive_stack_params(self, stacks):
        self.point_src_params = stacks
        self.stacks_id = list(map(int,self.point_src_params[:,0]))
        self.stacks_lookup = {member: idx for idx, member in enumerate(self.stacks_id)}
        self.numstk = len(self.stacks_id)

        xorig = self.cfg.domain.xorg - self.cfg.domain.nx*self.cfg.domain.delx/2.0
        yorig = self.cfg.domain.yorg - self.cfg.domain.ny*self.cfg.domain.dely/2.0
        maxlevel = 20000
        mlevels = [ int(float(l)) for l in self.cfg.postproc.netcdf3dwriter.levels ]
        heights = np.zeros((maxlevel),dtype=int)
        for i in range(1,len(mlevels)):
            heights[mlevels[i-1]:mlevels[i]] = i

        self.stacks_x = [ min(int((self.point_src_params[s,2]-xorig)/self.cfg.domain.delx),self.cfg.domain.nx-1) for s in range(self.numstk)]
        self.stacks_y = [ min(int((self.point_src_params[s,4]-yorig)/self.cfg.domain.dely),self.cfg.domain.ny-1) for s in range(self.numstk)]
        self.stacks_layer =[ heights[int(self.point_src_params[s,5])] for s in range(self.numstk)]

    def receive_all_species(self, aspecies):
        self.aspecies = aspecies
        self.aspecies_lookup = {member[0]: idx
                                  for idx, member in enumerate(self.aspecies)}
    def receive_all_categories(self, acategories):
        self.acategories = acategories
        self.acategories_lookup = {member[0]: idx
                                  for idx, member in enumerate(self.acategories)}

        self.outfile.createDimension(self.names['category_dim'],
                                     len(self.acategories))
        cat_id_var = self.outfile.createVariable(
            self.names['category_id_var'], 'i8', (self.names['category_dim'], ))
        cat_id_var[:] = [i[0] for i in self.acategories]

        cat_name_var = self.outfile.createVariable(
            self.names['category_name_var'], 'str',
            (self.names['category_dim'], ))
        cat_name_var[:] = np.array([i[1] for i in self.acategories], dtype='object')

    @requires('all_species', 'all_categories')
    def receive_time_shifts(self, time_shifts):
        self.time_shifts = time_shifts
        self.ts = []
        for ts_id in time_shifts:
            if ts_id[0] not in self.ts:
                self.ts.append(ts_id[0])
        self.ts_lookup = {member: idx for idx, member in enumerate(self.ts)}

        self.outfile.createDimension(self.names['ts_dim'], len(self.ts))
        ts_id_var = self.outfile.createVariable(
            self.names['ts_id_var'], 'i8', (self.names['ts_dim'], ))
        ts_id_var[:] = [i for i in self.ts]
        self.outvars = []

        for specid, specname in self.aspecies:
            emisvar = self.outfile.createVariable(
                specname, 'f4', (self.names['ts_dim'],
                                 self.names['category_dim'],
                                 self.names['z_dim'],
                                 self.names['y_dim'],
                                 self.names['x_dim']),
                fill_value=self.undef)
            emisvar.long_name = specname
            emisvar.units = 'moles/s for gases and g/s for aerosols'
            emisvar.var_desc = 'Model species ' + specname
            emisvar.missing_value = self.undef
            emisvar[...] = 0.0
            self.outvars.append(emisvar)

    def finalize(self):
        self.outfile.FILEDESC = '3D area + point emission file created by FUME '
        super().finalize()


class NetCDFPointTimeDisaggregator(NetCDFWriter):
    """
    Postprocessor class for writing time series of point emissions into a generic
    NetCDF file.
    """
    def setup(self, *args, **kwargs):
        if 'filename' not in kwargs:
            if self.cfg.postproc.netcdfpointwriter.timedfile:
                kwargs['filename'] = self.cfg.postproc.netcdfpointwriter.timedfile
            else:
                log.error('Missing configuration parameter postproc.netcdfpointwriter.timedfile!')

        kwargs['no_create_x_dim'] = True
        kwargs['no_create_y_dim'] = True
        kwargs['no_create_z_dim'] = True
        super().setup(*args, **kwargs)
        # we need the total point file here to extract information about the stacks
        self.infile = Dataset(self.cfg.postproc.netcdfpointwriter.totalfile)
        self.numstk = len(self.infile.dimensions['z'])
#        if self.actions['create_z_dim']:
        self.outfile.createDimension(self.names['z_dim'], self.numstk)
#        if self.actions['create_y_dim']:
        self.outfile.createDimension(self.names['y_dim'],1)
#        if self.actions['create_x_dim']:
        self.outfile.createDimension(self.names['x_dim'],1)

#        if self.actions['create_t_dim']:
##            self.outfile.createDimension(self.names['t_dim'], None)

        # copy stack attributes to the dissagregated file
        toinclude = ["ISTACK", "STKCNT", "ROW", "COL", "LMAJOR", "LPING", "LATITUDE", "LONGITUDE", "STKDM", "STKHT", "STKTK", "STKVE", "STKFLW", "XLOCA", "YLOCA"]
        for name, variable in self.infile.variables.items():
            if name  in toinclude:
                x = self.outfile.createVariable(name, variable.datatype, (self.names['z_dim'],))
                x.setncatts(self.infile.variables[name].__dict__)
                x[:] = self.infile.variables[name][:]

    @requires('point_categories')
    def receive_point_species(self, pspecies):
        self.pspecies = pspecies
        if self.actions['create_v_dim']:
            self.outfile.createDimension(self.names['v_dim'],
                                     len(self.pspecies))
        if self.actions['create_spec_vars']:
            self.outvars = []
            for specid, specname in self.pspecies:
                emisvar = self.outfile.createVariable(
                    specname, 'f4', (self.names['t_dim'],
                                     self.names['z_dim']),
                    fill_value=self.undef)
                emisvar.long_name = specname
                emisvar.units = self.names['emission_units']
                emisvar.var_desc = 'Model species ' + specname
                emisvar.missing_value = self.undef
                self.outvars.append(emisvar)

    def receive_point_categories(self, pcategories):
        self.pcategories = pcategories

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
        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
            log.debug('Time step ', time_idx, stepdt, stepdt.replace(tzinfo=None))
            for spec_idx, (specid, specname) in enumerate(self.pspecies):
                self.outvars[spec_idx][time_idx, :] = 0
                for ts_id in self.ts:
                    ts_idx = self.ts_lookup[ts_id]
                    for cat_idx, (catid, catname) in enumerate(self.pcategories):
                        dtutc = self.time_shifts[(ts_id, stepdt)]
                        tf = self.time_factors[dtutc]
                        try:
                            time_factor = float(tf[catid])
                        except KeyError:
                            continue
                        self.outvars[spec_idx][time_idx,  :] += \
                            self.infile.variables[specname][ts_idx, cat_idx, :].filled(fill_value=0)*time_factor*self.names['scale_factor']

        if self.actions['create_t_var']:
            self.timevar = self.outfile.createVariable(self.names['t_var'],
                                                       'f4',
                                                       (self.names['t_dim'], ))
            self.timevar.units = 'hours since ' + self.rt_cfg['run']['datestimes'][0].strftime('%Y-%m-%d %H:%M')
            self.timevar[:] = date2num([d.replace(tzinfo=None) for d in
                                        self.rt_cfg['run']['datestimes']],
                                       units=self.timevar.units,
                                       calendar='standard')

        self.outfile.FILEDESC = 'Point emissions created by FUME ' + self.cfg.run_params.output_params.output_description

        super().finalize()


class NetCDF3DTimeDisaggregator(NetCDFWriter):
    """
    Postprocessor class for writing time series of 3D emissions into a generic
    NetCDF file.
    """

    def setup(self, *args, **kwargs):
        if 'filename' not in kwargs:
            if self.cfg.postproc.netcdf3dwriter.timedfile:
                kwargs['filename'] = self.cfg.postproc.netcdf3dwriter.timedfile
            else:
                log.error('Missing configuration parameter postproc.netcdf3dwriter.timedfile!')

        super().setup(*args, **kwargs)

    def receive_time_factors(self, factors):
        self.time_factors = factors

    def receive_time_shifts(self, time_shifts):
        self.time_shifts = time_shifts
        self.ts = []
        for ts_id in time_shifts:
            if ts_id[0] not in self.ts:
                self.ts.append(ts_id[0])
        self.ts_lookup = {member: idx for idx, member in enumerate(self.ts)}

    @requires('time_shifts', 'time_factors')
    def finalize(self):
        self.infile = Dataset(self.cfg.postproc.netcdf3dwriter.totalfile)

        # get the list of categories
        category_ids = self.infile.variables[self.names['category_id_var']][:]
        category_names = self.infile.variables[self.names['category_name_var']][:]
        categories = [(catid, catname) for catid, catname in zip(category_ids,category_names)]

        # get the list of variables
        species = []
        for v in self.infile.variables:
            try:
                if "species" in  self.infile.variables[v].var_desc:
                    species.append(v)
            except AttributeError:
                pass

        if self.actions['create_spec_vars']:
            self.outvars = []
            for specname in species:
                emisvar = self.outfile.createVariable(
                    specname, 'f4', (self.names['t_dim'],
                                     self.names['z_dim'],
                                     self.names['y_dim'],
                                     self.names['x_dim']),
                    fill_value=float(undef))
                emisvar.long_name = specname
                emisvar.units = self.names['emission_units']
                emisvar.var_desc = 'Model species ' + specname
                emisvar.missing_value = float(self.cfg.postproc.netcdf.undef)
                self.outvars.append(emisvar)


        for time_idx, stepdt in enumerate(self.rt_cfg['run']['datestimes']):
            log.debug('Time step ', time_idx, stepdt, stepdt.replace(tzinfo=None))
            for spec_idx, specname in enumerate(species):
                self.outvars[spec_idx][time_idx, :] = 0
                for ts_id in self.ts:
                    ts_idx = self.ts_lookup[ts_id]
                    for cat_idx, (catid, catname) in enumerate(categories):
                        dtutc = self.time_shifts[(ts_id, stepdt)]
                        tf = self.time_factors[dtutc]
                        try:
                            time_factor = float(tf[catid])
                        except KeyError:
                            continue

                        self.outvars[spec_idx][time_idx, :, :, :] += \
                            self.infile.variables[specname][ts_idx, cat_idx, :, :, :].filled(fill_value=0)*time_factor*self.names['scale_factor']

        if self.actions['create_t_var']:
            self.timevar = self.outfile.createVariable(self.names['t_var'],
                                                       'f4',
                                                       (self.names['t_dim'], ))
            self.timevar.units = 'hours since ' + self.rt_cfg['run']['datestimes'][0].strftime('%Y-%m-%d %H:%M')
            self.timevar[:] = date2num([d.replace(tzinfo=None) for d in
                                        self.rt_cfg['run']['datestimes']],
                                       units=self.timevar.units,
                                       calendar='standard')

        self.outfile.FILEDESC = '3D emissions created by FUME '

        super().finalize()
