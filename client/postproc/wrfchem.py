"""
Description: WRFChem output postprocessor

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
from postproc.netcdf import NetCDFAreaTimeDisaggregator, NetCDF3DTimeDisaggregator

# WRF-Chem-specific projection IDs
gdtyp_mapping = {
        'LATLON': 1,
        'UTM': 5,
        'LAMBERT': 2,
        'STEREO': 4,
        'POLAR': 6,
        'EMERCATOR': 7,
        'MERCATOR': 3
}


def wrfchem_date_time(dt):
    return ( dt.strftime('%Y-%m-%d_%H:%M:%S'), dt.year, dt.timetuple().tm_yday)


class WRFCHEMAreaTimeWriter(NetCDFAreaTimeDisaggregator):
    """
    Postprocessor writing time disaggregated area emissions.
    *Time-optimized version: time disaggregation performed with NetCDF
    files. Requires a prior run of NetCDFTotalAreaWriter!*
    """

    def setup(self):
        """
        Run NetCDF setup with WRF-Chem-specific overrides:
        Dimensions names are x=west_east, Y=south_north, Z=emission_zdim, T=Time
        Do not let the parent class create a time variable,
        projection attributes and close the file during finalize.
        """
#       mole/(km*km)/hr. The units of all aerosol species are microgram/(m*m)/s
        gas_coef = 3600.0/(self.cfg.domain.delx*self.cfg.domain.dely)
        aer_coef =    1.0/(self.cfg.domain.delx*self.cfg.domain.dely)
        ##################################################################
        gases = ['NO2', 'NO'] # fill explicitly the gaseous species in WRF (without the 'E' prefix)
        aero = ['PEC', 'POA'] # same for aerosols 

        units_conversion = { }        
        for g in gases:
            units_conversion[g] = gas_coef
        for a in aero:
            units_conversion[a] = aer_coef

        super().setup(filename=self.cfg.postproc.wrfchem.outfile,
                      no_create_t_var=True, no_create_projection_attrs=True,
                      #create_v_dim=True, v_dim='VAR',
                      no_create_x_dim=True, no_create_y_dim=True, no_create_z_dim=True,
                      t_dim='Time', x_dim='west_east', y_dim='south_north', z_dim='emission_zdim',
                      no_close_outfile=True, scale_factor = units_conversion)

        self.strdim = self.outfile.createDimension('DateStrLen', 19)
        self.outfile.createDimension(self.names['x_dim'],self.cfg.domain.nx)
        self.outfile.createDimension(self.names['y_dim'],self.cfg.domain.ny)
        self.outfile.createDimension(self.names['z_dim'],self.cfg.domain.nz)
        self.timevar = self.outfile.createVariable('Times', 'c', ('Time', 'DateStrLen'))


    def finalize(self):
        """
        Finalization steps
        ------------------

         - create the WRF-Chem-specific time variable TFLAG
         - run parent finalize (save output data and generic NetCDF attrib	utes)
         - save time data
         - change the long_name attribute of output variables to WRF-Chem format
         - save WRF-Chem-specific attributes
         - close the file

        """

        for outvar in self.outvars:
            outvar.FieldType = np.int32(104)
            outvar.MemoryOrder = "XYZ"
            outvar.description = "EMISSIONS"
            outvar.units = "mole/(km*km)/hr. The units of all aerosol species are microgram/(m*m)/s"
            outvar.stagger = ""
            outvar.coordinates = "XLONG XLAT"
            oldname = outvar.name
            newname = 'E_'+oldname
            self.outfile.renameVariable(oldname, newname)

        for timestep, datetime in enumerate(self.rt_cfg['run']['datestimes']):
            date, yyyy, ddd = wrfchem_date_time(datetime)
            self.timevar[timestep,:] = date

        self.outfile.CEN_LAT = np.float64(self.rt_cfg['projection_params']['lat_central'])
        self.outfile.CEN_LON = np.float64(self.rt_cfg['projection_params']['lon_central'])
        self.outfile.TRUELAT1 = np.float64(self.rt_cfg['projection_params']['p_alp'])
        self.outfile.TRUELAT2 = np.float64(self.rt_cfg['projection_params']['p_bet'])
        self.outfile.MOAD_CEN_LAT = np.float64(self.rt_cfg['projection_params']['lat_central'])
        self.outfile.STAND_LON = np.float64(self.rt_cfg['projection_params']['p_gam'])
        self.outfile.POLE_LAT = np.float64(90.)
        self.outfile.POLE_LON = np.float64(0.)
        self.outfile.GMT = np.float64(0.)
        self.outfile.JULYR = np.int32(yyyy)
        self.outfile.JULDAY = np.int32(ddd)
        self.outfile.MAP_PROJ = np.int32(1)
        self.outfile.MAP_PROJ_CHAR = 'Lambert Conformal'
        self.outfile.MMINLU = 'USGS'
        self.outfile.NUM_LAND_CAT = np.int32(24)
        self.outfile.ISWATER = np.int32(16)
        self.outfile.ISLAKE = np.int32(-1)
        self.outfile.ISICE = np.int32(24)
        self.outfile.ISURBAN = np.int32(1)
        self.outfile.ISOILWATER = np.int32(14)

        super().finalize()


class WRFCHEM3DTimeWriter(NetCDF3DTimeDisaggregator):
    """
    Postprocessor writing time disaggregated area emissions.
    *Time-optimized version: time disaggregation performed with NetCDF
    files. Requires a prior run of NetCDFTotalAreaWriter!*
    """

    def setup(self):
        """
        Run NetCDF setup with WRF-Chem-specific overrides:
        Dimensions names are x=west_east, Y=south_north, Z=emission_zdim, T=Time
        Do not let the parent class create a time variable,
        projection attributes and close the file during finalize.
        """
#       mole/(km*km)/hr. The units of all aerosol species are microgram/(m*m)/s
        gas_coef = 3600.0/(self.cfg.domain.delx*self.cfg.domain.dely)
        aer_coef =    1.0/(self.cfg.domain.delx*self.cfg.domain.dely)
        ##################################################################
        gases = ['NO2', 'NO'] # fill explicitly the gaseous species in WRF (without the 'E' prefix)
        aero = ['PEC', 'POA'] # same for aerosols 

        units_conversion = { }        
        for g in gases:
            units_conversion[g] = gas_coef
        for a in aero:
            units_conversion[a] = aer_coef

        super().setup(filename=self.cfg.postproc.wrfchemareawriter.outfile,
                      no_create_t_var=True, no_create_projection_attrs=True,
                      #create_v_dim=True, v_dim='VAR',
                      no_create_x_dim=True, no_create_y_dim=True, no_create_z_dim=True,
                      t_dim='Time', x_dim='west_east', y_dim='south_north', z_dim='emission_zdim',
                      no_close_outfile=True, scale_factor = units_conversion)

        self.outfile.createDimension('DateStrLen', 19)
        self.outfile.createDimension(self.names['x_dim'],self.cfg.domain.nx)
        self.outfile.createDimension(self.names['y_dim'],self.cfg.domain.ny)
        self.outfile.createDimension(self.names['z_dim'],self.cfg.domain.nz)
        self.timevar = self.outfile.createVariable('Times', 'c', ('Time', 'DateStrLen'))

    def finalize(self):
        """
        Finalization steps
        ------------------

         - create the WRF-Chem-specific time variable TFLAG
         - run parent finalize (save output data and generic NetCDF attrib      utes)
         - save time data
         - change the long_name attribute of output variables to WRF-Chem format
         - save WRF-Chem-specific attributes
         - close the file

        """
        super().finalize()

        for timestep, datetime in enumerate(self.rt_cfg['run']['datestimes']):
            date, yyyy, ddd = wrfchem_date_time(datetime)
            self.timevar[timestep,:] = date

        #for outvar in self.outvars:
        self.outfile.CEN_LAT = np.float64(self.rt_cfg['projection_params']['lat_central'])
        self.outfile.CEN_LON = np.float64(self.rt_cfg['projection_params']['lon_central'])
        self.outfile.TRUELAT1 = np.float64(self.rt_cfg['projection_params']['p_alp'])
        self.outfile.TRUELAT2 = np.float64(self.rt_cfg['projection_params']['p_bet'])
        self.outfile.MOAD_CEN_LAT = np.float64(self.rt_cfg['projection_params']['lat_central'])
        self.outfile.STAND_LON = np.float64(self.rt_cfg['projection_params']['p_gam'])
        self.outfile.POLE_LAT = np.float64(90.)
        self.outfile.POLE_LON = np.float64(0.)
        self.outfile.GMT = np.float64(0.)
        self.outfile.JULYR = np.int32(yyyy)
        self.outfile.JULDAY = np.int32(ddd)
        self.outfile.MAP_PROJ = np.int32(1)
        self.outfile.MAP_PROJ_CHAR = 'Lambert Conformal'
        self.outfile.MMINLU = 'USGS'
        self.outfile.NUM_LAND_CAT = np.int32(24)
        self.outfile.ISWATER = np.int32(16)
        self.outfile.ISLAKE = np.int32(-1)
        self.outfile.ISICE = np.int32(24)
        self.outfile.ISURBAN = np.int32(1)
        self.outfile.ISOILWATER = np.int32(14)
        self.outfile.close()
