"""
Description:
Main module to import (read and eventually interpolate) meteorological model data into FUME (e.g. for MEGAN):

We use IPCC-abbrevations for internal meteorological variable naming:

tas - temperature at surface [K]
ta  - 3D temperature [K]
qas - specific humidity at the surface [kg/kg]
qa  - 3D specific humidty
rsds - surface incident SW radiation [W/m2]
par - photosyntetically active radiation [W/m2]
pa - 3D pressure [Pa]
zf - layer interface heights [m]
uas - U-wind anemometer height (usually 10m) [m/s]
vas - V-wind anemometer height (usually 10m) [m/s]
ua - U-wind [m/s]
va - V-wind [m/s]
wndspd - 3D wind speed [m/s]
wndspd10m- wind speed at anemometer height (usually 10m) [m/s]
pr - precipiation flux [kg m-2 s-1]
pr24 - accumulated precipitation [kg m-2]
soim1 - Soil moisture [m3/m3]top cm (according to MEGAN input meteorology)
soilt - Soil temperature [K] top cm
etc.
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

from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times,  ep_rtcfg
from lib.ep_geo_tools import *
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
from importlib import import_module
from  met.ep_met_netcdf import write_netcdf
from met.ep_met_interp import interp_weights, interpolate, met_interp

def get_met(met):
    """
    Main wrapping function to import meteorological data from one of ALADIN, WRF or RegCM
    """
    if len(met) != 0:
        log.fmt_debug('Getting meteorology for {}', met)
        met_type  = ep_cfg.input_params.met.met_type
        if met_type == 'ALADIN':
            mtls = 'aladin' 
        elif met_type == 'WRF':
            mtls = 'wrf'   
        elif met_type == 'RegCM':
            mtls = 'regcm'
        else:
            log.debug('EE: Unknown met model.')
            raise ValueError

        dts = ep_dates_times()
        mod_name = 'met.{}.ep_{}'.format(mtls,mtls)
        func_name = 'ep_{}_met'.format(mtls)
        mod_obj = import_module(mod_name)
        func_obj = getattr(mod_obj, func_name)
        met_data = func_obj(met, dts)
        
        if ep_cfg.input_params.met.met_interp:
            ep_rtcfg['met'] = [met_interp(m) for m in met_data]
        else:
            ep_rtcfg['met'] = met_data

        if ep_cfg.input_params.met.met_netcdf:
            log.fmt_debug('II: writing met data to {} for testing purposes.', ep_cfg.input_params.met.met_netcdf_file)
            write_netcdf(ep_rtcfg['met'], dts, ep_cfg.input_params.met.met_netcdf_file, exclude = ep_cfg.input_params.met.met_netcdf_exclude)
    else:
        log.debug('No meteorology needed to import.')     
