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

Copyright 2014-2026 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
Copyright 2014-2026 Charles University, Faculty of Mathematics and Physics, Prague, Czech Republic
Copyright 2014-2026 Czech Hydrometeorological Institute, Prague, Czech Republic
Copyright 2014-2017 Czech Technical University in Prague, Czech Republic
"""

import numpy as np
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_dates_times,  ep_rtcfg
from lib.ep_geo_tools import *
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
from importlib import import_module
from  met.ep_met_netcdf import write_netcdf_header, write_netcdf_timestep, check_netcdf_file, read_netcdf_timestep
from met.ep_met_interp import interp_weights, interpolate, met_interp, met_create_vinterp
from  met.ep_met_data import ep_met_data

vinterp_vars = ['ta','qa','pa','ua','va','wndspd'] # zf only for testing purposes

def get_met():
    """
    Main wrapping function to import meteorological data from one of ALADIN, WRF or RegCM
    """
    # First test reading the saved processed meteo file
    log.fmt_debug('Checking saved processed meteorology')
    met_vars = list(ep_rtcfg['required_met'])
    dts = ep_dates_times()
    if check_netcdf_file(dts):
        log.fmt_debug('Saved meteorology satisfies conditions')
        return

    # saved processed meteorology is not available, read and interpolate from mesoscale files
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

    mod_name = 'met.{}.ep_{}'.format(mtls,mtls)
    mod_obj = import_module(mod_name)

    # read needed meteorological model datetimes and associated files
    func_name = 'ep_{}_met_modeldates'.format(mtls)
    func_obj = getattr(mod_obj, func_name)
    modeldates = func_obj(dts)

    # function for reading of one fume timestep
    func_name = 'ep_{}_met'.format(mtls)
    func_obj = getattr(mod_obj, func_name)
    first = True
    for it, dt in enumerate(dts):
        # read individual timestep
        met_data = func_obj(dt, modeldates, met_vars)

        if ep_cfg.input_params.met.met_interp:
            log.fmt_debug('Interpolation of the timestep {}: {}', it, dt)
            # Interpolate horizontally
            met_data = met_interp(met_data)

            # Interpolate vertically
            if ep_cfg.input_params.met.met_vinterp:
                zf, = [v for v in met_data if v.name=='zf']
                hght = ep_rtcfg.model_levels
                interp = met_create_vinterp(hght, zf.data)
                vdata = []
                for v in met_data:
                    if v.name == 'zf':
                        # Assign prescibed model heights.
                        # Note: Tested that zf interpolates correctly to prescribed heights (with extrapolated
                        # bottom) when using interp().
                        nx, ny, nzm = v.data.shape
                        arr = np.empty((nx, ny, len(hght)), v.data.dtype)
                        arr[:,:,:] = hght[np.newaxis, np.newaxis, :]
                        vdata.append(ep_met_data(v.name, arr))
                    elif v.name in vinterp_vars:
                        vdata.append(ep_met_data(v.name, interp(v.data)))
                    else:
                        vdata.append(v)

                met_data = vdata

        if first:
            # write netcdf file header
            ncf = write_netcdf_header(met_data, dts, ep_cfg.input_params.met.met_netcdf_file)
            first = False

        # write timestep to netcdf file
        write_netcdf_timestep(met_data, it, dt, ncf)

    # close ncf file
    ncf.close()
