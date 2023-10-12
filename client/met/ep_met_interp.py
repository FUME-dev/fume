"""
Description:
Interpolation of meteorological data into case grid.
Done in two steps based on http://stackoverflow.com/questions/20915502/speedup-scipy-griddata-for-multiple-interpolations-between-two-irregular-grids
    First the qhull Delaunay triangulation is called and the weights are returned. This is done only for the first timestep/field. (slow)
    After that these weights are used for the rest of the timesteps/fields. (very fast)
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
import scipy.spatial.qhull as qhull
import pyproj
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_rtcfg,ep_dates_times
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
from  met.ep_met_data import ep_met_data

def interp_weights(xy, uv,d=2):
    tri = qhull.Delaunay(xy)
    simplex = tri.find_simplex(uv)
    vertices = np.take(tri.simplices, simplex, axis=0)
    temp = np.take(tri.transform, simplex, axis=0)
    delta = uv - temp[:, d]
    bary = np.einsum('njk,nk->nj', temp[:, :d, :], delta)
    return (vertices, np.hstack((bary, 1 - bary.sum(axis=1, keepdims=True))))


def interpolate(values, vtx, wts):
    return (np.einsum('nj,nj->n', np.take(values, vtx), wts))


def met_interp(met_data):
    """
    Returns the interpolated met data into the case grid.

        Parameters:
            met_data: list of original ep_met_data class objects
        Returns:
            met_data_i: list of interpolated ep_met_data class objects

    """

    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny

    if  'lmet_interp' not in ep_rtcfg.keys():
        log.debug('II: ep.met.met_interp: first interpolation => calculating interpolation weights for the met and case grid')
        proj4 = ep_cfg.projection_params.projection_proj4
        delx = ep_cfg.domain.delx
        dely = ep_cfg.domain.dely
        xorg = ep_cfg.domain.xorg
        yorg = ep_cfg.domain.yorg

        case_proj = pyproj.Proj(proj4)
        met_proj = pyproj.Proj(ep_cfg.input_params.met.met_proj)
        lon = np.empty((nx,ny),dtype=float)
        lat = np.empty((nx,ny),dtype=float)

        nx_met = ep_cfg.input_params.met.met_nx
        ny_met = ep_cfg.input_params.met.met_ny
        xorg_met = ep_cfg.input_params.met.met_xorg
        yorg_met = ep_cfg.input_params.met.met_yorg
        dx_met   = ep_cfg.input_params.met.met_dx
        npoints_met = nx_met*ny_met
        points_met = np.empty((nx_met * ny_met,2),dtype=float)
        ii = 0
        for j in range(ep_cfg.input_params.met.met_ny):
            for i in range(ep_cfg.input_params.met.met_nx):
                points_met[ii][0] = xorg_met+i*dx_met+dx_met/2-nx_met*dx_met/2
                points_met[ii][1] = yorg_met+j*dx_met+dx_met/2-ny_met*dx_met/2
                ii += 1

        points_case  = np.empty((nx*ny,2),dtype=float)

        ii = 0
        for j in range(ny):
            for i in range(nx):
                points_case[ii][0], points_case[ii][1] = pyproj.transform(case_proj, met_proj, xorg+i*delx+delx/2-nx*delx/2, yorg+j*dely+dely/2-ny*dely/2)
                ii += 1

        vtx, wts = interp_weights(points_met, points_case)
        ep_rtcfg['lmet_interp'] = 1
        ep_rtcfg['met_interp_vtx'] = vtx
        ep_rtcfg['met_interp_wts'] = wts
    else:
        vtx = ep_rtcfg['met_interp_vtx']
        wts = ep_rtcfg['met_interp_wts']

    met_data_i = []
    for d in met_data[:]:
        # 2D or 3D?
        l3d = (len(d.data.shape) == 3)
        if not l3d:
            values = (d.data[:,:]).flatten(order='F')
            log.fmt_debug('II: ep_met.ep_interp: regridding for {}.', d.name)
            if ep_cfg.input_params.met.met_type in ['WRF', 'ALADIN']:            
                data_i = (interpolate(values, vtx, wts)).reshape((nx, ny), order = 'F')
            else:
                data_i = (interpolate(values, vtx, wts)).reshape((nx, ny), order = 'C')
            met_data_i.append(ep_met_data(d.name, data_i))    
        else:
            nzz = d.data.shape[2]
            data_i_3d = np.empty((nx, ny, nzz), dtype=float)
            for i in range(nzz):
                values = d.data[:,:,i].flatten(order='F')
                log.fmt_debug('II: ep_met.ep_interp: regridding for {}.', d.name)
                data_i = interpolate(values, vtx, wts)
                if ep_cfg.input_params.met.met_type in ['WRF', 'ALADIN'] :
                    data_i_3d[:,:,i] = data_i.reshape((nx, ny), order = 'F')
                else:
                    data_i_3d[:,:,i] = data_i.reshape((nx, ny), order = 'C')

            met_data_i.append(ep_met_data(d.name, data_i_3d))

    return(met_data_i)
