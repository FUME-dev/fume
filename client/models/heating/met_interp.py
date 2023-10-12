"""
Description: Interpolating done in two points following

http://stackoverflow.com/questions/20915502/speedup-scipy-griddata-for-multiple-interpolations-between-two-irregular-grids

Thanks to Jaime!

    First the qhull Delaunay triangulation is called and the weights are returned. This is done only for the first interpolation. (slow)
    After these weights are used to each data. (superfast)

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
            

def met_interp(met_data, cfg):
    """Interpolating data into the case grid """

    nx = ep_cfg.domain.nx
    ny = ep_cfg.domain.ny

    log.debug('II: interpolation of heating sum to the case grid')

    proj4 = ep_cfg.projection_params.projection_proj4
    delx = ep_cfg.domain.delx
    dely = ep_cfg.domain.dely
    xorg = ep_cfg.domain.xorg
    yorg = ep_cfg.domain.yorg

    case_proj = pyproj.Proj(proj4)
    met_proj = pyproj.Proj(cfg.met_proj)
    lon = np.empty((nx,ny),dtype=float)
    lat = np.empty((nx,ny),dtype=float)

    nx_met = cfg.met_nx
    ny_met = cfg.met_ny
    xorg_met = cfg.met_xorg
    yorg_met = cfg.met_yorg
    dx_met   = cfg.met_dx
    npoints_met = nx_met*ny_met
    if cfg.met_interp_type == '2d':
        points_met = np.empty((nx_met * ny_met,2),dtype=float)
        ii = 0
        for j in range(ny_met):
            for i in range(nx_met):
                points_met[ii][0] = xorg_met+i*dx_met+dx_met/2-nx_met*dx_met/2
                points_met[ii][1] = yorg_met+j*dx_met+dx_met/2-ny_met*dx_met/2
                ii += 1

        points_case  = np.empty((nx*ny,2),dtype=float)
        ii = 0
        for j in range(ny):
            for i in range(nx):
                points_case[ii][0], points_case[ii][1] = pyproj.transform(case_proj, met_proj, xorg+i*delx+delx/2-nx*delx/2, yorg+j*dely+dely/2-ny*dely/2)
                ii += 1

    elif cfg.met_interp_type == 'cn':
        points_case_x = np.empty((nx*ny),dtype=float)
        points_case_y = np.empty((nx*ny),dtype=float)
        coords_case  = np.empty((nx,ny),dtype=object)
        ii = 0
        for j in range(ny):
            for i in range(nx):
                points_case_x[ii], points_case_y[ii] = pyproj.transform(case_proj, met_proj, xorg+i*delx+delx/2-nx*delx/2, yorg+j*dely+dely/2-ny*dely/2)
                coords_case[i,j] =  (points_case_x[ii], points_case_y[ii])
                ii += 1


    d = met_data
    values = (d.data[:,:]).flatten(order='F')
    if cfg.met_interp_type in ['2d']:
        vtx, wts = interp_weights(points_met, points_case)
        data_i = (interpolate(values, vtx, wts)).reshape((nx, ny), order = 'F')

    elif cfg.met_interp_type == 'cn':
        log.debug('II: starting closest neighbour inerpoltation....')
        data_i = np.zeros((nx, ny))
        for j in range(ny):
            for i in range(nx):
                met_i = int((coords_case[i,j][0] - xorg_met + nx_met*dx_met/2)/dx_met)
                met_j = int((coords_case[i,j][1] - yorg_met + ny_met*dx_met/2)/dx_met)
                data_i[i,j] = d.data[met_i, met_j]

    return(ep_met_data(d.name, data_i))    


