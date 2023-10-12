"""
Description: miscellaneous helper functions

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

import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as mpl_polygon 
import shapely.geometry as sh_geom
from fiona import collection
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)


def get_polygons_from_file(infilename):
    try:
        infile = collection(infilename, 'r')
    except IOError:
        log.debug('No such file or directory: ', infilename)
        return []
    return [sh_geom.shape(g['geometry']) for g in infile]


def draw_polygon_on_map(polygon, m, alpha, facecolor='none'):
    lons = [c[0] for c in polygon.exterior.coords]
    lats = [c[1] for c in polygon.exterior.coords]
    x, y = m(lons, lats)
    poly = mpl_polygon(list(zip(x,y)), facecolor=facecolor, alpha=alpha,
                       edgecolor='#dddddd')
    plt.gca().add_patch(poly)


def draw_multipolygon_on_map(multipolygon, m, alpha=1.0, facecolor='none'):
    if multipolygon.geom_type=='Polygon':
        draw_polygon_on_map(multipolygon, m, alpha, facecolor=facecolor)
    else:
        for geom in multipolygon.geoms: 
            draw_polygon_on_map(geom, m, alpha, facecolor=facecolor)
