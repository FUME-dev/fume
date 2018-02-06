import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as mpl_polygon 
import shapely.geometry as sh_geom
from fiona import collection

def get_polygons_from_file(infilename):
    infile = collection(infilename, 'r')
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
