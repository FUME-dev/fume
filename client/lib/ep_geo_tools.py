"""
Description: projection conversion classes and functions

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
import pyproj
from pygrib import open as gribopen
from netCDF4 import Dataset
import lib.ep_io_fortran as fio
from osgeo import osr, ogr
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)

class Domain:
    def __init__(self, nx, ny, nz, delx, dely, xorg, yorg, proj):
        self.nx = nx
        self.ny = ny
        self.nz = nz
        self.delx = delx
        self.dely = dely
        self.xorg = xorg
        self.yorg = yorg
        if isinstance(proj,pyproj.Proj):
            self.proj = proj
            self.nx = nx
            self.ny = ny
            self.nz = nz
            self.delx = delx
            self.dely = dely
            self.xorg = xorg
            self.yorg = yorg
        else:
            log.error('EE: no valid projection provided!')
            raise ValueError


def get_projection_params(srs):
        if srs.IsGeographic():
            proj = 'LATLON'
            p_alp = 0.
            p_bet = 0.
            p_gam = 0.
            YCENT = 0.
            XCENT = 0.
        else:
            srs_proj = srs.GetAttrValue("PROJECTION")
    
            if srs_proj == "Lambert_Conformal_Conic_2SP": # lcc
                proj = 'LAMBERT'
                p_alp = srs.GetProjParm("standard_parallel_1")
                p_bet = srs.GetProjParm("standard_parallel_2")
                p_gam = srs.GetProjParm("central_meridian")
                YCENT = srs.GetProjParm("latitude_of_origin")
                XCENT = srs.GetProjParm("central_meridian")
            elif srs_proj == "Lambert_Conformal_Conic_1SP": # lcc
                proj = 'LAMBERT'
                p_alp = srs.GetProjParm("latitude_of_origin")
                p_bet = p_alp
                p_gam = srs.GetProjParm("central_meridian")
                YCENT = p_alp
                XCENT = p_gam
            elif srs_proj == "Transverse_Mercator": # tmerc
                proj = 'MERCATOR'
                p_alp = srs.GetProjParm("latitude_of_origin")
                p_bet = srs.GetProjParm("central_meridian")
                p_gam = p_bet
                XCENT = p_bet
                YCENT = p_alp
            elif srs_proj == "Mercator_1SP": # merc
                proj = 'EMERCATOR'
                p_alp = 0.
                p_bet = 0.
                p_gam = srs.GetProjParm("central_meridian")
                XCENT = p_gam
                YCENT = 0.
            elif srs_proj == "Mercator_2SP": # merc
                proj = 'EMERCATOR'
                p_alp = srs.GetProjParm("standard_parallel_1")
                p_bet = 0.
                p_gam = srs.GetProjParm("central_meridian")
                XCENT = p_gam
                YCENT = 0.
            elif srs_proj == "Polar_Stereographic": # stere
                proj = 'POLAR'
                p_gam = srs.GetProjParm("central_meridian")
                XCENT = p_gam
                YCENT = srs.GetProjParm("latitude_of_origin")
                p_alp = YCENT/abs(YCENT)
                p_bet = 90.0*p_alp
            elif srs_proj == "Oblique_Stereographic": # sterea
                proj = 'STEREO'
                YCENT = srs.GetProjParm("latitude_of_origin")
                XCENT = srs.GetProjParm("central_meridian")
                p_alp = YCENT
                p_bet = XCENT
                p_gam = XCENT
            
        return(proj, XCENT, YCENT, p_alp, p_bet, p_gam)


def projection_parameters_from_srid(srid):
    """ Function that returns the internal projection name and individual projection parameters XCENT, YCENT, p_alp, p_bet, p_gam from srid or PROJ4 string using the osgeo.osr class and its methods"""
    srs = osr.SpatialReference()
    ret = srs.ImportFromEPSG(srid)
    if ret != 0: # ogr.OGRERR_NONE:
        from lib.ep_libutil import ep_connection
        cur = ep_connection.cursor()
        cur.execute('SELECT trim(proj4text) FROM spatial_ref_sys WHERE srid=%s',(srid,))
        srs.ImportFromProj4(cur.fetchone()[0])

    return get_projection_params(srs)


def projection_parameters_from_proj4(proj4str):
    """ Function that returns the internal projection name and individual projection parameters XCENT, YCENT, p_alp, p_bet, p_gam from srid or PROJ4 string using the osgeo.osr class and its methods"""
    srs = osr.SpatialReference()
    srs.ImportFromProj4(proj4str)

    return get_projection_params(srs)


def get_projection_domain_params(ifilename, ftype='MCIP', endian='big'):

    """ Extracts projection and domain parameters from different filetypes (CMAQ, WRF, CAMx, ALADIN(GRIB) ...)  """
    # default is the CMAQ IO/API format
    try:
        if ftype == 'MCIP' or ftype == 'WRF' or ftype == 'RegCM':
            ifile = Dataset(ifilename, 'r', format='NETCDF4')
        elif ftype == 'CAMx':
            ifile = open(ifilename, mode='rb')
        elif ftype == 'ALADIN':
            ifile = gribopen(ifilename)
        else:
            log.error('EE: Unknown file format in get_projection_domain:', ftype)
            raise ValueError

        if ftype == 'ALADIN':
            ifile.seek(0)
            grbmsg=ifile.message(1)
            projparams = grbmsg.projparams
            pjargs = []
            for key,value in projparams.items():
                pjargs.append('+'+key+"="+str(value)+' ')
            proj4string = ''.join(pjargs)
            proj, XCENT, YCENT, p_alp, p_bet, p_gam = projection_parameters_from_srid_or_proj4(proj4str=proj4string)
            nx, ny, nz, delx, dely = grbmsg.Nx, grbmsg.Ny, None, grbmsg.DxInMetres, grbmsg.DyInMetres
            lat0 = grbmsg.latitudeOfFirstGridPointInDegrees
            lon0 = grbmsg.longitudeOfFirstGridPointInDegrees
            myproj = pyproj.Proj(projparams)
            xorg, yorg = myproj(lon0,lat0)
            if projparams['proj'] == 'lcc':
                projection = "LAMBERT"
            elif projparams['proj'] == 'tmerc':
                projection = "MERCATOR"
            elif projparams['proj'] == 'merc':
                projection = "EMERCATOR"
            elif projparams['proj'] == 'stere':
                projection = "POLAR"
            elif projparams['proj'] == 'sterea':
                projection = "STEREO"
        elif ftype == 'MCIP':
            nx, ny, nz, delx, dely, xorg, yorg = ifile.NCOLS, ifile.NROWS, ifile.NLAYS, ifile.XCELL, ifile.YCELL, ifile.XORIG, ifile.YORIG
            XCENT, YCENT, p_alp, p_bet, p_gam = ifile.XCENT, ifile.YCENT, ifile.P_ALP, ifile.P_BET, ifile.P_GAM

            if ifile.GDTYP == 1:
                projection = "LATLON"
            elif ifile.GDTYP == 2:
                projection = "LAMBERT"
            elif ifile.GDTYP == 3:
                projection = "MERCATOR"
            elif ifile.GDTYP == 4:
                projection = "STEREO"
            elif ifile.GDTYP == 5:
                projection = "UTM"
            elif ifile.GDTYP == 6:
                projection = "POLAR"
            elif ifile.GDTYP == 7:
                projection = "EMERCATOR"
            else:
                log.error('EE: MCIP input has unsupported projection. Check the GDTYP value!')
                raise ValueError


        elif ftype == 'WRF':
            delx, dely =  ifile.DX, ifile.DY

            nx = getattr(ifile,"WEST-EAST_GRID_DIMENSION")
            ny = getattr(ifile,"SOUTH-NORTH_GRID_DIMENSION")
            nz = getattr(ifile,"BOTTOM-TOP_GRID_DIMENSION")

            nx -= 1
            ny -= 1
            nz -= 1

            xorg, yorg = -(nx/2.0*delx), -(ny/2.0*dely)
            #1=Lambert, 2=polar stereographic, 3=mercator, 6=lat-lon
            if ifile.MAP_PROJ == 1:
                projection = "LAMBERT"
                XCENT, YCENT, p_alp, p_bet, p_gam = ifile.CEN_LON, ifile.CEN_LAT, ifile.TRUELAT1, ifile.TRUELAT2, ifile.STAND_LON
            elif ifile.MAP_PROJ == 2:
                projection = "POLAR"
                XCENT, YCENT, p_alp, p_bet, p_gam = ifile.CEN_LON, ifile.CEN_LAT, ifile.TRUELAT1/abs(ifile.TRUELAT1), ifile.TRUELAT1, ifile.STAND_LON
            elif ifile.MAP_PROJ == 3:
                projection = "EMERCATOR"
                XCENT, YCENT, p_alp, p_bet, p_gam = ifile.CEN_LON, ifile.CEN_LAT, ifile.TRUELAT1, None, ifile.STAND_LON
            elif ifile.MAP_PROJ == 6:
                projection = "LATLON"
                XCENT, YCENT, p_alp, p_bet, p_gam = 0.0, 0.0, None, None, None
        elif ftype == 'CAMx':
           # first record
            ifile.seek(0)
            var = fio.read_record (ifile, endian, '40s240siiifif')
            # second record - this is what we need
            plon,plat,iutm,xorg,yorg,delx,dely,nx,ny,nz,iproj,istag,tlat1,tlat2,rdum = fio.read_record (ifile, endian, 'ffiffffiiiiifff')
            if iproj == 0:
                projection = "LATLON"
                XCENT, YCENT, p_alp, p_bet, p_gam = 0.0, 0.0, None, None, None
            elif iproj == 1:
                projection = "UTM"
                XCENT, YCENT, p_alp, p_bet, p_gam = None, None, iutm, None, None
            elif iproj == 2:
                projection = "LAMBERT"
                XCENT, YCENT, p_alp, p_bet, p_gam = plon, plat, tlat1, tlat2, plon
            elif iproj == 3:
                projection = "STEREO"
                XCENT, YCENT, p_alp, p_bet, p_gam = plon, plat, plat, plon, None
            elif iproj == 4:
                projection = "POLAR"
                XCENT, YCENT, p_alp, p_bet, p_gam = plon, plat, tlat1/abs(tlat1) , tlat1, plon
            elif iproj == 5:
                projection = "EMERCATOR"
                XCENT, YCENT, p_alp, p_bet, p_gam = plon, plat, tlat1, None, plon
        elif ftype == 'RegCM':
            nx = len(ifile.dimensions["jx"])
            ny = len(ifile.dimensions["iy"])
            nz = len(ifile.dimensions["kz"])
            delx = float(ifile.grid_size_in_meters)
            dely = delx
            xorg = -nx*delx/2.0
            yorg = -ny*dely/2.0
            projregcm = ifile.projection
            latitude_of_projection_origin = ifile.latitude_of_projection_origin
            longitude_of_projection_origin= ifile.longitude_of_projection_origin
            standard_parallel1, standard_parallel2 = ifile.standard_parallel
            if projregcm == "LAMCON":
                projection = "LAMBERT"
                XCENT, YCENT, p_alp, p_bet, p_gam = longitude_of_projection_origin, latitude_of_projection_origin, standard_parallel1, standard_parallel2, longitude_of_projection_origin
            elif  projregcm == "NORMER":
                projection = "EMERCATOR"
                XCENT, YCENT, p_alp, p_bet, p_gam = longitude_of_projection_origin, latitude_of_projection_origin, 0., None , longitude_of_projection_origin
            elif  projregcm == "ROTMER":
                projection = "MERCATOR"
                XCENT, YCENT, p_alp, p_bet, p_gam = longitude_of_projection_origin, latitude_of_projection_origin, longitude_of_projection_origin, latitude_of_projection_origin, latitude_of_projection_origin
            else:
                raise ValueError('EE: Unknown projection in RegCM {}'.format(projregcm))
        else:
            log.error('EE: Unknown format', ftype)
            raise ValueError
    except IOError:
        log.fmt_debug('EE: Error opening/reading file {}', ifilename)
        raise

    # xorg and yorg will mean the coordinates of the domain center
    xorg = xorg + nx*delx/2.0
    yorg = yorg + ny*dely/2.0
    return ( nx, ny, nz, delx, dely, xorg, yorg, projection, XCENT, YCENT, p_alp, p_bet, p_gam     )



def create_projection(projection, XCENT, YCENT, p_alp, p_bet, p_gam, **kwargs):

    """ From projection parameters return pyproj projection object. """
    if   projection == 'UTM': # Universal Transvers Mercator
        proj_string='+proj=utm  +zone='+str(p_alp)+' +units=m'
    elif projection == 'LAMBERT': # Lamber Conformal Conic projection
        proj_string='+proj=lcc +lat_1='+str(p_alp)+' +lat_2='+str(p_bet)+' +y_0='+str(XCENT)+' +lon_0='+str(p_gam)+' +units=m'
    elif projection == 'STEREO': # general stereographic projection for aby point on the Earth
        proj_string='+proj=sterea  +lon_0='+str(p_bet)+' +lat_0='+str(p_alp)+' +x_0='+str(p_gam)+' +y_0'+str(XCENT)

    elif projection == 'POLAR': # stereographic projection plane paralel to the equatorial plane
        proj_string='+proj=stere +lat_ts='+str(p_alp)+'  +lon_0='+str(p_bet)+' +k_0=1.0 +x_0='+str(XCENT)+' +y_0='+str(YCENT)
    elif projection == 'EMERCATOR': # equatorial mercator 1 or 2 standard paralels
        proj_string = '+proj=merc +lat_ts='+str(p_alp)+' +lon_0='+str(p_gam)
    elif projection == 'MERCATOR': # general mercator - i.e. any angle between the cylinder and the north pole axis
        proj_string = '+proj=tmerc +lat_0='+str(p_alp)+' +lon_0='+str(p_bet)+' +k_0=1.0 +x_0='+str(XCENT)+' +y_0='+str(YCENT)
    else:
        log.error('EE: so far we suuport only Lambert Conic Conformal, UTM, Equatorial Secant Mercator and Longlat projections.')
        raise ValueError

    for k,v in kwargs.items():
        proj_string += ' +{}={}'.format(k,v)

    myproj = pyproj.Proj(proj_string)
    return (myproj)

def regrid(nx0, ny0, delx0, dely0, xorg0, yorg0, orig_projection, nx, ny, delx, dely, xorg, yorg, dest_projection):

    """ Calculates weights to new projection and domain using the inverse distance to neighbours method """

    xorg0 = xorg0 - nx0*delx0/2.0
    yorg0 = yorg0 - ny0*dely0/2.0

    xorg = xorg - nx*delx/2.0
    yorg = yorg - ny*dely/2.0


    # get origin and destination projections
    myproj0 = orig_projection   #create_projection(projection0, XCENT0, YCENT0, p_alp0, p_bet0, p_gam0)
    myproj  = dest_projection   #create_projection(projection , XCENT , YCENT , p_alp , p_bet , p_gam )
    # first check if the new domain is smaller than the mother (zero) domain
    try:
    # SW corner

        lon01, lat01 = myproj0(xorg0,yorg0,inverse=True)
        lon1 ,  lat1 = myproj (xorg ,yorg ,inverse=True)
        if (lat1 <= lat01 or lon1 <= lon01):
            log.error('EE: inner domain spawns outside of the outer domain: lat01, lon01 = '+str(lat01)+', '+str(lon01))
            log.error('                                                     lat1,   lon1 = '+str(lat1) +', '+str(lon1))
            raise ValueError

    # SE corner

        lon02, lat02 = myproj0(xorg0+nx0*delx0,yorg0,inverse=True)
        lon2 ,  lat2 = myproj (xorg+nx*delx   ,yorg ,inverse=True)
        if (lat2 <= lat02 or lon2 >= lon02):
            log.error('EE: inner domain spawns outside of the outer domain: lat02, lon02 = '+str(lat02)+', '+str(lon02))
            log.error('                                                     lat2,   lon2 = '+str(lat2) +', '+str(lon2))
            raise ValueError

    # NE corner

        lon03, lat03 = myproj0(xorg0+nx0*delx0,yorg0+ny0*dely0,inverse=True)
        lon3 ,  lat3 = myproj (xorg+nx*delx   ,yorg+ny*dely   ,inverse=True)
        if (lat3 >= lat03 or lon3 >= lon03):
            log.error('EE: inner domain spawns outside of the outer domain: lat03, lon03 = '+str(lat03)+', '+str(lon03))
            log.error('                                                     lat3,   lon3 = '+str(lat3) +', '+str(lon3))
            raise ValueError

    #NW corner

        lon04, lat04 = myproj0(xorg0,yorg0+ny0*dely0,inverse=True)
        lon4 ,  lat4 = myproj (xorg ,yorg +ny*dely  ,inverse=True)
        if (lat4 >= lat04 or lon4 <= lon04):
            log.error('EE: inner domain spawns outside of the outer domain: lat04, lon04 = '+str(lat04)+', '+str(lon04))
            log.error('                                                     lat4,   lon4 = '+str(lat4) +', '+str(lon4))
            raise ValueError

# initialize the final mapping matrix

        matrix  = np.empty( (nx+1,ny+1), dtype=object)

# scan trough the final grid
# for edges check if the mother domain contains ihte inner one

        for i in 1, nx:
            for j in range(1, ny+1):
                # calculate the lat lon
                x = xorg + (i-1)*delx
                y = yorg + (j-1)*dely
                lon, lat = myproj (x,y, inverse=True)
                x0,y0    = myproj0(lon,lat )
                if (x0 <= xorg0 or x0 >= xorg0+nx0*delx0 or y0 <= yorg0 or y0 >= yorg0+ny0*dely0):
                    log.error('EE: point (i,j) ='+str(i)+','+str(j)+' is outside of the mother domain: x0,y0 '+str(x0)+' ,'+str(y0), lat,lon )
                    raise ValueError

                i0 = int((x0-xorg0)/delx0)+1
                j0 = int((y0-yorg0)/dely0)+1

                x0_1, y0_1 = (i0-1)*delx0+xorg0, (j0-1)*dely0+yorg0
                x0_2, y0_2 = x0_1+delx0, y0_1
                x0_3, y0_3 = x0_1+delx0, y0_1+dely0
                x0_4, y0_4 = x0_1, y0_1+dely0

                d1 = ( (x0 - x0_1)**2 + (y0 - y0_1)**2  )**(0.5)
                d2 = ( (x0 - x0_2)**2 + (y0 - y0_2)**2  )**(0.5)
                d3 = ( (x0 - x0_3)**2 + (y0 - y0_3)**2  )**(0.5)
                d4 = ( (x0 - x0_4)**2 + (y0 - y0_4)**2  )**(0.5)

                matrix[i,j] = (i0,j0,max(d1,1.), max(d2,1.), max(d3,1.), max(d4,1.))

# for edges check if the mother domain contains ihte inner one
        for i in range(1, nx+1):
            for j in 1, ny:
                # calculate the lat lon
                x = xorg + (i-1)*delx
                y = yorg + (j-1)*dely
                lon, lat = myproj (x,y, inverse=True)
                x0,y0    = myproj0(lon,lat )
                if (x0 <= xorg0 or x0 >= xorg0+nx0*delx0 or y0 <= yorg0 or y0 >= yorg0+ny0*dely0):
                    log.error('EE: point (i,j) ='+str(i)+','+str(j)+' is outside of the mother domain: x0,y0 '+str(x0)+' ,'+str(y0), lat,lon )
                    raise ValueError

                i0 = int((x0-xorg0)/delx0)+1
                j0 = int((y0-yorg0)/dely0)+1

                x0_1, y0_1 = (i0-1)*delx0+xorg0, (j0-1)*dely0+yorg0
                x0_2, y0_2 = x0_1+delx0, y0_1
                x0_3, y0_3 = x0_1+delx0, y0_1+dely0
                x0_4, y0_4 = x0_1, y0_1+dely0

                d1 = ( (x0 - x0_1)**2 + (y0 - y0_1)**2  )**(0.5)
                d2 = ( (x0 - x0_2)**2 + (y0 - y0_2)**2  )**(0.5)
                d3 = ( (x0 - x0_3)**2 + (y0 - y0_3)**2  )**(0.5)
                d4 = ( (x0 - x0_4)**2 + (y0 - y0_4)**2  )**(0.5)

                matrix[i,j] =  (i0,j0,max(d1,1.), max(d2,1.), max(d3,1.), max(d4,1.))


        for i in range(2, nx):
            for j in range(2, ny):
                # calculate the lat lon
                x = xorg + (i-1)*delx
                y = yorg + (j-1)*dely
                lon, lat = myproj (x,y, inverse=True)
                x0,y0    = myproj0(lon,lat )

                i0 = int((x0-xorg0)/delx0)+1
                j0 = int((y0-yorg0)/dely0)+1

                x0_1, y0_1 = (i0-1)*delx0+xorg0, (j0-1)*dely0+yorg0
                x0_2, y0_2 = x0_1+delx0, y0_1
                x0_3, y0_3 = x0_1+delx0, y0_1+dely0
                x0_4, y0_4 = x0_1, y0_1+dely0

                d1 = ( (x0 - x0_1)**2 + (y0 - y0_1)**2  )**(0.5)
                d2 = ( (x0 - x0_2)**2 + (y0 - y0_2)**2  )**(0.5)
                d3 = ( (x0 - x0_3)**2 + (y0 - y0_3)**2  )**(0.5)
                d4 = ( (x0 - x0_4)**2 + (y0 - y0_4)**2  )**(0.5)

                matrix[i,j] = (i0,j0,max(d1,1.), max(d2,1.), max(d3,1.), max(d4,1.))


        return(matrix[1:,1:])

    except ValueError as e:

        raise

def vert_interp(field0,zht0,zht,mpoints=True):

    """ vertically interpolates the column vector field0 using column vectors of layer interface heights of the original (zht0) and detination vertical grid (zht); mpoints=true means the input field is defined on layer midpoints and not on layer interfaces (mpoints=False) """

    try:
        nz0   = np.size(field0)
        nz0_1 = np.size(zht0)
        if nz0 != nz0_1:
            log.error('EE: the input field vertical gridding does not match the grid definition')
            raise ValueError

        nz =  np.size(zht)

        field0i = np.empty((nz0),dtype=float)
        field   = np.empty((nz),dtype=float)
        fieldi  = np.empty((nz),dtype=float)



        field0i = field0
        if mpoints == True: # first interpolate the original field0 to layer interface heights
            for j in range(nz0-1):
                field0i[j] = 0.5*(field0[j]+field0[j+1])
            field0i[nz0-1] = field0[nz0-1]

        for i in range(nz):
            if zht0[0] >= zht[i]:
                fieldi[i] = field0i[0]
            else:
                j = 1
                while (j <= nz0-1) and (zht0[j] < zht[i]):
                    j += 1
                if zht0[j] >= zht[i]:
                    fieldi[i] = field0i[j-1] + ( field0i[j] - field0i[j-1] ) * ( zht[i] - zht0[j-1] ) / (zht0[j] - zht0[j-1] )
                else:
                    fieldi[i] = field0i[nz0-1]

        field = fieldi

        if mpoints == True: # interpolate from interfaces to midpoints
            for i in range(1,nz):
                field[i] = 0.5*(fieldi[i-1]+fieldi[i])
            field[0] = fieldi[0]
        return (field)

    except ValueError as ve:

        raise

def grid_desc(cfg,cdo_griddesc_file='griddesc',m3_griddesc_file=None):
    # create the griddesc file for CDO corresponding to the case grid
    nx = cfg.domain.nx
    ny = cfg.domain.ny
    nz = cfg.domain.nz
    delx = cfg.domain.delx
    dely = cfg.domain.dely
    xorg = cfg.domain.xorg
    yorg = cfg.domain.yorg

    from lib.ep_libutil import ep_rtcfg
    proj = ep_rtcfg['projection_params']['proj']
    p_alp = ep_rtcfg['projection_params']['p_alp']
    p_bet = ep_rtcfg['projection_params']['p_bet']
    p_gam = ep_rtcfg['projection_params']['p_gam']
    XCENT = ep_rtcfg['projection_params']['lon_central']
    YCENT = ep_rtcfg['projection_params']['lat_central']
    proj4str = ep_rtcfg['projection_params']['proj4string']
    
    case_proj = pyproj.Proj(proj4str)

    wgs84_proj = pyproj.Proj(init="epsg:4326")

    gridsize  = nx*ny
    xsize = nx
    ysize = ny
    xvals = np.empty((nx,ny),dtype=float)
    yvals = np.empty((nx,ny),dtype=float)
    xbounds = np.empty((nx,ny,4),dtype=float)
    ybounds = np.empty((nx,ny,4),dtype=float)



    for i in range(nx):
        for j in range(ny):
            # calculate the mid point
            lon, lat = pyproj.transform(case_proj,wgs84_proj,(xorg+i*delx+delx/2-nx*delx/2),(yorg+j*dely+dely/2-ny*dely/2))
            xvals[i,j], yvals[i,j] = lon, lat

            # SW corner
            lon, lat = pyproj.transform(case_proj,wgs84_proj,(xorg+i*delx-nx*delx/2),(yorg+j*dely+ny*dely/2))
            xbounds[i,j,0], ybounds[i,j,0] = lon, lat
            # SE corner
            lon, lat = pyproj.transform(case_proj,wgs84_proj,(xorg+i*delx+delx-nx*delx/2),(yorg+j*dely+ny*dely/2))
            xbounds[i,j,1], ybounds[i,j,1] = lon, lat
            # NE corner
            lon, lat = pyproj.transform(case_proj,wgs84_proj,(xorg+i*delx+delx-nx*delx/2),(yorg+j*dely+dely-ny*dely/2))
            xbounds[i,j,2], ybounds[i,j,2] = lon, lat
            # NW corner
            lon, lat = pyproj.transform(case_proj,wgs84_proj,(xorg+i*delx+nx*delx/2),(yorg+j*dely+dely+ny*dely/2))
            xbounds[i,j,3], ybounds[i,j,3] = lon, lat

    ofile = open(cdo_griddesc_file,'w')
    ofile.write('gridtype = curvilinear\n')
    ofile.write('gridsize = {}\n'.format(gridsize))
    ofile.write('xsize    = {}\n'.format(xsize))
    ofile.write('ysize    = {}\n'.format(ysize))

    ofile.write('xvals    = {}\n'.format(' '.join(map(str, xvals[:,0]))))
    for i in range(1,ysize):
        
        ofile.write('           {}\n'.format(' '.join(map(str, xvals[:,i]))))

    for i in range(0,ysize):
        for j in range(0,xsize):

            if ( i == 0 and j == 0 ):
                lbegin = 'xbounds    = {}\n'
            else:
                lbegin = '             {}\n'
            ofile.write(lbegin.format(' '.join(map(str, xbounds[j,i,:]))))


    ofile.write('yvals    = {}\n'.format(' '.join(map(str, yvals[:,0]))))
    for i in range(1,ysize):
        ofile.write('           {}\n'.format(' '.join(map(str, yvals[:,i]))))

    for i in range(0,ysize):
        for j in range(0,xsize):

            if ( i == 0 and j == 0 ):
                lbegin = 'ybounds    = {}\n'
            else:
                lbegin = '             {}\n'
            ofile.write(lbegin.format(' '.join(map(str, ybounds[j,i,:]))))

    ofile.close()

    if m3_griddesc_file != None:    # write GRIDDESC file for MEGAN
        if  proj == "LATLON":
            GDTYP = 1
        elif  proj == "UTM":
            GDTYP = 5
        elif  proj == "LAMBERT":
            GDTYP = 2
        elif  proj == "STEREO":
            GDTYP = 4
        elif  proj == "POLAR":
            GDTYP = 6
        elif  proj == "EMERCATOR":
            GDTYP = 7
        elif  proj == "MERCATOR":
            GDTYP = 3
        else:
            log.error( "EE: projection not known. Exit.....")
            raise ValueError

        grid_name = cfg.domain.grid_name
        proj_name = grid_name
        ofile = open(m3_griddesc_file,'w')
        ofile.write('!  coords --2 lines:  name; type, P-alpha, P-beta, P-gamma, xcent, ycent\n')
        ofile.write('\'{}\'\n'.format(proj_name))
        ofile.write('     {}, {}, {}, {}, {}, {} \n'.format(GDTYP,p_alp,p_bet,p_gam,XCENT,YCENT))
        ofile.write('\' \' ! end coords.')
        ofile.write('\' \'\n')
        ofile.write('\'{}\'\n'.format(grid_name))
        ofile.write('\'{}\' {} {} {} {} {} {} {}\n'.format(proj_name,xorg-nx*delx/2.0, yorg-ny*dely/2.0, delx, dely, nx, ny, 1))
        ofile.write('\' \' ! end grids.')

        ofile.close()

    return(xvals, yvals)
