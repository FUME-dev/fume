"""
Description: shape-file import

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

import osgeo.ogr
from lib.ep_libutil import ep_connection, ep_get_proj4_srid
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)

def osgeo_str_to_utf(s, encoding='Latin-1'):
    try:
        return bytes([ord(c) for c in s]).decode(encoding)
    except ValueError:
        return s

def ep_shp2postgis(shpfile, shpcoding='Latin-1', shpsrid=None, proj4=None, conn=None, schema=None, tablename=None, tabletemp=False, tablesrid=None, geomdim=2, makevalid=True):
    """ Imports shpfile into database. If the projection is not defined within the shp file it can be given either by shpsrid or proj4 string (in this priority)."""
    #global sql,sqlv
    # mapping of shp data types to postgresql types
    pgtype = dict({'Integer': 'integer', 'Real': 'float', 'String': 'varchar',
                   'Integer64': 'bigint', 'Date': 'date'})
    pggeomtype = dict({
        osgeo.ogr.wkbPoint: 'POINT',
        osgeo.ogr.wkbLineString: 'LINESTRING',
        osgeo.ogr.wkbPolygon: 'POLYGON',
        osgeo.ogr.wkbMultiPoint: 'MULTIPOINT',
        osgeo.ogr.wkbMultiLineString: 'MULTILINESTRING',
        osgeo.ogr.wkbMultiPolygon: 'MULTIPOLYGON'
    })
    pggeomdim = dict({
        osgeo.ogr.wkbPoint: 0,
        osgeo.ogr.wkbLineString: 1,
        osgeo.ogr.wkbPolygon: 2,
        osgeo.ogr.wkbMultiPoint: 0,
        osgeo.ogr.wkbMultiLineString: 1,
        osgeo.ogr.wkbMultiPolygon: 2
    })
    tablegeomdim = -1
    # first open and read structure of shpfile
    try:
        # open shp file
        shp = osgeo.ogr.Open(shpfile)
        # shp and layer properties
        shpname = shp.GetName()
        layer = shp.GetLayer(0)
        # layer properties
        geomtype = layer.GetGeomType()
        geomtypename = osgeo.ogr.GeometryTypeToName(geomtype)
        # table properties
        nfeatures = layer.GetFeatureCount()
        feature = layer.GetFeature(0)
        if tablename is None or tablename == '':
            tablename = feature.GetDefnRef().GetName()
        if tabletemp:
            tempstr = 'TEMP'
        else:
            tempstr = ''
        if schema is None or schema == '' or tabletemp:
            fulltablename = '"'+tablename+'"'
        else:
            fulltablename = '"'+schema+'"."'+tablename+'"'
        # reading fields and their properties
        shpfields = []
        feature = layer.GetFeature(0)
        for i in range(feature.GetFieldCount()):
            shpfield = dict({'name': feature.GetFieldDefnRef(i).GetName(), \
                             # 'typename': bytearray(feature.GetFieldDefnRef(i).GetTypeName(),'UTF-8','replace').decode(shpcoding), \
                             'typename': osgeo_str_to_utf(feature.GetFieldDefnRef(i).GetTypeName(), shpcoding), \
                             'fieldtype': feature.GetFieldType(i), \
                             'width': feature.GetFieldDefnRef(i).GetWidth(), \
                             'precision': feature.GetFieldDefnRef(i).GetPrecision(), \
                             'value': feature.GetField(i)})
            shpfields.append(shpfield)
    except Exception as err:
        log.fmt_error('Error reading shpfile {}.', shpfile)
        log.fmt_error(str(err))
        return False, tablegeomdim
    #
    # create table in database
    try:
        # open connection and cursor to database
        if conn is None:
            conn = ep_connection
        cur = conn.cursor()
        #
        # get geom type - we can not take it only from the first feature
        # because LINE and MULTILINE can be mixed in shp as well as
        # POLYGON and MULTIPOLYGON and POINT and MULTIPOINT
        # !!! beter using postgis function ST_Multi(geom) !!!
        layer.ResetReading()
        feature = layer.GetNextFeature()
        gt = feature.GetGeometryRef().GetGeometryType()
        while feature is not None and \
              not gt in [osgeo.ogr.wkbMultiPoint, osgeo.ogr.wkbMultiLineString, osgeo.ogr.wkbMultiPolygon]:
            gt = feature.GetGeometryRef().GetGeometryType()
            feature = layer.GetNextFeature()
        tablegeomtype = pggeomtype[gt]
        tablegeomdim = pggeomdim[gt]
        #
        # get srid of shapefile
        tempsrid = False        
        try:  # try get srid from prj file
            shpproj4 = layer.GetSpatialRef().ExportToProj4()            
            shpsrid, tempsrid = ep_get_proj4_srid(shpproj4)
        except:
            raise        
            log.fmt_debug('Projection information was not possible to retrieve from prj file {}. Will try to use epsg or proj4 from config file.', shpfile)
        # if srid not set so far try proj4 string if given
        if (shpsrid is None or shpsrid <=0) and proj4:
            shpsrid, tempsrid = ep_get_proj4_srid(proj4)
        if shpsrid is None or shpsrid <= 0:
            log.fmt_warning('Can not detect proper shape srid for file {}', shpfile)
            return False, tablegeomdim
        if tablesrid is None or tablesrid <= 0:
            tablesrid = shpsrid
        log.fmt_tracing('Shp and table srid: {}, {}', shpsrid, tablesrid)
        #
        # test if table exists
        sql = 'DROP TABLE IF EXISTS ' + fulltablename
        cur.execute(sql)
        #
        # create psql table
        sql = 'CREATE ' + tempstr + ' TABLE ' + fulltablename + ' (gid serial'
        for f in shpfields:
            fl = ', "' + f['name'] + '" ' + pgtype[f['typename']]
            if not (f['typename'].startswith('Integer') or f['typename']=='Date') and f['width'] > 0:
                fl += '(' + str(f['width']) + ')'
            sql += fl
        sql += ', geom geometry(' + tablegeomtype + ',' + str(tablesrid) + ')'
        sql += ', PRIMARY KEY (gid));'
        log.debug(sql)
        cur.execute(sql)
        #
        # populate data
        sql1 = 'INSERT INTO ' + fulltablename + ' ('
        sql2 = ''
        for i in range(len(shpfields)):
            f = shpfields[i]
            sql1 += '"' + f['name'] + '",'
            sql2 += '%s,'
        sql1 += '"geom") VALUES ('
        sql = sql1 + sql2
        sql1 = 'ST_GeometryFromText(%s, %s)'
        if tablegeomtype.startswith('MULTI'):
            sql1 = 'ST_Multi(' + sql1 + ')'
        if tablesrid != shpsrid:
            sql1 = 'ST_Transform(' + sql1 + ',%s)'
        if makevalid:
            sql1 = 'ST_MakeValid(' + sql1 + ')'
        sql += sql1 + ');'
        #
        # reading features (rows) and import them into table
        layer.ResetReading()
        feature = layer.GetNextFeature()
        while feature is not None:
            sqlv = []
            for i in range(len(shpfields)):
                if (shpfields[i]['typename'] == 'String') and (feature.GetField(i) is not None):
                    sqlv.append(osgeo_str_to_utf(feature.GetFieldAsString(i), shpcoding))
                else:
                    sqlv.append(feature.GetField(i))
            wkt = feature.GetGeometryRef().ExportToWkt()
            sqlv.append(wkt)
            # ??? shouldn't this be also integer?
            sqlv.append(str(shpsrid))
            if tablesrid != shpsrid:
                sqlv.append(int(tablesrid))
            cur.execute(sql, sqlv)
            feature = layer.GetNextFeature()
        # create geometry index
        sql = 'create index "' + tablename + '_geom" on ' + fulltablename + ' using gist(geom);'
        cur.execute(sql)
        #
        # calculate statistics
        sqla = 'ANALYZE ' + fulltablename + ';'
        cur.execute(sqla)
    except Exception as err:
        log.fmt_error('Error creating table {}.', fulltablename)
        log.fmt_error(str(err))
        conn.rollback()
        retval = False
        raise
    else:
        log.fmt_debug('Table {} successfully imported.', fulltablename)
        conn.commit()
        retval = True
    finally:
        cur.close()
        conn.commit()

    return retval, tablegeomdim
