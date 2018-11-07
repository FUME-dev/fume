import osgeo.ogr
from lib.ep_libutil import ep_connection
from lib.ep_config import ep_cfg

def osgeo_str_to_utf(s, encoding='Latin-1'):
    try:
        return bytes([ord(c) for c in s]).decode(encoding)
    except ValueError:
        return s

def ep_shp2postgis(shpfile, shpcoding='Latin-1', shpsrid=None, conn=None, schema=None, tablename=None, tabletemp=False, tablesrid=None, geomdim=2):
    #global sql,sqlv
    # mapping of shp data types to postgresql types
    pgtype = dict({'Integer': 'integer', 'Real': 'float', 'String': 'varchar',
                   'Integer64': 'bigint'})
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
        geomproj4 = layer.GetSpatialRef().ExportToProj4()
        print('Shape: ', shpname, geomtype, geomtypename, geomproj4)
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
        #if tablesrid is None or tablesrid == 0:
        #    tablesrid = ep_cfg.domain.srid
        print('Table: {}, {}, {}'.format(tablename, fulltablename, tempstr))
        # reading fields and their properties
        # for i in range(layer.GetFeatureCount()):
        shpfields = []
        feature = layer.GetFeature(0)
        for i in range(feature.GetFieldCount()):
            #?? .decode("Latin-1").encode("utf8")
            #bytearray(feature.GetFieldDefnRef(i).GetTypeName(),'Latin-1','replace').decode(shpcoding)
            shpfield = dict({'name': feature.GetFieldDefnRef(i).GetName(), \
                             # 'typename': bytearray(feature.GetFieldDefnRef(i).GetTypeName(),'UTF-8','replace').decode(shpcoding), \
                             'typename': osgeo_str_to_utf(feature.GetFieldDefnRef(i).GetTypeName(), shpcoding), \
                             'fieldtype': feature.GetFieldType(i), \
                             'width': feature.GetFieldDefnRef(i).GetWidth(), \
                             'precision': feature.GetFieldDefnRef(i).GetPrecision(), \
                             'value': feature.GetField(i)})
            shpfields.append(shpfield)
#            print('Field: ', shpfield['name'], shpfield['typename'], shpfield['fieldtype'], \
#                   shpfield['width'], shpfield['precision'], shpfield['value'])
    except Exception as err:
        print('Error reading shpfile '+shpfile)
        print(str(err))
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
        if shpsrid is None or shpsrid <= 0:
            shpproj4 = layer.GetSpatialRef().ExportToProj4()
            cur.execute('select "srid" from "spatial_ref_sys" where trim("proj4text") = trim(%s)', [shpproj4])
            rec = cur.fetchone()
            if rec is None:
                # create temporary srid for transformation during the import
                tempsrid = True
                cur.execute('select max("srid") from "spatial_ref_sys"')
                shpsrid = cur.fetchone()[0] + 1
                print('create temporary user srid {}'.format(shpsrid))
                cur.execute('insert into "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") values (%s,%s,%s,%s,%s)', \
                            [shpsrid, 'USER', shpsrid, 'PROJCS["import"]', shpproj4])
            else:
                shpsrid = rec[0]
        if shpsrid is None or shpsrid <= 0:
            print('Can not detect proper shape srid for file {}'.format(shpfile))
            return False, tablegeomdim
        if tablesrid is None or tablesrid <= 0:
            tablesrid = shpsrid
        print('Shp and table srid: {}, {}'.format(shpsrid, tablesrid))
        #
        # test if table exists
        sql = 'DROP TABLE IF EXISTS ' + fulltablename
        print(sql)
        cur.execute(sql)
        #
        # create psql table
        sql = 'CREATE ' + tempstr + ' TABLE ' + fulltablename + ' (gid serial'
        for f in shpfields:
            fl = ', "' + f['name'] + '" ' + pgtype[f['typename']]
            if not f['typename'].startswith('Integer') and f['width'] > 0:
                fl += '(' + str(f['width']) + ')'
            sql += fl
        sql += ', geom geometry(' + tablegeomtype + ',' + str(tablesrid) + ')'
        sql += ', PRIMARY KEY (gid));'
        print(sql)
        cur.execute(sql)
        # add geometry - already added in create table
        #sql = "SELECT AddGeometryColumn('" + schema + "','" + tablename + "','geom','" + str(tablesrid) + "','" + tablegeomtype + "'," + str(geomdim) + ");"
        #print(sql)
        #cur.execute(sql)
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
        if tablesrid != shpsrid:
            sql += 'ST_Transform('
        if tablegeomtype.startswith('MULTI'):
            sql += 'ST_Multi(ST_GeometryFromText(%s,%s))'
        else:
            sql += 'ST_GeometryFromText(%s, %s)'
        if tablesrid != shpsrid:
            sql += ',% s));'
        else:
            sql += ');'
        print(sql)
        #
        # reading features (rows) and import them into table
        layer.ResetReading()
        feature = layer.GetNextFeature()
        while feature is not None:
            sqlv = []
            for i in range(len(shpfields)):
                if (shpfields[i]['typename'] == 'String') and (feature.GetField(i) is not None):
                    # sqlv.append(bytearray(feature.GetField(i), 'Latin-1', #'UTF-8',
                    #                       'replace').decode(shpcoding))
                    sqlv.append(osgeo_str_to_utf(feature.GetFieldAsString(i), shpcoding))
                else:
                    sqlv.append(feature.GetField(i))
            #print(sqlv)
            wkt = feature.GetGeometryRef().ExportToWkt()
            sqlv.append(wkt)
            # ??? shouldn't this be also integer?
            sqlv.append(str(shpsrid))
            if tablesrid != shpsrid:
                sqlv.append(int(tablesrid))
            #print(sqlv)
            #print(cur.mogrify(sql, sqlv))
            cur.execute(sql, sqlv)
            feature = layer.GetNextFeature()
        # create geometry index
        sql = 'create index "' + tablename + '_geom" on ' + fulltablename + ' using gist(geom);'
        cur.execute(sql)
        #
        # calculate statistics
        sqla = 'ANALYZE ' + fulltablename + ';'
        print(sqla)
        cur.execute(sqla)
        #if tempsrid:
        #    # delete temporary srid
        #    cur.execute('delete from "spatial_ref_sys" where "srid"=%s',
        #                [shpsrid])
    except Exception as err:
        print('Error creating table '+ fulltablename)
        print(str(err))
        conn.rollback()
        retval = False
        raise
    else:
        print('Table ' + fulltablename + ' successfully imported.')
        conn.commit()
        retval = True
    finally:
        cur.close()
        conn.commit()
    #
    return retval, tablegeomdim
