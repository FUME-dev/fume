"""
Description:
This module contain additional functions to import and process source data. It may be possibly integrated to ep_sources.
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

import csv
import pint
import psycopg2.extras

from input.ep_csv2table import ep_read_header
from input.ep_shp import ep_shp2postgis
from lib.ep_libutil import ep_connection, ep_get_proj4_srid, ep_rtcfg
from lib.ep_config import ep_cfg
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)


def ep_register_inventory(inv_name, schema, description=''):
    """
    Register inventory in the table ep_inventories

    Returns:
    (int) id of the inventory and flag (True, False) indicating whether the inventory was new
    """

    with ep_connection.cursor() as cur:
        try:
            cur.execute('SELECT inv_id FROM "{schema}"."ep_inventories" WHERE inv_name=%s'
                        .format(schema=schema), (inv_name,))
            inv_id = cur.fetchone()[0]
            new_inv = False
        except TypeError:
            new_inv = True
            cur.execute('INSERT INTO "{schema}"."ep_inventories" '
                        '(inv_name, description) '
                        'VALUES (%s, %s) RETURNING inv_id'
                        .format(schema=schema), (inv_name, description))
            inv_id = cur.fetchone()[0]
            log.fmt_debug('Inventory {} registered as ID {}', inv_name, inv_id)
            ep_connection.commit()

    return inv_id, new_inv


def ep_get_source_file_id(file_name, schema):
    """
    Returns ID of inventory file and flag of new inventory file
    """

    with ep_connection.cursor() as cur:
        # get file_id and file_table name
        try:
            cur.execute('SELECT file_id, file_table FROM "{schema}"."ep_source_files" '
                        'WHERE file_name=%s'.format(schema=schema), (file_name,))
            row = cur.fetchone()
            file_id, file_table = row[0], row[1]
            new_file = False
        except TypeError:
            new_file = True
            file_id = -1
            file_table = ''

    return file_id, file_table, new_file


def ep_register_source_file(file_name, inv_id, file_path, file_table, schema):
    """
    Register source file in the table ep_source_files
    Returns ID of file
    """
    with ep_connection.cursor() as cur:
        cur.execute('INSERT INTO "{schema}"."ep_source_files" '
                    '(file_name, inv_id, file_path, file_table) '
                    'VALUES (%s, %s, %s, %s) RETURNING file_id'
                    .format(schema=schema), (file_name, inv_id, file_path, file_table))
        file_id = cur.fetchone()[0]
        ep_connection.commit()

    return file_id


def ep_get_gset_id(gset_name, schema):

    with ep_connection.cursor() as cur:
        try:
            cur.execute('SELECT gset_id FROM "{schema}"."ep_geometry_sets" '
                        'WHERE gset_name=%s'.format(schema=schema),
                        (gset_name,))
            gset_id = cur.fetchone()[0]
            new_gset = False
        except TypeError:
            new_gset = True
            gset_id = -1

    return gset_id, new_gset


def ep_register_gset(gset_name, gset_table, gset_filter, gset_path, gset_info, file_id, schema, description=''):
    """
    Register geometry set in the table ep_geometry_sets

    Returns ID of geom. set and flag of new set
    """

    with ep_connection.cursor() as cur:

        cur.execute('INSERT INTO "{schema}"."ep_geometry_sets" (gset_name, gset_table, gset_filter, gset_path, gset_info, file_id, description) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING gset_id'.format(schema=schema),
                    (gset_name, gset_table, gset_filter, gset_path, gset_info, file_id, description))
        gset_id = cur.fetchone()[0]
        cur.execute('ANALYZE "{schema}"."ep_geometry_sets"'.format(schema=schema))
        log.fmt_debug('Geometry set {} registered as ID {} into raw geometry table {}',
                      gset_name, gset_id, gset_table)

    return gset_id


def ep_process_raw_geometries(con, schema, gset_table, gset_id, gset_filter, config):

    """ Process geometry to unified geometry internal format.
        The geometry is processed from raw data in tableraw or it is read from.
    """

    # get inventory parameters from configfile
    src_type = config.src_type
    file_type = config.file_type
    geom_type = config.geom_type
    srid_in = config.EPSG
    proj4 = config.proj4
    weight = config.weight

    # prepare sql where clause based on conf. conditions for subsetting sources
    sqlcond, cond_vals = ep_filter_raw_sources(config, gset_filter)
    
    # get srid from proj_string if not given by epsg 
    if (srid_in is None or srid_in <=0) and proj4:
    	srid_in = ep_get_proj4_srid(proj4)[0]
    
    # geometry type
    normalize_geom = []
    if geom_type == 'shp':
        geom_string = "t.geom"
    elif geom_type == 'grid_from_points':
        assert src_type == 'A'
        dx2 = config.grid_dx / 2
        dy2 = config.grid_dy / 2
        if file_type == 'shp':
            geom_string = " || ' ' || ".join(["ST_GeomFromText('POLYGON(('", "ST_X(t.geom) - {dx}", "ST_Y(t.geom) - {dy}", "','",
                                                                             "ST_X(t.geom) + {dx}", "ST_Y(t.geom) - {dy}", "','",
                                                                             "ST_X(t.geom) + {dx}", "ST_Y(t.geom) + {dy}", "','",
                                                                             "ST_X(t.geom) - {dx}", "ST_Y(t.geom) + {dy}", "','",
                                                                             "ST_X(t.geom) - {dx}", "ST_Y(t.geom) - {dy}",
                                       "'))', Find_SRID('{schema}', '{table}', 'geom'))"])
            geom_string = geom_string.format(schema=schema, table=gset_table, dx=dx2, dy=dy2)
        elif file_type == 'text' or file_type == 'netcdf':
            crd_east = '"' + config.crd_east + '"::float'
            crd_north = '"' + config.crd_north + '"::float'
            geom_string = " || ' ' || ".join(["ST_GeomFromText('POLYGON(('", crd_east + " - {dx}", crd_north + " - {dy}", "','",
                                                                             crd_east + " + {dx}", crd_north + " - {dy}", "','",
                                                                             crd_east + " + {dx}", crd_north + " + {dy}", "','",
                                                                             crd_east + " - {dx}", crd_north + " + {dy}", "','",
                                                                             crd_east + " - {dx}", crd_north + " - {dy}",
                                       "'))', {})"])
            geom_string = geom_string.format(srid_in, dx=dx2, dy=dy2)
            normalize_geom = [crd_east, crd_north]
    elif geom_type == 'grid_from_parameters':
        assert src_type == 'A'
        crd_east = '{xcorn} + ("' + config.ind_east + '"::integer - 1) * {dx}'
        crd_north = '{ycorn} + ("' + config.ind_north + '"::integer - 1) * {dy}'
        geom_string = " || ' ' || ".join(["ST_GeomFromText('POLYGON(('", crd_east, crd_north, "','",
                                                                         crd_east + " + {dx}", crd_north, "','",
                                                                         crd_east + " + {dx}", crd_north + " + {dy}", "','",
                                                                         crd_east, crd_north + " + {dy}", "','",
                                                                         crd_east, crd_north,
                                       "'))', {})"])
        geom_string = geom_string.format(srid_in, dx=config.grid_dx, dy=config.grid_dy, xcorn=config.xcorn, ycorn=config.ycorn)
        normalize_geom = [config.ind_east, config.ind_north]
    elif geom_type == 'infile':
        assert src_type == 'P'
        crd_east = '"' + config.crd_east + '"'
        crd_north = '"' + config.crd_north + '"'
        geom_string = " || ' ' || ".join(["ST_GeomFromText('POINT('", crd_east, crd_north, "')', {})".format(srid_in)])
        normalize_geom = [crd_east, crd_north]

    if len(normalize_geom) > 0:
        # normalize by columns in normalize_geom
        # assign one geom_orig_id to all rows with the same normalize_geom values
        s1 = ''
        s2 = sqlcond
        for nc in normalize_geom:
            if s1 != '':
                s1 += ','
            if s2 == '':
                s2 += ' WHERE '
            else:
                s2 += ' AND '
            s1 += nc
            s2 += 'u.' + nc + '=g.' + nc

        sqltext = 'WITH g AS( ' \
                  ' SELECT {0}, min(geom_orig_id) AS min_geom_orig_id ' \
                  '  FROM "{1}"."{2}" ' \
                  '  {4} ' \
                  '  GROUP BY {0} ' \
                  ') UPDATE "{1}"."{2}" u ' \
                  '    SET geom_orig_id = g.min_geom_orig_id ' \
                  '    FROM g {3}' \
                  .format(s1, schema, gset_table, s2, sqlcond)

        log.fmt_debug("Normalize geometry inputs: {}", sqltext)
        cur = con.cursor()
        cur.execute(sqltext, cond_vals*2)
        con.commit()
        sqldist = 'DISTINCT ON(geom_orig_id)'
    else:
        sqldist = ''

    epsrid = ep_cfg.projection_params.ep_projection_srid

    if weight is None or weight == '':
        weightcol = ''
        weightval = ''
    else:
        weightcol = ', weight'
        weightval = ', "{}"'.format(weight)

    log.fmt_debug("Geom_string: {}", geom_string)
    #geom_orig_id_term = 'concat_ws(\'_\',"' + '","'.join(geom_id) + '")'
    #log.fmt_debug("Geom_orig_id_term is: {}", geom_orig_id_term)
    sqltext = 'INSERT INTO "{}"."ep_in_geometries" (gset_id, geom_orig_id, geom, source_type {}) ' \
              'SELECT {} {}::integer, geom_orig_id,  ST_Transform({}::geometry,{}::integer), '\
              '\'{}\'::text {} FROM "{}"."{}" t {};'.format(schema, weightcol, sqldist, gset_id, geom_string, epsrid, \
                                                            src_type, weightval, schema, gset_table, sqlcond)
    log.fmt_debug("Process geometry set: {}", sqltext)
    log.fmt_debug("Cond_vals: {}", cond_vals)
    cur = con.cursor()
    cur.execute(sqltext, cond_vals)
    con.commit()
    # recompile statistics
    cur.execute('analyze "{}"."ep_in_geometries"'.format(schema))
    cur.close()


def ep_get_eset_id(eset_name, schema):
    with ep_connection.cursor() as cur:
        try:
            cur.execute('SELECT eset_id FROM "{schema}"."ep_emission_sets" '
                        'WHERE eset_name=%s'.format(schema=schema),
                        (eset_name,))
            eset_id = cur.fetchone()[0]
            new_eset = False
        except TypeError:
            new_eset = True
            eset_id = -1

    return eset_id, new_eset


def ep_register_eset(eset_name, file_id, eset_info, gset_id, eset_filter, data_type, schema, scenario_names, vdist_names):
    """
    Register emission set in the table ep_emission_sets

    Returns eset ID
    """

    with ep_connection.cursor() as cur:
        if not scenario_names[0]:
            scenario_ids = None
        else:
            cur.execute('SELECT scenario_id FROM "{source_schema}"."ep_scenario_list" '
                    'WHERE scenario_name = ANY (%s)'
                    .format(source_schema=schema), (scenario_names,))
            scenario_ids = [i[0] for i in cur.fetchall()]
            
            # check fof scenario_names not defined in senario list
            cur.execute('SELECT * FROM UNNEST(%(scenarios)s) '
                        'EXCEPT '
                        'SELECT scenario_name FROM "{source_schema}"."ep_scenario_list" '.
                        format(source_schema=schema),
                        {'scenarios': scenario_names})
            not_scenarios = [scen[0] for scen in cur.fetchall()]  
            if not_scenarios:
                raise ValueError('Scenarios {} applied on eset {} are not defined.'.format(', '.join(map(str, not_scenarios)), eset_name))

        if not vdist_names[0]:
            vdist_ids = None
        else:

            cur.execute('SELECT * FROM "{schema}"."ep_vdistribution_names"; '
                    .format(schema=schema))
            vdist = list(cur.fetchall())
            vdist_ids = [i[0] for i in vdist if i[1] in vdist_names]
            if vdist_ids == []:
                raise ValueError('Vertical distribution {} applied on eset {} was not defined.'.format(vdist_names, eset_name))
            else:
                ep_rtcfg["vdist"] = 1



        cur.execute('INSERT INTO "{schema}"."ep_emission_sets" '
                    '(eset_name, file_id, eset_info, gset_id, eset_filter, scenario_id, vdistribution_id, data_type) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING eset_id'
                    .format(schema=schema), (eset_name, file_id, eset_info, gset_id, eset_filter, scenario_ids, vdist_ids, data_type))
        eset_id = cur.fetchone()[0]
        cur.execute('ANALYZE "{schema}"."ep_emission_sets"'.format(schema=schema))
        ep_connection.commit()

    return eset_id


def ep_read_raw_sources(file_path, schema, raw_table, config):

    """ Calls appropriate ep_read_raw_... function based on input file format. """

    file_type = config.file_type
    encoding = config.encoding
    con = ep_connection
    cur = con.cursor()    

    # import raw sources based on file format
    if file_type == 'text':
        delim = config.field_delimiter
        qchar = config.text_delimiter
        ep_read_raw_csv(con, file_path, schema, raw_table, encoding=encoding, delimiter=delim, quotechar=qchar)
    elif file_type == 'netcdf':
        ep_read_raw_netcdf(con, file_path, schema, raw_table, config.netcdf_dims, config.netcdf_dimvars, config.netcdf_timedim)
    elif file_type == 'shp':
        epsg = config.EPSG
        proj4str = config.proj4
        ep_read_raw_shp(con, file_path, schema, raw_table, epsg, proj4str, encoding)

    log.fmt_debug('Input file {} read into raw table {}', file_path, raw_table)

    source_id = config.source_id
    geom_id = config.geom_id
    alter_term = 'add source_orig_id text'
    if source_id is None:          
        cur.execute('CREATE TEMP SEQUENCE IF NOT EXISTS serial_ids;')
        cur.execute('ALTER SEQUENCE serial_ids RESTART WITH 1;')
        update_term_s = "source_orig_id = nextval('serial_ids')"
    else:
        update_term_s = 'source_orig_id = concat_ws(\'_\',"' + '","'.join(source_id) + '")'
        
    alter_term += ', add geom_orig_id text'    
    if geom_id is None:
        update_term_g = 'geom_orig_id = source_orig_id'
    else:
        update_term_g = 'geom_orig_id = concat_ws(\'_\',"' + '","'.join(geom_id) + '")'

    log.fmt_debug('Input file alter and update terms are: {} {} {}', alter_term, update_term_s, update_term_g)

    if alter_term != '':
        # perform alter table
        sqltext = 'alter table "{}"."{}" {}'.format(schema, raw_table, alter_term)
        cur.execute(sqltext)
        # perform update table
        sqltext = 'update "{}"."{}" set {}'.format(schema, raw_table, update_term_s)
        cur.execute(sqltext)
        sqltext = 'update "{}"."{}" set {}'.format(schema, raw_table, update_term_g)
        cur.execute(sqltext)
        con.commit()
        log.fmt_debug('Original source and geom IDs added to the raw table {}.', raw_table)
    cur.close()

    if config.fix_overlaps:
        # check and fix overlaps of geometries (useful for wrong surrogates files)
        log.fmt_debug('Check and fix potential overlapping geometries in raw table {}.', raw_table)
        cur = con.cursor()
        sqltext = 'select * from ep_surrogates_check(%s, %s)'
        sqltext = cur.mogrify(sqltext, [schema, raw_table])
        cur.execute(sqltext)
        con.commit()
        log.fmt_debug('The overlapping geometries were check and fixed in the raw table {}.', raw_table)
        cur.close()


def ep_process_raw_sources(con, schema, file_table, temp_view, eset_id, eset_filter, config):

    """ Process sources in raw data format in file_table to unified internal format,
    converts units, transforms coordinates. """

    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #SELECT UpdateGeometrySRID('sources', 'ep_in_sources', 'geom', 4326)

    # get inventory parameters from configfile
    specie_input_type = config.specie_input_type
    specie_name = config.specie_name
    specie_val_col = config.specie_val_col
    category_input_type = config.category_input_type
    category_name = config.category_name

    if config.category:
        log.fmt_warning('Found deprecated source configuration option "category". '
                'Use "category_input_type = row" (default) and "category_name = {}".', config.category)
        category_input_type = 'row'
        category_name = config.category
 
    if config.category_def:
        log.fmt_warning('Found deprecated source configuration option "category_def". '
                'Use "category_input_type = predef" and "category_name = {}".', config.category_def)
        category_input_type = 'predef'
        category_name = config.category_def

    if category_input_type == 'column' and specie_input_type == 'column':
        log.error('Found category_input_type = ''column'' and specie_input_type = ''column''. This combination of source parameters is not valid.')
        raise ValueError

    # source type specific parameters
    params_col = []
    if config.height is not None:
        mul, add = get_conv_factors(config.height[1])
        params_col = [[config.height[0], str(mul), str(add)]]
    if config.diameter is not None:
        mul, add = get_conv_factors(config.diameter[1])
        params_col.append([config.diameter[0], str(mul), str(add)])
    if config.temperature is not None:
        mul, add = get_conv_factors(config.temperature[1])
        params_col.append([config.temperature[0], str(mul), str(add)])
    if config.velocity is not None:
        mul, add = get_conv_factors(config.velocity[1])
        params_col.append([config.velocity[0], str(mul), str(add)])
    log.fmt_debug('params_col: {}', params_col)

    # prepare a filtered temporary view for simplifying of the processing in ep_process_sources
    # (the problem is the transfer of the condition into pgsql proc)
    # prepare sql where clause based on conf. conditions for subsetting sources
    cur = con.cursor()
    cur.execute('DROP VIEW IF EXISTS "{}"'.format(temp_view))
    sqlcond, cond_vals = ep_filter_raw_sources(config, eset_filter)
    sqltext = 'CREATE TEMP VIEW "{}" AS SELECT * FROM "{}"."{}" {};'.format(temp_view, schema, file_table, sqlcond)
    log.fmt_debug('Temp view: {}, {}', sqltext,cond_vals)
    cur.execute(sqltext, cond_vals)

    # process raw sources
    cur.execute('SELECT scenario_id FROM "{schema}"."ep_emission_sets" WHERE eset_id=%s'.format(schema=schema), (eset_id,))
    scen_id = cur.fetchone()[0]
    log.fmt_debug('ep_process_sources: {}, {}, {}, {}, {}, {}, {}, {}, {}, {}', schema, temp_view, eset_id, scen_id, specie_input_type, specie_name, specie_val_col, category_input_type, category_name, params_col)
    cur.callproc('ep_process_sources', [schema, temp_view, eset_id, scen_id, specie_input_type, specie_name, specie_val_col, category_input_type, category_name, params_col])
    log.sql_debug(con)

    con.commit()

    cur.close()


def ep_filter_raw_sources(config, eset_filter=''):

    """ prepare sql where clause based on conf. conditions for subsetting sources. """

    sqlcond = ''
    if eset_filter:
        efl = eset_filter.split(',')
    else:
        efl = []
    subset_cond = config.subset_cond+efl
    cond_vals = tuple()
    if subset_cond:
        cond_cols = []
        for cond in subset_cond:
            if '!=' in cond:
                oper = '!='
            else:
                oper = '='
            cond_cols.append('"' + cond.split(oper)[0].strip() + '"' + oper + '%s')
            cond_vals = cond_vals + (cond.split(oper)[1].strip(),)
        sqlcond = 'WHERE ' + ' AND '.join(cond_cols)
    return sqlcond, cond_vals


def ep_read_raw_csv(con, file_path, schema, tablename, encoding='utf8', delimiter=',', quotechar='"'):

    """ Imports csv file (file_path) to schema.tablename as is. The table is created here.
    This function assumes the csv file has exactly one header row with column names. """

    cur = con.cursor()

    # try to open the file
    try:
        f = open(file_path, mode='r', encoding=encoding)
    except IOError:
        log.fmt_error("Error while trying to open file {}.", file_path)
        raise

    # create table
    decoded_del = bytes(delimiter, "utf-8").decode("unicode_escape")
    reader = csv.reader(f, delimiter=decoded_del, quotechar=quotechar)
    header = ep_read_header(reader)
    fieldnames = ', '.join(['"{}" varchar'.format(colname) for colname in header])

    cur.execute('DROP TABLE IF EXISTS "{}"."{}";'.format(schema, tablename))
    sqltext = 'CREATE TABLE "{}"."{}" ({});'.format(schema, tablename, fieldnames)
    cur.execute(sqltext)

    # read data from csv file
    with f:
        try:
            copy_sql = 'COPY "{}"."{}" FROM stdin WITH CSV DELIMITER \'{}\''.format(schema, tablename, decoded_del)
            cur.copy_expert(sql=copy_sql, file=f)
            con.commit()
        except psycopg2.ProgrammingError:
            log.fmt_error("Unable to import data from file {} into table {}.{}.", file_path, schema, tablename)
            con.rollback()
            raise
        finally:
            cur.close()


def ep_read_raw_shp(con, file_path, schema, tablename, epsg, proj4str, encoding):

    """ Imports sources from shp format (file_path) to schema.tablename as is.
    Stores information in contained in dbf table. """

    #shptable = os.path.splitext(os.path.basename(file_path))[0]
    ep_shp2postgis(file_path, shpcoding=encoding, shpsrid=epsg, proj4=proj4str, conn=con, schema=schema, tablename=tablename, tablesrid=epsg)


def ep_read_raw_netcdf(con, file_path, schema, tablename, dims=[], dimvars=[], timedim=None):

    """ Imports variables from netcdf file (file_path) to schema.tablename. The table is created here.
    dims has to be in same order as in netcdf. """

    from itertools import product
    from operator import itemgetter
    from netCDF4 import Dataset, num2date
    from tempfile import SpooledTemporaryFile
    import numpy as np

    cur = con.cursor()

    # read netcdf data
    data = Dataset(file_path, 'r')

    # get dimensions if not given by user
    if not dims:
        for dim in data.dimensions.items():
            dims.append(dim[0])

    # get dimension variable names if not given by user
    if not dimvars:
        for dimvar in dims:
            try:
                dimvars.append(data.variables[dimvar].name)
            except KeyError:
                dimvars.append(None)
    elif len(dimvars) != len (dims):
        raise ValueError('Given dimvars of netcdf file {} do not match dimension number.'.format(file_path))

    # get dimension values
    dimvars_list = []
    dimvars_names = []
    for d, dimvar in enumerate(dimvars):
        if dimvar:
            if dims[d]==timedim:
                dates = num2date(data.variables[dimvar][:], data.variables[dimvar].units)
                dimvars_list.append(np.array([date.strftime('%Y-%m-%d %H:%M:%S') for date in dates], dtype='datetime64'))
            else:
                dimvars_list.append(np.array(data.variables[dimvar]))
            dimvars_names.append(dimvar)
        else:
            dimvars_list.append(np.array([i for i in range(1, data.dimensions[dims[d]].size + 1)], dtype='int32'))
            dimvars_names.append(dims[d])

    # get variable names with correct dimension
    all_vars = [var for var in data.variables]
    vars = []
    for var in all_vars:
        if list(data.variables[var].dimensions) == dims:
            vars.append(var)

    # create table in db
    fieldnames = ['"{}" {}'.format(colname, ep_get_nc_var_type(dimvars_list[d]))
                  for d, colname in enumerate(dimvars_names)]
    fieldnames = fieldnames + ['"{}" {}'.format(colname, ep_get_nc_var_type(data.variables[colname]))
                  for colname in vars]
    fieldnames = ', '.join(fieldnames)
    cur.execute('DROP TABLE IF EXISTS "{}"."{}";'.format(schema, tablename))
    cur.execute('CREATE TABLE "{}"."{}" ({});'.format(schema, tablename, fieldnames))

    # prepare sql string for copy data into db
    fieldnames = ['"{}"'.format(colname) for colname in dimvars_names + vars]
    fieldnames = ', '.join(fieldnames)
    copy_sql = 'COPY "{}"."{}" ({}) FROM stdin WITH CSV DELIMITER \',\''.format(schema, tablename, fieldnames)

    # import data to db using temp csv text file
    max_spool = 500000000
    with SpooledTemporaryFile(max_size=max_spool) as temp_csv:
        ctypes = [str(varname.dtype)[0] for varname in dimvars_list] \
                 + [str(data.variables[varname].dtype)[0] for varname in vars]
        dimvars_list = [dim.tolist() for dim in dimvars_list]
        np.savetxt(temp_csv,
                   np.c_[tuple(np.array(list(map(itemgetter(idx), product(*dimvars_list)))) for idx in range(len(dimvars_list)))
                         + tuple(data.variables[var][:].flatten() for var in vars)],
                   delimiter=',',
                   fmt=create_savetxt_fmt(ctypes))
        temp_csv.seek(0)
        cur.copy_expert(sql=copy_sql, file=temp_csv)

    data.close()
    con.commit()
    cur.close()


def ep_get_nc_var_type(var):
    """ For netcdf or numpy variable var finds its type and returns corresponding postgres type.
    If data type is unknown varchar is returned. """

    pdtype = 'varchar'
    dtype = str(var.dtype)
    if dtype in ('float32', 'f4'):
        pdtype = 'real'
    elif dtype in ('float64', 'f8'):
        pdtype = 'double precision'
    elif dtype in ('int16', 'i2', 'int8', 'i1', 'uint8', 'u1'):
        pdtype = 'smallint'
    elif dtype in ('int32', 'i4', 'uint16', 'u2'):
        pdtype = 'integer'
    elif dtype in ('int64', 'i8', 'unit32', 'u4'):
        pdtype = 'bigint'
    elif dtype in ('unit64', 'u8'):
        pdtype = 'numeric'
    elif dtype in ('char', 'S1'):
        pdtype = 'character'
    elif dtype.startswith('datetime'):
        pdtype = 'timestamp'

    return pdtype


def create_savetxt_fmt(ctypes):
    """ ctypes is a list of characters. Function creates format string for numpy savetxt function.
    If character is not in (i, u, c, f) it is considered as unknown and treated as string."""

    fmt = []
    for c in ctypes:
        if c in ('i', 'u'):
            fmt.append('%5' + c)
        elif c in ('f'):
            fmt.append('%.18e')
        elif c in ('c'):
            fmt.append('%c')
        else:
            fmt.append('%s')
    return fmt


def get_conv_factors(unit):
    """ Calculates multiplicative and additive unit conversion factors assuming conversion can be formulated as
     base_unit = (unit + add) * mul """

    # create unit registry for unit conversion
    ureg = pint.UnitRegistry()

    k1 = ureg.Quantity(1, unit).to_base_units().magnitude
    k2 = ureg.Quantity(0, unit).to_base_units().magnitude

    mul = k1 - k2
    add = k2 / mul

    return mul, add

