import csv
import pint
import psycopg2.extras

from input.ep_shp import ep_shp2postgis
from lib.ep_libutil import ep_debug, ep_connection
from lib.ep_config import ep_cfg


def ep_register_inventory(inv_name, schema, description=''):
    """
    Register inventory in the table ep_inventories

    Returns ID of inventory and flag of new inventory
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
            ep_debug('Inventory {} registered as ID {}'.format(inv_name, inv_id))
            ep_connection.commit()

    return inv_id, new_inv


def ep_get_source_file_id(file_name, schema):
    """
    Returns ID of inventory file and flag of new inventory file
    """

    with ep_connection.cursor() as cur:
        # get file_id and file_table name
        try:
            cur.execute('SELECT file_id, file_table, file_info, gset_id FROM "{schema}"."ep_source_files" '
                        'WHERE file_name=%s'.format(schema=schema), (file_name,))
            row = cur.fetchone()
            file_id, file_table, file_info, gset_id = row[0], row[1], row[2], row[3]
            new_file = False
        except TypeError:
            new_file = True
            file_id = -1
            file_table = ''
            file_info = ''
            gset_id = -1

    return file_id, file_table, file_info, gset_id, new_file


def ep_register_source_file(file_name, inv_id, file_path, file_table, file_info, gset_id, schema):
    """
    Register source file in the table ep_source_files
    Returns ID of file
    """
    with ep_connection.cursor() as cur:
        cur.execute('INSERT INTO "{schema}"."ep_source_files" '
                    '(file_name, inv_id, file_path, file_table, file_info, gset_id) '
                    'VALUES (%s, %s, %s, %s, %s, %s) RETURNING file_id'
                    .format(schema=schema), (file_name, inv_id, file_path, file_table, file_info, gset_id))
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


def ep_register_gset(gset_name, gset_table, gset_path, gset_info, schema, description=''):
    """
    Register geometry set in the table ep_geometry_sets

    Returns ID of geom. set and flag of new set
    """

    with ep_connection.cursor() as cur:

        cur.execute('INSERT INTO "{schema}"."ep_geometry_sets" (gset_name, gset_table, gset_path, gset_info, description) '
                    'VALUES (%s, %s, %s, %s, %s) RETURNING gset_id'.format(schema=schema),
                    (gset_name, gset_table, gset_path, gset_info, description))
        gset_id = cur.fetchone()[0]
        cur.execute('ANALYZE "{schema}"."ep_geometry_sets"'.format(schema=schema))
        ep_debug('Geometry set {} registered as ID {} into raw geometry table {}'
                 .format(gset_name, gset_id, gset_table))

    return gset_id

"""
def ep_read_raw_geometries(con, geom_file, schema, raw_geom_table, config):

    # Calls appropriate ep_read_raw_... function based on input file format. 

    file_type = config.file_type
    encoding = config.encoding
    epsg = config.EPSG

    if file_type == 'text':
        delim = config.field_delimiter
        qchar = config.text_delimiter
        skip_lines = config.skip_lines
        ep_read_raw_csv(con, geom_file, schema, raw_geom_table, encoding=encoding, delimiter=delim, quotechar=qchar, skip=skip_lines)
    elif file_type == 'shp':
        ep_read_raw_shp(con, geom_file, schema, raw_geom_table, epsg, encoding)

    ep_debug('Geometry file {} read into raw table {}'.format(geom_file, raw_geom_table))

    cur = con.cursor()
    sqltext = 'alter table "{}"."{}" add geom_orig_id text;'.format(schema, raw_geom_table)
    cur.execute(sqltext)
    geom_id = config.geom_id
    if geom_id is None:
        geom_id = config.source_id
    geom_orig_id_term = 'concat_ws(\'_\',"' + '","'.join(geom_id) + '")'
    ep_debug("Geom_orig_id_term: {}".format(geom_orig_id_term))
    sqltext = 'update "{}"."{}" set "geom_orig_id" = {};'.format(schema, raw_geom_table, geom_orig_id_term)
    cur.execute(sqltext)
    cur.close()
    ep_debug('Original IDs added to the raw geometry table {}.'.format(raw_geom_table))
"""

def ep_process_raw_geometries(con, schema, gset_table, gset_id, config):

    """ Process geometry to unified geometry internal format.
        The geometry is processed from raw data in tableraw or it is read from.
    """

    # get inventory parameters from configfile
    src_type = config.src_type
    epsg_in = config.EPSG
    file_type = config.file_type
    geom_type = config.geom_type

    # prepare sql where clause based on conf. conditions for subsetting sources
    sqlcond, cond_vals = ep_filter_raw_sources(config)

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
        elif file_type == 'text':
            crd_east = '"' + config.crd_east + '"'
            crd_north = '"' + config.crd_north + '"'
            geom_string = " || ' ' || ".join(["ST_GeomFromText('POLYGON(('", crd_east + "::float - {dx}", crd_north + "::float - {dy}", "','",
                                                                             crd_east + "::float + {dx}", crd_north + "::float - {dy}", "','",
                                                                             crd_east + "::float + {dx}", crd_north + "::float + {dy}", "','",
                                                                             crd_east + "::float - {dx}", crd_north + "::float + {dy}", "','",
                                                                             crd_east + "::float - {dx}", crd_north + "::float - {dy}",
                                       "'))', {})"])
            geom_string = geom_string.format(epsg_in, dx=dx2, dy=dy2)
            normalize_geom = [crd_east, crd_north]
    elif geom_type == 'infile':
        assert src_type == 'P'
        crd_east = '"' + config.crd_east + '"'
        crd_north = '"' + config.crd_north + '"'
        geom_string = " || ' ' || ".join(["ST_GeomFromText('POINT('", crd_east, crd_north, "')', {})".format(epsg_in)])
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

        ep_debug("Normalize geometry inputs: {}".format(sqltext))
        cur = con.cursor()
        cur.execute(sqltext, cond_vals*2)
        #for notice in con.notices:
        #    print(notice)
        con.commit()
        sqldist = 'DISTINCT ON(geom_orig_id)'
    else:
        sqldist = ''

    epsrid = ep_cfg.projection_params.ep_projection_srid

    ep_debug("Geom_string: {}".format(geom_string))
    #geom_orig_id_term = 'concat_ws(\'_\',"' + '","'.join(geom_id) + '")'
    #ep_debug("Geom_orig_id_term is: {}".format(geom_orig_id_term))
    sqltext = 'INSERT INTO "{}"."ep_in_geometries" (gset_id, geom_orig_id, geom, source_type) ' \
              'SELECT {} {}::integer, geom_orig_id,  ST_Transform({}::geometry,{}::integer), \'{}\'::text FROM "{}"."{}" t {};'\
              .format(schema, sqldist, gset_id, geom_string, epsrid, src_type, schema, gset_table, sqlcond)    
    ep_debug("Process geometry set: {}".format(sqltext))
    ep_debug("Cond_vals: {}".format(cond_vals))
    cur = con.cursor()
    cur.execute(sqltext, cond_vals)
    #for notice in con.notices:
    #    print(notice)
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


def ep_register_eset(eset_name, file_id, eset_filter, data_type, schema):
    """
    Register inventory file in the table ep_emission_sets

    Returns ID of file
    """

    with ep_connection.cursor() as cur:
        cur.execute('INSERT INTO "{schema}"."ep_emission_sets" '
                    '(eset_name, file_id, eset_filter, data_type) '
                    'VALUES (%s, %s, %s, %s) RETURNING eset_id'
                    .format(schema=schema), (eset_name, file_id, eset_filter, data_type))
        eset_id = cur.fetchone()[0]
        cur.execute('ANALYZE "{schema}"."ep_emission_sets"'.format(schema=schema))
        ep_connection.commit()

    return eset_id


def ep_read_raw_sources(file_path, schema, raw_table, config):

    """ Calls appropriate ep_read_raw_... function based on input file format. """

    file_type= config.file_type
    encoding = config.encoding
    epsg = config.EPSG
    con = ep_connection

    if file_type == 'text':
        delim = config.field_delimiter
        qchar = config.text_delimiter
        skip_lines = config.skip_lines
        ep_read_raw_csv(con, file_path, schema, raw_table, encoding=encoding, delimiter=delim, quotechar=qchar, skip=skip_lines)
    elif file_type == 'shp':
        ep_read_raw_shp(con, file_path, schema, raw_table, epsg, encoding)

    ep_debug('Input file {} read into raw table {}'.format(file_path, raw_table))

    source_id = config.source_id
    geom_id = config.geom_id
    if geom_id is None:
        geom_id = source_id
    alter_term = ''
    update_term = ''
    if source_id is not None:
        alter_term = 'add source_orig_id text'
        update_term = 'source_orig_id = concat_ws(\'_\',"' + '","'.join(source_id) + '")'
    if geom_id is not None:
        if source_id is not None:
            alter_term += ','
            update_term += ','
        alter_term += 'add geom_orig_id text'
        update_term += 'geom_orig_id = concat_ws(\'_\',"' + '","'.join(geom_id) + '")'

    ep_debug('Input file alter and update terms are: {} {}'.format(alter_term, update_term))

    if alter_term != '':
        cur = con.cursor()
        # perform alter table
        sqltext = 'alter table "{}"."{}" {}'.format(schema, raw_table, alter_term)
        cur.execute(sqltext)
        # perform update table
        sqltext = 'update "{}"."{}" set {}'.format(schema, raw_table, update_term)
        cur.execute(sqltext)
        cur.close()
        con.commit()
        ep_debug('Original source and geom IDs added to the raw table {}.'.format(raw_table))


def ep_process_raw_sources(con, schema, file_table, temp_view, eset_id, eset_filter, config):

    """ Process sources in raw data format in file_table to unified internal format,
    converts units, transforms coordinates. """

    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #SELECT UpdateGeometrySRID('sources', 'ep_in_sources', 'geom', 4326)

    # get inventory parameters from configfile
    #src_type = config.src_type
    category = config.category
    category_def = config.category_def

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
    ep_debug('params_col: {}'.format(params_col))

    # prepare a filtered temporary view for simplifying of the processing in ep_process_sources
    # (the problem is the transfer of the condition into pgsql proc)
    # prepare sql where clause based on conf. conditions for subsetting sources
    cur = con.cursor()
    cur.execute('DROP VIEW IF EXISTS "{}"'.format(temp_view))
    sqlcond, cond_vals = ep_filter_raw_sources(config, eset_filter)
    sqltext = 'CREATE TEMP VIEW "{}" AS SELECT * FROM "{}"."{}" {};'.format(temp_view, schema, file_table, sqlcond)
    ep_debug('Temp view: {}, {}'.format(sqltext,cond_vals))
    cur.execute(sqltext, cond_vals)

    # process raw sources
    try:
        cur.callproc('ep_process_sources', [schema, temp_view, eset_id, category, category_def, params_col])
    except Exception as e:
        for notice in con.notices:
             print(notice)
        raise

    # check notices
    #for notice in con.notices:
    #    print(notice)
    con.commit()

    cur.close()


def ep_filter_raw_sources(config, eset_filter=''):

    """  prepare sql where clause based on conf. conditions for subsetting sources. """

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


def ep_read_raw_csv(con, file_path, schema, tablename, encoding='utf8', delimiter=',', quotechar='"', skip=0):

    """ Imports csv file (file_path) to schema.tablename as is. The table is created here.
    This function assumes the csv file has exactly one header row with column names. """

    cur = con.cursor()

    # try to open the file
    try:
        f = open(file_path, mode='r', encoding=encoding)
        for _ in range(skip):
            next(f)
    except IOError:
        print("Error while trying to read {} file.".format(file_path))
        raise

    # create table
    decoded_del = bytes(delimiter, "utf-8").decode("unicode_escape")
    reader = csv.reader(f, delimiter=decoded_del, quotechar=quotechar)
    header = [colname.strip() for colname in next(reader)]
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
            print("Unable to import data from file {} into table {}.{}.".format(file_path, schema, tablename))
            con.rollback()
            raise
        finally:
            cur.close()


def ep_read_raw_shp(con, file_path, schema, tablename, epsg, encoding):

    """ Imports sources from shp format (file_path) to schema.tablename as is.
    Stores information in contained in dbf table. """

    #shptable = os.path.splitext(os.path.basename(file_path))[0]
    ep_shp2postgis(file_path, shpcoding=encoding, shpsrid=epsg, conn=con, schema=schema, tablename=tablename, tablesrid=epsg)


def ep_read_raw_netcdf(con, file_path, schema, tablename, dims, dimvals=None):

    """ Imports variables from netcdf file (file_path) to schema.tablename. The table is created here.
    dims has to be in same order as in netcdf ... dim_vals ..."""

    from itertools import product
    from operator import itemgetter
    from netCDF4 import Dataset
    from tempfile import SpooledTemporaryFile
    import numpy as np

    cur = con.cursor()

    # read netcdf data
    data = Dataset(file_path, 'r')

    # get variable names with correct dimension
    # TODO do not require dim names in same order as in netcdf
    # TODO alternatively do not do this, get list of variables as argument
    all_vars = [var for var in data.variables]
    vars = []
    for var in all_vars:
        if list(data.variables[var].dimensions) == dims:
            vars.append(var)

    # TODO maybe omit whole dimvals stuff
    # get dimension variables
    dimvars = [t[1] for t in dimvals]
    # TODO
    dimvars = []

    # create table
    fieldnames = ['"dim_{}" integer'.format(colname) for colname in dims]
    fieldnames = fieldnames + ['"{}" {}'.format(colname, ep_get_nc_var_type(data.variables[colname]))
                               for colname in dimvars + vars]
    fieldnames = ', '.join(fieldnames)
    cur.execute('DROP TABLE IF EXISTS "{}"."{}";'.format(schema, tablename))
    cur.execute('CREATE TABLE "{}"."{}" ({});'.format(schema, tablename, fieldnames))

    # create arrays with dimension indexes
    dim_list = []
    for dim in dims:
        dim_list.append([i for i in range(1, data.dimensions[dim].size + 1)])
    idx_tup = tuple(np.array(list(map(itemgetter(idx), product(*dim_list)))) for idx in range(len(dim_list)))

    # create table in db
    fieldnames = ['"dim_{}"'.format(colname) for colname in dims]
    fieldnames = fieldnames + ['"{}"'.format(colname) for colname in dimvars + vars]
    fieldnames = ', '.join(fieldnames)
    copy_sql = 'COPY "{}"."{}" ({}) FROM stdin WITH CSV DELIMITER \',\''.format(schema, tablename, fieldnames)

    # import data to db
    max_spool = 500000000
    with SpooledTemporaryFile(max_size=max_spool) as temp_csv:
        ctypes = ['i'] * len(dims) + [str(data.variables[colname].dtype)[0] for colname in dimvars + vars]
        np.savetxt(temp_csv,
                   np.c_[idx_tup + tuple(data.variables[var][:].flatten() for var in vars)],
                   delimiter=',',
                   fmt=create_savetxt_fmt(ctypes))
        temp_csv.seek(0)
        cur.copy_expert(sql=copy_sql, file=temp_csv)

    data.close()
    con.commit()
    cur.close()


def ep_get_nc_var_type(var):
    """ For netcdf variable var finds its type and returns corresponding postgres type.
    If data type is unknown varchar is returned. """

    pdtype = 'varchar'
    dtype = var.dtype
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
