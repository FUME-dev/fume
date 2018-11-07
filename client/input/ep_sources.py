""" Source import module.
Source files to be imported are listed in input.emission_inventories file (defined in configuration file),
located in input.sources directory. This file is a txt file, has exactly one header line and is tab separated.
It has 5 columns - inventory name, inventory version, short name for imported file, file name to be imported
(it is expected to be in input.sources directory) and infofile expected in the same place.
 """

import os
import csv
import pint

import psycopg2

from lib.ep_libutil import ep_connection, ep_create_schema, ep_debug, ep_internal_path
from lib.ep_config import ep_cfg, ConfigFile
from input.ep_read_sources import \
            ep_get_gset_id, ep_register_gset, ep_process_raw_geometries, \
            ep_register_inventory, ep_get_source_file_id, ep_register_source_file, \
            ep_get_eset_id, ep_register_eset, \
            ep_read_raw_sources, ep_process_raw_sources

# names of output tables
TABLE_IN_RAW = 'ep_raw_sources'


def import_sources(path=None, source_schema=None):
    import time

    start_is = time.time()

    if path is None:
        path = ep_cfg.input_params.sources

    if source_schema is None:
        source_schema = ep_cfg.db_connection.source_schema

    conf_schema = ep_cfg.db_connection.conf_schema

    ep_create_schema(source_schema, 'ep_create_source_tables.sql', ep_cfg.projection_params.ep_projection_srid)

    ep_debug('importing sources ...')

    # import emission inventories

    # TODO when this crashes somewhere in the middle of the process tables remain half-filled and next re-run possibly won't work
    # e.g. when read_category_mapping fails it won't be called next time because inventory is already registred

    # read sources with emission data
    inv_file = ep_cfg.input_params.emission_inventories
    if os.path.exists(inv_file):
        ep_read_sources(inv_file, path, source_schema, conf_schema, 'emission')
    else:
      ep_debug('no emission inventory file found')

    # calculate new pollutants defined in path/calulate_pollutants.csv file
    ep_debug('Calculation of pollutants ...')
    ts = time.time()
    ep_read_calculate_pollutants_file(path, source_schema, conf_schema)
    ep_calculate_pollutants(source_schema)
    te = time.time()
    print("calculate_pollutants: {}".format(te - ts))

    end_is = time.time()
    print("emission sources import {}".format(end_is - start_is))

    start_is = time.time()
    # read sources with activity data
    inv_file = ep_cfg.input_params.activity_data_inventories
    if os.path.exists(inv_file):
        ep_read_sources(inv_file, path, source_schema, conf_schema, 'activity')
    else:
        ep_debug('no activity data inventory file found')
    end_is = time.time()
    print("activity data import {}".format(end_is - start_is))


def ep_read_sources(inv_file, path, source_schema, conf_schema, input_type):

    confspec_file = ep_internal_path('conf', 'configspec-sources.conf')

    with open(inv_file, mode='r', encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
        next(reader)
        lines = list(reader)

    for line in lines:
        try:
            inv_name = line[0]
        except IndexError:
            continue

        if inv_name.startswith('#'):
            continue

        file_name = line[1]

        if inv_name is None or inv_name == '':
            # the file is not real emission inventory file
            # it contains just geometry values
            inv_id = 0
        else:
            # the real emission file - register inventory and read species and mappings
            # fetch inventory id
            inv_id, new_inv = ep_register_inventory(inv_name, source_schema)

            # read category and emission specie mappings if necessary
            if new_inv:
                read_category_mapping(inv_id, source_schema, path, inv_name)
                read_specie_mapping(inv_id, conf_schema, source_schema, path, inv_name, input_type)

            try:
                ep_connection.commit()
            except psycopg2.Error:
                pass

        # read raw data
        # check if the raw file is already read
        file_id, file_table, file_info, gset_id, new_file = ep_get_source_file_id(file_name, source_schema)

        if new_file:

            # file raw table name
            file_table = '_'.join([TABLE_IN_RAW, file_name])

            # raw file path
            file_path = os.path.join(path, line[2])

            # get config from the info file
            file_info = line[3]
            ifile = os.path.join(path, file_info)
            if os.path.exists(ifile):
                config = ConfigFile(ifile, confspec_file).values()
            else:
                ep_debug('info file {} not found'.format(ifile))

            ep_read_raw_sources(file_path, source_schema, file_table, config)

            # processing geometries of the new file
            # get name of geometry set
            geom_name = config.geom_name
            # TODO check type and length of geom_name!!!
            if geom_name is not None:
                gset_name = geom_name
            else:
                gset_name = file_name

            # check if geometry is already available
            gset_id, new_gset = ep_get_gset_id(gset_name, source_schema)

            if new_gset:
                # the geometry needs to be available in inventory file
                # get geometry info
                gset_info = config.geom_info
                if gset_info is None:
                    # the file has no separate geom_info files
                    # the description of the geometry is included in file_info
                    gset_info = [file_info]

                # register geometry set
                gset_id = ep_register_gset(gset_name, file_table, file_path, gset_info, source_schema)

                # process raw geometry into ep_in_geometries
                # process all geom_info files
                for ginfo in gset_info:
                    # get config from the info file
                    gfile = os.path.join(path, ginfo)
                    gconfig = ConfigFile([ifile, gfile], confspec_file).values()

                    # process raw geometries
                    ep_process_raw_geometries(ep_connection, source_schema, file_table, gset_id, gconfig)
                #
                ep_debug('Geometry set {} imported as ID {}.'.format(gset_name, gset_id))

            # register new file
            file_id = ep_register_source_file(file_name, inv_id, file_path, file_table, file_info, gset_id, source_schema)

            try:
                ep_connection.commit()
            except psycopg2.Error:
                pass

            # debug log
            ep_debug('Source file {} imported as ID {} into table {}.'.format(file_name, file_id, file_table))

    # process source files into source sets in next cycle
    # due to parallelization of the process
    # (we need avoid double registering of one source file which
    # has more source sets)

    ep_debug('processing sources ...')

    for line in lines:
        try:
            inv_name = line[0]
        except IndexError:
            continue
        if inv_name.startswith('#'):
            continue

        if inv_name is not None and inv_name != '':

            # get file info
            file_name = line[1]
            file_id, file_table, file_info, gset_id, new_file = ep_get_source_file_id(file_name, source_schema)

            if len(line) > 4:
                eset_name = line[4]
            else:
                eset_name = file_name

            # get eset_id
            eset_id, new_eset = ep_get_eset_id(eset_name, source_schema)

            if new_eset:
                # get config from the info file
                ifile = os.path.join(path, file_info)
                config = ConfigFile(ifile, confspec_file).values()

                # process raw inventory data into a new source set
                if len(line) > 5:
                    eset_filter = line[5]
                else:
                    eset_filter = ""

                # register new eset
                eset_id = ep_register_eset(eset_name, file_id, eset_filter, config.data_type, source_schema)

                # process raw inventory data
                ep_debug(
                    'Processing emission raw file {} into set {} with eset_id {}.'.format(file_name, eset_name, eset_id))
                temp_view = 'RAW_SOURCES_TEMP'
                ep_process_raw_sources(ep_connection, source_schema, file_table, temp_view, eset_id, eset_filter, config)
                #
                ep_debug('Emission set {} processed as eset_id {}.'.format(eset_name, eset_id))


def read_category_mapping(id, schema, path, inv_name):

    """ Reads category mapping for inventory. Inserts data in table schema.ep_classification_mapping. """

    tablename = 'ep_classification_mapping'

    # create filename
    filename = "_".join(["category", inv_name]) + "." + "csv"
    filepath = os.path.join(path, filename)

    cur = ep_connection.cursor()
    sqltext = 'INSERT INTO "{}".{} (inv_id, orig_cat_id, cat_id) VALUES (%s, %s, %s)'.format(schema, tablename)
    with open(filepath, mode='r', encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader)
        for line in reader:
            cur.execute(sqltext, (id, line[0], line[1]))

    ep_connection.commit()
    cur.close()


def read_specie_mapping(id, conf_schema, schema, path, inv_name, input_type):

    """ Reads specie names mapping and units for inventory. Inserts data in table schema.ep_in_specie_mapping. """

    if input_type == 'emission':
        ftable = 'ep_in_species'
        tablename = 'ep_in_specie_mapping'
        id_col_name = 'spec_in_id'
        filename = "_".join(["species", inv_name]) + "." + "csv"
    else:
        ftable = 'ep_activity_units'
        tablename = 'ep_activity_units_mapping'
        id_col_name = 'act_unit_id'
        filename = "_".join(["activity_units", inv_name]) + "." + "csv"

    filepath = os.path.join(path, filename)

    cur = ep_connection.cursor()
    sqltext = 'INSERT INTO "{}".{} (inv_id, orig_name, {}, unit, conv_factor) VALUES (%s, %s, %s, %s, %s)'.format(schema, tablename, id_col_name)

    with open(filepath, mode='r', encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader)
        for line in reader:

            # fetch foreign_key
            cur.execute('SELECT {} FROM "{}".{} WHERE name = %s'.format(id_col_name, conf_schema, ftable), (line[1], ))
            spec_in_id = cur.fetchone()[0]

            # calculate unit conversion factor
            unit = line[2]
            ureg = pint.UnitRegistry()
            ureg.default_system = 'mks'
            if input_type == 'emission':
                conv_factor = ureg.Quantity(1, unit).to(ureg.g / ureg.s).magnitude
            else:
                conv_factor = ureg.Quantity(1, unit).to_base_units().magnitude
            cur.execute(sqltext, (id, line[0], spec_in_id, unit, conv_factor))

    ep_connection.commit()
    cur.close()


def ep_read_calculate_pollutants_file(path, schema, conf_schema):
    """ Reads calculate_pollutants.csv file from path location. With expressions to calculate new inventory pollutants.
    New pollutant can be described as linear combination of existing ones. E.g. POL = 0.95 * POL1 + POL2 - 1.1 * POL3
    Equations are stored in table ep_calculate_pollutants"""

    import re

    cp_table = 'ep_calculate_pollutants'

    filepath = os.path.join(path, 'calculate_pollutants.csv')

    if os.path.isfile(filepath):
        # populate ep_calculate_pollutants table
        cur = ep_connection.cursor()
        cur.execute('TRUNCATE "{}"."{}"'.format(schema, cp_table))

        with open(filepath, mode='r', encoding='utf8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader)
            categories = []
            cat_count = {}
            for line in reader:
                category = line[0]
                # do not allow to have same category inserted multiple times
                if category in categories:
                    raise ValueError('Category {} found more than once in {}.'.format(category, filepath))
                else:
                    categories.append(category)

                for expr in line[1].split(','):
                    try:
                        cat_count[category] += 1
                    except KeyError:
                        cat_count[category] = 1

                    sides = expr.replace(" ", "").split('=')
                    output = sides[0]
                    cur.execute('SELECT spec_in_id FROM "{}".ep_in_species WHERE name = %s'.format(conf_schema), (output,))
                    output_id = cur.fetchone()[0]

                    rside = re.split('\-|\+', sides[1])
                    opers = list(re.sub('[^\-\+]', '', sides[1]))

                    # treat the situation that first sign is not stated (must be + in that case)
                    if len(rside) != len(opers):
                        opers.insert(0, '+')

                    for idx, term in enumerate(rside):
                        sterm = term.split('*')
                        if len(sterm) == 1:
                            coef = float(opers[idx] + '1')
                            input = sterm[0]
                        elif len(sterm) == 2:
                            coef = float(opers[idx] + sterm[0])
                            input = sterm[1]
                        cur.execute('SELECT spec_in_id FROM "{}".ep_in_species WHERE name = %s'.format(conf_schema), (input,))
                        input_id = cur.fetchone()[0]

                        # store equation term in table
                        cur.execute('INSERT INTO "{}".{} (cat_id, spec_out_id, spec_inp_id, coef, cat_order) '
                                'VALUES (%s, %s, %s, %s, %s)'.format(schema, cp_table),
                                    (category, output_id, input_id, coef, cat_count[category]))
        ep_connection.commit()
        cur.close()


def ep_calculate_pollutants(schema):
    """ Calculates new inventory pollutants into ep_in_emissions table. Based on equations stored
    in ep_calculate_pollutants. If pollutant is already there for source/category, nothing happens. """

    emis_table = 'ep_in_emissions'
    cp_table = 'ep_calculate_pollutants'

    cur = ep_connection.cursor()

    # create rule to get 'ON CONFLICT DO NOTHING' behaviour
    cur.execute('CREATE OR REPLACE RULE "ep_in_emissions_on_duplicate_do_nothing" AS '
                'ON INSERT TO "{schema}".{emis_table} WHERE EXISTS (SELECT 1 FROM "{schema}".{emis_table} '
                'WHERE (source_id, spec_in_id, cat_id) = (NEW.source_id, NEW.spec_in_id, NEW.cat_id)) '
                'DO INSTEAD NOTHING'.format(schema=schema, emis_table=emis_table))

    # fetch number of evaluating levels in calculate pollutants table (cat_order)
    cur.execute('SELECT max(cat_order) FROM "{}".{}'.format(schema, cp_table))
    max_eval_level = cur.fetchone()[0]

    if max_eval_level is not None:
        for cat_order in range(1, max_eval_level+1):
            cur.execute('INSERT INTO "{schema}".{emis_table} (source_id, spec_in_id, cat_id, emission) '
                        'SELECT e.source_id, e.spec_out_id, e.cat_id, SUM(e.coef*emission) '
                        'FROM '       # join to all sources/categories needed species (src)
                          '(SELECT source_id, cat_id, spec_out_id, spec_inp_id, coef '
                             'FROM '  # select all calculate expressions for current evaluate level
                                '(SELECT * FROM "{schema}".{cp_table} WHERE cat_order = %s) AS un '
                                'JOIN ' # select unique source_id, category pairs
                                   '(SELECT DISTINCT ON (source_id, cat_id) source_id, cat_id '
                                      'FROM "{schema}".{emis_table}'
                                   ') AS src USING (cat_id) '
                          ') AS e '
                          'LEFT JOIN '    # join available species for each source/category, (null where not available)
                             '"{schema}".{emis_table} AS emis '
                             'ON (e.source_id = emis.source_id AND e.spec_inp_id = emis.spec_in_id AND e.cat_id = emis.cat_id) '
                       'GROUP BY e.source_id, e.spec_out_id, e.cat_id '
                        # we want the SUM(e.coef*emission) to return no row when any of input row is null (i.e. specie not available)
                       'HAVING NOT bool_or(emission IS NULL)'.format(schema=schema,
                                                                     emis_table=emis_table,
                                                                     cp_table=cp_table),
                        (cat_order, ))

        ep_connection.commit()

    cur.execute('DROP RULE "ep_in_emissions_on_duplicate_do_nothing" ON "{schema}".{emis_table}'.
                format(schema=schema, emis_table=emis_table))

    cur.close()
