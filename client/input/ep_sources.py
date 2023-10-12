"""
Description: Source import module.
Source files to be imported are listed in input.emission_inventories file (defined in configuration file),
located in input.sources directory. This file is a txt file, has exactly one header line and is tab separated.
It has 5 columns - inventory name, inventory version, short name for imported file, file name to be imported
(it is expected to be in input.sources directory) and infofile expected in the same place.
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

import os
import csv
import pint
import itertools

import psycopg2

from lib.ep_libutil import ep_connection, ep_create_schema, ep_internal_path
from lib.ep_config import ep_cfg, ConfigFile
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
import lib.ep_reporting
report = lib.ep_reporting.Reporter(__name__)
from input.ep_csv2table import ep_read_header, ep_skip_line
from input.ep_read_sources import \
            ep_get_gset_id, ep_register_gset, ep_process_raw_geometries, \
            ep_register_inventory, ep_get_source_file_id, ep_register_source_file, \
            ep_get_eset_id, ep_register_eset, \
            ep_read_raw_sources, ep_process_raw_sources, ep_filter_raw_sources
            

# names of output tables
TABLE_IN_RAW = 'ep_raw_sources'

SUM_SQL = 'SELECT eset_name, name, sum(emission)*31.5576 from "{source_schema}".ep_in_emissions '\
       'JOIN "{source_schema}".ep_in_sources using (source_id) '\
       'JOIN "{source_schema}".ep_emission_sets using (eset_id) '\
       'JOIN "{conf_schema}".ep_in_species using (spec_in_id) '\
       'GROUP BY eset_name, name '\
       'ORDER BY eset_name  '

def import_sources(path=None, source_schema=None):
    """ The main source import function.

    Parameters:
    path (str): path to input files, if not given it is imported from config file
    source_schema (str): schema to import data, if not given it is imported from config file

    """
    from lib.ep_libutil import exec_timer

    if path is None:
        path = ep_cfg.input_params.sources

    if source_schema is None:
        source_schema = ep_cfg.db_connection.source_schema

    conf_schema = ep_cfg.db_connection.conf_schema

    ep_create_schema(source_schema, 'ep_create_source_tables.sql', ep_cfg.projection_params.ep_projection_srid)

    log.info('Importing emission sources informations ...')

    # read vertical distributions
    log.info("Importing vertical distributions.")
    with exec_timer("vdistribution import") as timer:
        inv_file = ep_cfg.input_params.vertical_distribution_list
        if os.path.exists(inv_file):
            ep_read_vdistributions(inv_file, source_schema, conf_schema)
            report.record.message('Vertical distribution list imported: {}.', os.path.abspath(inv_file))
        else:
            log.info('... no emission vertical distribution list file found.')
            report.record.message('Vertical distribution list not imported.')

    # read scenarios files
    log.info("Importing emission scenarios.")
    with exec_timer("scenarios import") as timer:
        scenarios_list_file = ep_cfg.input_params.scenarios_list
        if os.path.exists(scenarios_list_file):
            ep_read_scenarios(scenarios_list_file, source_schema, conf_schema)
            report.record.message('Scenarios list imported: {}.', os.path.abspath(scenarios_list_file))
        else:
            log.info('... no scenarios list file found')
            report.record.message('Scenarios list not imported.')

    # import emission inventories
    log.info("Importing emission inventories.")
    with exec_timer("emission files import") as timer:
        inv_file = ep_cfg.input_params.emission_inventories
        if os.path.exists(inv_file):
            report.record.message('Emission inventory file: {}.', os.path.abspath(inv_file))
            ep_read_sources(inv_file, path, source_schema, conf_schema, 'emission')
        else:
            log.info('... no emission inventory file found.')
            report.record.message('Emission inventory file not imported.')

    # control sums of imported emissions
    report.sum.sql('Control sums of imported emissions (scenarios applied) - emission per specie and eset [t/year]',
       SUM_SQL, conf_schema=conf_schema, source_schema=source_schema)

    # calculate new pollutants defined in path/calulate_pollutants.csv file
    log.info('Calculation of pollutants.')
    with exec_timer("calculation of pollutants") as timer:
        filepath = os.path.join(path, 'calculate_pollutants.csv')
        ep_read_calculate_pollutants_file(filepath, source_schema, conf_schema)
        report.record.message('\nCalculate pollutants file imported: {}.', os.path.abspath(filepath))
        ep_calculate_pollutants(source_schema, conf_schema)

    # control sums after calculate pollutants
    report.sum.sql('Control sums after calculate pollutants - emission per specie and eset [t/year]',
       SUM_SQL, conf_schema=conf_schema, source_schema=source_schema)

    # read sources with activity data
    log.info('Activity data import.')
    with exec_timer("activity data import") as timer:
        inv_file = ep_cfg.input_params.activity_data_inventories
        if os.path.exists(inv_file):
            ep_read_sources(inv_file, path, source_schema, conf_schema, 'activity')
            report.record.message('ACtivity data inventory file imported: {}.', os.path.abspath(inv_file))
        else:
            log.info('... no activity data inventory file found.')
            report.record.message('Activity data inventory file not imported.')
            

def ep_read_sources(inv_file, path, source_schema, conf_schema, input_type):
    """ Reads emission (activity) sources data listed in inventory file.

    Parameters:
    inv_file (str): name of the inventory file with listed emission files to import
    path (str): path to input files
    source_schema (str): schema to import data
    conf_schema (str): schema where configuration data are stored
    input_type (str): if 'emission' it is treated as emission sources, otherwise it is assumed the data are activity
    """

    confspec_file = ep_internal_path('conf', 'configspec-sources.conf')

    with open(inv_file, mode='r', encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
        _ = ep_read_header(reader)
        lines = list(reader)

    old_inv_name = ''
    for line in lines:
        if ep_skip_line(line):
            continue

        inv_name = line[0]
        file_name = line[1]

        if inv_name is None or inv_name == '':
            # the file is not real emission inventory file
            # it contains just geometry values
            inv_id = 0
            report.record.message('Geometry:')
        else:
            # the real emission file - register inventory and read species and mappings
            # fetch inventory id
            inv_id, new_inv = ep_register_inventory(inv_name, source_schema)
            if old_inv_name != inv_name: report.record.message('Inventory: {}.', inv_name)

            # read category and emission specie mappings if necessary
            if new_inv:
                read_category_mapping(inv_id, source_schema, path, inv_name)
                read_specie_mapping(inv_id, conf_schema, source_schema, path, inv_name, input_type)
                report.record.message('   Specie and category mapping imported from directory: {}.', os.path.abspath(path))
                
            try:
                ep_connection.commit()
            except psycopg2.Error:
                pass

        # read raw data
        # check if the raw file is already read
        file_id, file_table, new_file = ep_get_source_file_id(file_name, source_schema)

        if new_file:
            log.fmt_info('Importing file {}.', file_name)

            # file raw table name
            file_table = '_'.join([TABLE_IN_RAW, file_name])

            # raw file path
            file_path = os.path.join(path, line[2])
            
            # get config from the info file
            ifile = os.path.join(path, line[3])
            if os.path.exists(ifile):
                config = ConfigFile(ifile, confspec_file).values()
            else:
                log.fmt_error('Info file {} not found.', ifile)

            ep_read_raw_sources(file_path, source_schema, file_table, config)
            report.record.message('   Sources file {} imported: {}.', file_name, os.path.abspath(file_path))
            
            # register new file
            file_id = ep_register_source_file(file_name, inv_id, file_path, file_table, source_schema)
            
            # debug log
            log.fmt_debug('Source file {} imported as ID {} into table {}.', file_name, file_id, file_table)           

        old_inv_name = inv_name
        

    # process source files into source sets in next cycle
    # due to parallelization of the process
    # (we need avoid double registering of one source file which
    # has more source sets)

    log.info('Processing imported emission sources into inner FUME format.')
    report.record.message('\nFollowing esets were processed:')

    for line in lines:
        if ep_skip_line(line):
            continue

        inv_name = line[0]
        
        # get file info
        file_name = line[1]
        file_id, file_table, new_file = ep_get_source_file_id(file_name, source_schema)
        
        # get config from the info file
        file_info = line[3]
        ifile = os.path.join(path, file_info)
        config = ConfigFile(ifile, confspec_file).values()
        
        # get eset name
        if (len(line) > 4) and (line[4].strip() != ''):
            eset_name = line[4]
        else:
            eset_name = file_name
        
        log.fmt_info('Processing set {} into inner FUME format.', eset_name)
        
        # processing geometries 
        # get name of geometry set
        geom_name = config.geom_name
        # TODO check type and length of geom_name!!!
        if geom_name is not None:
            gset_name = geom_name
        else:
            gset_name = eset_name
            
        if len(line) > 5:
            set_filter = line[5].strip()
        else:
            set_filter = ""

        # check if geometry is already available
        gset_id, new_gset = ep_get_gset_id(gset_name, source_schema)

        if new_gset:
            # get geometry info
            gset_info = config.geom_info
            if gset_info is None:
                # the file has no separate geom_info files
                # the description of the geometry is included in file_info
                gset_info = [file_info]

            # process raw inventory data into a new source set
            # register geometry set
            gset_id = ep_register_gset(gset_name, file_table, set_filter, file_path, gset_info, file_id, source_schema)

            # process raw geometry into ep_in_geometries
            # process all geom_info files
            for ginfo in gset_info:
                # get config from the info file
                gfile = os.path.join(path, ginfo)
                gconfig = ConfigFile([ifile, gfile], confspec_file).values()

                # process raw geometries
                log.debug('Processing raw geometries ', gset_name)
                ep_process_raw_geometries(ep_connection, source_schema, file_table, gset_id, set_filter, gconfig)

            log.fmt_debug('Geometry set {} imported as ID {}.', gset_name, gset_id)

        if inv_name is not None and inv_name != '':
            # get eset_id
            eset_id, new_eset = ep_get_eset_id(eset_name, source_schema)

            if new_eset:
                # process raw inventory data into a new source set
                # assign scenario (if given) to eset
                if (len(line) > 6):
                    scenario_names = [scen.strip() for scen in line[6].strip().split(',')]
                else:
                    scenario_names = [""]

                # assign vdistribution (if given) to eset
                if (len(line) > 7):
                    vdist_names = line[7].strip().split(',')
                else:
                    vdist_names = [""]

                # register new eset
                eset_id = ep_register_eset(eset_name, file_id, ifile, gset_id, set_filter, config.data_type, source_schema, scenario_names, vdist_names)

                # process raw inventory data
                log.fmt_info('Processing emission raw file {} into set {}.',
                              file_name, eset_name)
                temp_view = 'RAW_SOURCES_TEMP'
                report.record.message('Eset: {}, Filter applied: {filter}, Scenarios applied: {scenario}, Vertical distributions assigned: {vdist}',
                   eset_name,
                   filter=set_filter if set_filter else "-",
                   scenario=', '.join(scenario_names) if scenario_names[0] else "-",
                   vdist=', '.join(vdist_names) if vdist_names[0] else "-")
                ep_process_raw_sources(ep_connection, source_schema, file_table, temp_view, eset_id, set_filter, config)
                log.sql_debug(ep_connection)

                report.record.sql('   Emission categories found in eset',
                   'SELECT DISTINCT cat_id::text FROM "{source_schema}".ep_in_emissions '\
                   'JOIN "{source_schema}".ep_in_sources using (source_id) '\
                   'WHERE eset_id = {} ',
                   eset_id, source_schema=source_schema)
                report.record.sql('   Species found in eset',
                   'SELECT DISTINCT name::text FROM "{source_schema}".ep_in_emissions '\
                   'JOIN "{source_schema}".ep_in_sources using (source_id) '\
                   'JOIN "{conf_schema}".ep_in_species using (spec_in_id) '\
                   'WHERE eset_id = {} ',
                   eset_id, conf_schema=conf_schema, source_schema=source_schema)
                log.fmt_debug('Emission set {} processed as eset_id {}.', eset_name, eset_id)


def read_category_mapping(id, schema, path, inv_name):

    """ Reads category mapping for inventory. Inserts data in table schema.ep_classification_mapping. """

    tablename = 'ep_classification_mapping'

    # create filename
    filename = "_".join(["category", inv_name]) + "." + "csv"
    filepath = os.path.join(path, filename)

    cur = ep_connection.cursor()
    with open(filepath, mode='r', encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        _ = ep_read_header(reader)
        lines = []
        for line in reader:
            if ep_skip_line(line):
                continue
            lines.append([id, line[0], line[1]])
        psycopg2.extras.execute_values(cur, 'INSERT INTO "{}".{} (inv_id, orig_cat_id, cat_id) VALUES %s'.format(schema, tablename), lines)

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
        _ = ep_read_header(reader)

        lines = []
        for line in reader:
            if ep_skip_line(line):
                continue

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
            lines.append([id, line[0], spec_in_id, unit, conv_factor])

        psycopg2.extras.execute_values(cur, 'INSERT INTO "{}".{} (inv_id, orig_name, {}, unit, conv_factor) VALUES %s'.format(schema, tablename, id_col_name), lines)

    ep_connection.commit()
    cur.close()


def ep_read_calculate_pollutants_file(filepath, schema, conf_schema):
    """ Reads text file filepath with expressions to calculate new inventory pollutants.
    New pollutant can be described as linear combination of existing ones. E.g. POL = 0.95 * POL1 + POL2 - 1.1 * POL3
    Equations are stored in table ep_calculate_pollutants"""

    import re

    cp_table = 'ep_calculate_pollutants'

    if os.path.isfile(filepath):
        # populate ep_calculate_pollutants table
        cur = ep_connection.cursor()
        cur.execute('TRUNCATE "{}"."{}"'.format(schema, cp_table))

        with open(filepath, mode='r', encoding='utf8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            _ = ep_read_header(reader)
            categories = []
            cat_count = {}
            lines = []
            for line in reader:
                if ep_skip_line(line):
                    continue

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

                        lines.append([category, output_id, input_id, coef, cat_count[category]])

            psycopg2.extras.execute_values(cur, 'INSERT INTO "{}".{} (cat_id, spec_out_id, spec_inp_id, coef, cat_order) '
                                'VALUES %s'.format(schema, cp_table), lines)

        ep_connection.commit()
        cur.close()


def ep_calculate_pollutants(schema, conf_schema):
    """ Calculates new inventory pollutants into ep_in_emissions table. Based on equations stored
    in ep_calculate_pollutants. If pollutant is already there for source/category, nothing happens. """

    emis_table = 'ep_in_emissions'
    cp_table = 'ep_calculate_pollutants_all'

    cur = ep_connection.cursor()
    
    cur.callproc('ep_find_missing_calculate_pollutants', [conf_schema, schema])
    log.sql_debug(ep_connection)
    
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


def ep_read_scenarios(scenarios_list_file, source_schema, conf_schema):
    """ Reads emission scenario definition. Scenario files are listed in inv_file. They are assumed to be in the same directory as inv_file. """

    path = os.path.dirname(scenarios_list_file)
    cur = ep_connection.cursor()

    # attribute filters are stored separately from the factors
    # equality of filter definition is tested with all spaces removed
    filters = {}
    remove_spaces_filter = str.maketrans({' ': '', '\t': '', '\n': ''})

    with open(scenarios_list_file, mode='r', encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader)
        lines = list(reader)

        for line in lines:
            try:
                scenario_name = line[0]
            except IndexError:
                continue

            if scenario_name.startswith('#'):
                continue

            scenario_name = line[0]
            file_name = line[1].strip()
            scenario_file = os.path.join(path, file_name)

            if os.path.exists(scenario_file):
                with open(scenario_file, mode='r', encoding='utf8') as scenfile:
                    scnreader = csv.reader(scenfile, delimiter=',', quotechar='"')
                    header = next(scnreader)
                    header = [s.strip() for s in header]
                    
                    try:
                        filter_idx = header.index('filter')
                    except ValueError:
                        filter_idx=None

                    try:
                        cat_idx = header.index('category')
                    except ValueError:
                        cat_idx=None

                    try:
                        spec_idx = header.index('species')
                    except ValueError:
                        spec_idx=None

                    try:
                        op_idx = header.index('operation')
                    except ValueError:
                        op_idx=None

                    try:
                        factor_idx = header.index('value')
                    except ValueError:
                        log.debug('*** No "value" column found in the scenario file {}. Skipping...'.format(scenario_file))
                        continue

                    scenario_id = ep_register_scenario(scenario_name, scenario_file, source_schema)
                    log.debug('*** Importing scenario ', scenario_name,
                             ' from file ', scenario_file, ' as ', scenario_id)

                    for row in scnreader:
                    	# skip comments and blank lines
                        if len(row)== 0 or row[0].startswith('#'):
                            continue

                        if filter_idx is None:
                            attribute_filter = ''                                            
                        else:
                            attribute_filter = row[filter_idx].translate(remove_spaces_filter)
                        try:
                            attribute_filter_id = filters[attribute_filter]
                        except KeyError:
                            cur.execute('INSERT INTO "{schema}"."ep_scenario_filters" '
                                         '(filter_definition) VALUES (%s) RETURNING filter_id'
                                             .format(schema=source_schema),
                                         (attribute_filter, ))
                            attribute_filter_id = cur.fetchone()[0]
                            filters[attribute_filter] = attribute_filter_id

                        if cat_idx is None or not(row[cat_idx]):
                            cur.execute('SELECT cat_id FROM "{schema}"."ep_emission_categories"'.format(schema=conf_schema))
                            cat_id = [i[0] for i in cur.fetchall()]
                        else:
                            cat_id = [int(row[cat_idx])]

                        if spec_idx is None or not(row[spec_idx]):
                            cur.execute('SELECT spec_in_id FROM "{schema}"."ep_in_species"'.format(schema=conf_schema))
                            spec_in_id = [i[0] for i in cur.fetchall()]
                        else:
                            cur.execute('SELECT spec_in_id FROM "{schema}"."ep_in_species" '
                                        'WHERE name=%s'.format(schema=conf_schema), (row[spec_idx], ))
                            spec_in_id = [cur.fetchone()[0]]

                        factor = float(row[factor_idx])

                        if op_idx is None or not row[op_idx].strip():
                            operation = '*'
                        else:
                            operation = row[op_idx].strip()

                        scenarios_data = list(itertools.product([scenario_id], [attribute_filter_id], cat_id, spec_in_id, [factor], [operation]))
                        psycopg2.extras.execute_values(cur,
                                                'INSERT INTO "{schema}"."ep_scenario_factors" '
                                                '(scenario_id, filter_id, cat_id, spec_in_id, factor, operation) VALUES %s'
                                                    .format(schema=source_schema),
                                                scenarios_data)
                report.record.message('Scenario definition file imported: {}.', os.path.abspath(scenario_file))
            else:
                log.fmt_debug('scenario file {} not found'.format(scenario_file))
    
    # apply category hierarchy
    cur.callproc('ep_find_missing_scenario_factors', [conf_schema, source_schema])

    ep_connection.commit()
    cur.close()


def ep_read_vdistributions(inv_file, source_schema, conf_schema):
    """ Reads vertical distribution definition. Vertical distribution files are listed in inv_file. They are assumed to be in the same directory as inv_file. """

    factor_table = 'ep_vdistribution_factors'

    path = os.path.dirname(inv_file)
    cur = ep_connection.cursor()

    with open(inv_file, mode='r', encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader)
        lines = list(reader)

        for line in lines:
            try:
                vdistribution_name = line[0]
            except IndexError:
                continue

            vdistribution_name = line[0]
            file_name = line[1].strip()
            filepath = os.path.join(path, file_name)

            if os.path.exists(filepath):
                vdistribution_id = ep_register_vdistribution(vdistribution_name, source_schema)

                with open(filepath, mode='r', encoding='utf8') as vdistfile:
                    vdreader = csv.reader(vdistfile, delimiter=',', quotechar='"')
                    next(vdreader)
                    rows = list(vdreader)

                    for row in rows:
                        try:
                            cat_id = row[0]
                        except IndexError:
                            continue

                        if cat_id.startswith('#'):
                            continue

                        if not cat_id:
                            cur.execute('SELECT cat_id FROM "{schema}"."ep_emission_categories"'.format(schema=conf_schema))
                            cat_id = [i[0] for i in cur.fetchall()]
                        else:
                            cat_id = [cat_id]

                        level = row[1]

                        height = row[2]
                        factor = row[3]

                        for c in cat_id:
                            cur.execute( 'INSERT INTO "{schema}"."{table}" '
                                         '(vdistribution_id, cat_id, level, height, factor) VALUES (%s, %s, %s, %s, %s) '
                            .format(schema=source_schema, table=factor_table), (vdistribution_id, int(c), int(level), float(height), float(factor)))

                ep_connection.commit()
                report.record.message('Vertical distribution file imported: {}.', os.path.abspath(filepath))
            else:
                log.fmt_debug('Vertical distribution file {} not found', filepath)

    cur.close()


def ep_register_scenario(scenario_name, scenario_file, schema):
    """
    Returns scenario_id based on scenario name from table source_schema.ep_scenario_list. If scenario is already here, it is deleted.
    """

    with ep_connection.cursor() as cur:
        cur.execute('DELETE FROM "{schema}"."ep_scenario_list" WHERE scenario_name=%s'.format(schema=schema), (scenario_name, ))
        cur.execute('INSERT INTO "{schema}"."ep_scenario_list" (scenario_name, scenario_file) VALUES (%s, %s) RETURNING scenario_id'
                    .format(schema=schema), (scenario_name, scenario_file))
        scenario_id = cur.fetchone()[0]
        ep_connection.commit()

    return scenario_id


def ep_register_vdistribution(vdistribution_name, schema):
    """
    Returns vdistribution_id based on vdistribution name from table source_schema.ep_vdistribution_names. If vdistribution is already here, it is deleted.
    """

    with ep_connection.cursor() as cur:
        cur.execute('DELETE FROM "{schema}"."ep_vdistribution_names" WHERE vdistribution_name=%s'.format(schema=schema), (vdistribution_name, ))
        cur.execute('INSERT INTO "{schema}"."ep_vdistribution_names" (vdistribution_name) VALUES (%s) RETURNING vdistribution_id'
                    .format(schema=schema), (vdistribution_name, ))
        vdistribution_id = cur.fetchone()[0]
        ep_connection.commit()

    return vdistribution_id

