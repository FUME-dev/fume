"""
Description:
This module imports data to static and confs schemas.
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

from input.ep_csv2table import ep_csv2table, \
    ep_read_model_specie_names, ep_read_sp_mod_specie_mapping, ep_read_ep_comp_mechanisms_assignment, ep_read_gspro
from input.ep_shp import ep_shp2postgis
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_connection, ep_create_schema
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
import lib.ep_reporting
report = lib.ep_reporting.Reporter(__name__)

def init_static(path=None, conf_schema=None, static_schema=None):

    if path is None:
        path = ep_cfg.input_params.static

    if static_schema is None:
        static_schema = ep_cfg.db_connection.static_schema

    if conf_schema is None:
        conf_schema = ep_cfg.db_connection.conf_schema

    cur = ep_connection.cursor()

    ep_create_schema(static_schema, None)
    ep_create_schema(conf_schema, 'ep_create_conf_tables.sql')

    # import inventory specie list
    filename = os.path.join(path, 'inventory_species.csv')
    fieldnames = ['name', 'description']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_in_species', fieldnames)
    report.record.message('Inventory species list imported: {}.', os.path.abspath(filename))

    # import activity units list
    filename = os.path.join(path, 'activity_units.csv')
    if os.path.isfile(filename):
        fieldnames = ['name', 'description']
        ep_csv2table(ep_connection, filename, conf_schema, 'ep_activity_units', fieldnames)
        report.record.message('Activity unit list imported: {}.', os.path.abspath(filename))
    else:
        report.record.message('Activity unit list not imported.')

    # emission categories
    filename = os.path.join(path, 'emission_categories.csv')
    fieldnames = ['cat_id', 'name', 'parent']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_emission_categories',
                 fieldnames)
    report.record.message('Emission categories list imported: {}.', os.path.abspath(filename))

    # import aq model list
    filename = os.path.join(path, 'model_list.csv')
    fieldnames = ['name', 'version']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_aq_models', fieldnames)
    report.record.message('Model names list imported: {}.', os.path.abspath(filename))

    # speciation tables ########################################################################
    # import chemical mechanisms list
    filename = os.path.join(path, 'speciations', 'mechanism_list.csv')
    fieldnames = ['name', 'description', 'type']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_mechanisms', fieldnames)
    report.record.message('Mechanism list imported: {}.', os.path.abspath(filename))

    # import model specie names
    filename = os.path.join(path, 'speciations', 'model_specie_names.csv')
    ep_read_model_specie_names(ep_connection, filename, conf_schema, 'ep_mod_species')
    report.record.message('Model species names imported: {}.', os.path.abspath(filename))

    # speciation profile species
    filename = os.path.join(path, 'speciations', 'sp_species.csv')
    fieldnames = ['mechanism_name', 'name']
    foreign_key = [('mechanism_name', 'ep_mechanisms', 'name', 'mech_id')]
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_sp_species', fieldnames, foreign_key)
    report.record.message('Chemical mechanisms species imported: {}.', os.path.abspath(filename))

    # speciation profile specie to model specie mapping
    filename = os.path.join(path, 'speciations', 'sp_mod_specie_mapping.csv')
    if os.path.isfile(filename):
        ep_read_sp_mod_specie_mapping(ep_connection, filename, conf_schema, 'ep_sp_mod_specie_mapping')
        report.record.message('Model specie mapping (calculation) imported: {}.', os.path.abspath(filename))
    else:
        report.record.message('Model specie mapping (calculation) not imported.')

    if os.path.isfile(os.path.join(path, 'speciations', 'speciation_profiles.csv')):
        # chemical compounds list
        filename = os.path.join(path, 'speciations', 'compounds.csv')
        fieldnames = ['chem_comp_id', 'name', 'mol_weight', 'non_vol']
        ep_csv2table(ep_connection, filename, conf_schema, 'ep_chem_compounds', fieldnames, naval='-99')
        report.record.message('Chemical compounds list imported: {}.', os.path.abspath(filename))

        # chemical compounds to mechanism species mapping
        filename = os.path.join(path, 'speciations', 'comp_mechanisms_assignment.csv')
        ep_read_ep_comp_mechanisms_assignment(ep_connection, filename, conf_schema, 'ep_comp_mechanisms_assignment')
        report.record.message('Chemical coumpounds to species mapping imported: {}.', os.path.abspath(filename))

        # speciation profiles
        filename = os.path.join(path, 'speciations', 'speciation_profiles.csv')
        fieldnames = ['cat_id', 'inv_specie', 'chem_comp_id', 'fraction']
        foreign_key = [('inv_specie', 'ep_in_species', 'name', 'spec_in_id')]
        ep_csv2table(ep_connection, filename, conf_schema, 'ep_comp_cat_profiles', fieldnames, foreign_key, naval='#N/A')
        report.record.message('Speciation profiles imported: {}.', os.path.abspath(filename))

    # read gspro files
    gspro_files = ep_cfg.input_params.speciation_params.gspro_files
    for gfile in gspro_files:
        filename = os.path.join(path, 'speciations', gfile)
        ep_read_gspro(ep_connection, filename, conf_schema, 'ep_gspro_sp_factors')
        report.record.message('Speciation gspro file imported: {}.', os.path.abspath(filename))

    # time profiles ##############################################################################
    filename = os.path.join(path, 'time_var', 'tv_def.csv')
    fieldnames = ['tv_id', 'name', 'resolution']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_time_var', fieldnames)
    report.record.message('Time profiles definition imported: {}.', os.path.abspath(filename))

    filename = os.path.join(path, 'time_var', 'tv_values.csv')
    fieldnames = ['tv_id', 'period', 'tv_factor']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_time_var_values', fieldnames)
    report.record.message('Time profiles values imported: {}.', os.path.abspath(filename))

    filename = os.path.join(path, 'time_var', 'tv_mapping.csv')
    fieldnames = ['cat_id', 'tv_id']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_time_var_mapping', fieldnames)
    report.record.message('Time profiles mapping imported: {}.', os.path.abspath(filename))

    filename = os.path.join(path, 'time_var', 'tv_series.csv')
    if os.path.isfile(filename):
        fieldnames = ['cat_id', 'time_loc', 'tv_factor']
        ep_csv2table(ep_connection, filename, conf_schema, 'ep_time_var_series', fieldnames)
        report.record.message('Time series imported: {}.', os.path.abspath(filename))
    else:
        report.record.message('Time series not imported.')

    # check whether time profiles sums are expected values
    sql_check = 'SELECT tv_id FROM ( ' \
       'SELECT tv_id, sum(tv_factor) AS suma FROM {conf_schema}.ep_time_var_values etvv ' \
       'JOIN {conf_schema}.ep_time_var etv USING (tv_id) '\
       'WHERE resolution={resolution} GROUP BY tv_id) a WHERE suma <> {sum_value}'
    report.check.sql('Checking sums of month time factors.', 'Following month time profiles ids do not sum up to 12', 
       sql_check, conf_schema=conf_schema, resolution=3, sum_value=12)    
    report.check.sql('Checking sums of week time factors.', 'Following week time profiles ids do not sum up to 7', 
       sql_check, conf_schema=conf_schema, resolution=2, sum_value=7)    
    report.check.sql('Checking sums of day time factors.', 'Following day time profiles ids do not sum up to 24', 
       sql_check, conf_schema=conf_schema, resolution=1, sum_value=24)      

    # timezones for all countries of the world
    # it reads timezone shapefile tz_world_mp.shp into static data
    filename = os.path.join(path, 'shp', 'tz_world_mp.shp')
    tablename = 'ep_tz_world'
    flds_orig = ['TZID']
    flds_ep = ['tz_id']
    flds_idx = ['tz_id']
    log.fmt_info('Importing timezones shapefile into {}.{}.', static_schema, tablename)
    ep_shp2postgis(filename, schema=static_schema, tablename=tablename, tablesrid=4326)
    for i in range(0, len(flds_orig)):
        cur.execute('alter table "{}"."{}" rename column "{}" to "{}"'.format(static_schema, tablename, flds_orig[i], flds_ep[i]))
    for i in range(0, len(flds_idx)):
        cur.execute('create index if not exists "{}" on "{}"."{}" ("{}")'.format(tablename + '_' + flds_idx[i], static_schema, tablename, flds_idx[i]))
    report.record.message('Timezones imported: {}.', os.path.abspath(filename))

    cur.close()
    ep_connection.commit()
