import os

from input.ep_csv2table import ep_csv2table, \
    ep_read_model_specie_names, ep_read_sp_mod_specie_mapping, ep_read_ep_comp_mechanisms_assignment, ep_read_gspro
from input.ep_shp import ep_shp2postgis
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_connection, ep_create_schema


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
    if os.path.isfile(filename):
        fieldnames = ['name', 'description']
        ep_csv2table(ep_connection, filename, conf_schema, 'ep_in_species', fieldnames)

    # import activity units list
    filename = os.path.join(path, 'activity_units.csv')
    if os.path.isfile(filename):
        fieldnames = ['name', 'description']
        ep_csv2table(ep_connection, filename, conf_schema, 'ep_activity_units', fieldnames)

    # emission categories
    filename = os.path.join(path, 'emission_categories.csv')
    fieldnames = ['cat_id', 'name', 'parent']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_emission_categories',
                 fieldnames)

    # import aq model list
    filename = os.path.join(path, 'model_list.csv')
    fieldnames = ['name', 'version']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_aq_models', fieldnames)

    # speciation tables ########################################################################
    # import chemical mechanisms list
    filename = os.path.join(path, 'speciations', 'mechanism_list.csv')
    fieldnames = ['name', 'description', 'type']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_mechanisms', fieldnames)

    # import model specie names
    filename = os.path.join(path, 'speciations', 'model_specie_names.csv')
    ep_read_model_specie_names(ep_connection, filename, conf_schema, 'ep_mod_species')

    # speciation profile species
    filename = os.path.join(path, 'speciations', 'sp_species.csv')
    fieldnames = ['mechanism_name', 'name']
    foreign_key = [('mechanism_name', 'ep_mechanisms', 'name', 'mech_id')]
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_sp_species', fieldnames, foreign_key)

    # speciation profile specie to model specie mapping
    filename = os.path.join(path, 'speciations', 'sp_mod_specie_mapping.csv')
    if os.path.isfile(filename):
        ep_read_sp_mod_specie_mapping(ep_connection, filename, conf_schema, 'ep_sp_mod_specie_mapping')

    if os.path.isfile(os.path.join(path, 'speciations', 'speciation_profiles.csv')):
        # chemical compounds list
        filename = os.path.join(path, 'speciations', 'compounds.csv')
        fieldnames = ['chem_comp_id', 'name', 'mol_weight', 'non_vol']
        ep_csv2table(ep_connection, filename, conf_schema, 'ep_chem_compounds', fieldnames, naval='-99')

        # chemical compounds to mechanism species mapping
        filename = os.path.join(path, 'speciations', 'comp_mechanisms_assignment.csv')
        ep_read_ep_comp_mechanisms_assignment(ep_connection, filename, conf_schema, 'ep_comp_mechanisms_assignment')

        # speciation profiles
        filename = os.path.join(path, 'speciations', 'speciation_profiles.csv')
        fieldnames = ['cat_id', 'inv_specie', 'chem_comp_id', 'fraction']
        foreign_key = [('inv_specie', 'ep_in_species', 'name', 'spec_in_id')]
        ep_csv2table(ep_connection, filename, conf_schema, 'ep_comp_cat_profiles', fieldnames, foreign_key, naval='#N/A')

    # read gspro files
    gspro_files = ep_cfg.input_params.speciation_params.gspro_files
    for gfile in gspro_files:
        filename = os.path.join(path, 'speciations', gfile)
        ep_read_gspro(ep_connection, filename, conf_schema, 'ep_gspro_sp_factors')

    # time profiles ##############################################################################
    filename = os.path.join(path, 'time_var', 'tv_def.csv')
    fieldnames = ['tv_id', 'name', 'resolution']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_time_var', fieldnames)

    filename = os.path.join(path, 'time_var', 'tv_values.csv')
    fieldnames = ['tv_id', 'period', 'tv_factor']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_time_var_values', fieldnames)

    filename = os.path.join(path, 'time_var', 'tv_mapping.csv')
    fieldnames = ['cat_id', 'tv_id']
    ep_csv2table(ep_connection, filename, conf_schema, 'ep_time_var_mapping', fieldnames)

    filename = os.path.join(path, 'time_var', 'tv_series.csv')
    if os.path.isfile(filename):
        fieldnames = ['cat_id', 'time_loc', 'tv_factor']
        ep_csv2table(ep_connection, filename, conf_schema, 'ep_time_var_series', fieldnames)

    # timezones for all countries of the world
    # it reads timezone shapefile tz_world_mp.shp into static data
    filename = os.path.join(path, 'shp', 'tz_world_mp.shp')
    tablename = 'ep_tz_world'
    flds_orig = ['TZID']
    flds_ep = ['tz_id']
    flds_idx = ['tz_id']
    print('Import', filename, static_schema, tablename)
    ep_shp2postgis(filename, schema=static_schema, tablename=tablename, tablesrid=4326)
    for i in range(0, len(flds_orig)):
        cur.execute('alter table "{}"."{}" rename column "{}" to "{}"'.format(static_schema, tablename, flds_orig[i], flds_ep[i]))
    for i in range(0, len(flds_idx)):
        cur.execute('create index if not exists "{}" on "{}"."{}" ("{}")'.format(tablename + '_' + flds_idx[i], static_schema, tablename, flds_idx[i]))

    cur.close()
    ep_connection.commit()
