"""
Description: implementation of the case tasks

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

from datetime import datetime, timedelta, timezone
import importlib
import numpy as np
from lib.ep_libutil import ep_create_grid, ep_create_schema, ep_projection_params, \
     ep_connection, ep_rtcfg, ep_model_id, ep_mechanism_ids, \
     ep_dates_times, ep_internal_path
from lib.ep_config import ep_cfg, ConfigFile
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)
import lib.ep_reporting
report = lib.ep_reporting.Reporter(__name__)


def create_new_case():
    """
    Initialization of a new case:

    - create case schema (if scratch is True or schema does not exist)
    - create and populate timezone definition tables
    - create timezone aware grid table
    - find air quality model id and mechanism ids from database
    - fill out species table
    """
    case_schema = ep_cfg.db_connection.case_schema
    srid = ep_cfg.projection_params.projection_srid

    if ep_cfg.domain.create_grid:
        ep_create_grid()

    # create new database case_schema for case and basic case tables
    ep_create_schema(case_schema, 'ep_create_case_tables.sql', srid)

    # create grid envelope for speedup of the geometric operations
    ep_create_grid_env()

    # create complete timezones for the case
    ep_create_timezones()

    # create grid with timezone information
    ep_create_grid_tz()

    # save model_id to runtime ConfigObject
    ep_model_id()

    # save mechanism_id to runtime ConfigObject
    ep_mechanism_ids()
    cur = ep_connection.cursor()

    # TODO
    # Add fk constraint to ep_sources_grid referencing grid_id

    # create specie table for the case
    fill_out_species_table(ep_connection, ep_rtcfg['model_id'], ep_rtcfg['mechanism_ids'])

    ep_connection.commit()
    cur.close()

def ep_create_grid_env():
    """
    This procedure creates envelope table ep_grid_env in case schema
    """
    case_schema = ep_cfg.db_connection.case_schema
    conf_schema = ep_cfg.db_connection.conf_schema
    grid_name = ep_cfg.domain.grid_name
    log.debug('Create grid envelope: ', case_schema, 'ep_grid_env', conf_schema, grid_name)
    cur = ep_connection.cursor()
    try:
        if ep_cfg.domain.create_grid:
            # regular grid created by fume, envelope is a rectangle
            cur.execute('select min(xmi), min(ymi), max(xma), max(yma) from "{}"."{}"'.format(conf_schema, grid_name))
            row = cur.fetchone()
            sqltext = 'insert into "{}".ep_grid_env select ST_MakeEnvelope({}, {}, {}, {}, {}) as geom'.format(
                       case_schema, row[0], row[1], row[2], row[3], ep_cfg.projection_params.projection_srid)
            log.debug('sqltext:', sqltext)
            cur.execute(sqltext)
        else:
            # user supplied grid possibly irregular
            cur.execute('insert into "{}".ep_grid_env select ST_Envelope((select ST_Union('
                    '(array(select geom from "{}"."{}"))))) as geom'.format(case_schema, conf_schema, grid_name))
        cur.close()
        ep_connection.commit()
    except Exception as e:
        log.fmt_error("create_grid_envelope: unable to create ep_grid_env table. \n Error: {}", e)
        ep_connection.rollback()
        raise e
    finally:
        log.sql_debug(ep_connection)


def ep_create_timezones():
    """
    This procedure imports TZ from raw table into case
    only needed timezones are imported/created
    """

    cur = ep_connection.cursor()
    # function is implemented in pgplsql on server from performance reasons
    log.debug('ep_case_timezones: ', ep_cfg.db_connection.conf_schema, ep_cfg.domain.grid_name, \
        ep_cfg.db_connection.static_schema, 'ep_tz_world',
        ep_cfg.db_connection.case_schema, 'ep_timezones',
        'ep_grid_env', ep_cfg.domain.grid_timezone,
        ep_cfg.projection_params.projection_srid)
    try:
        cur.execute('select ep_case_timezones(%s,%s,%s,%s,%s,%s,%s,%s,%s)', ( \
            ep_cfg.db_connection.conf_schema, ep_cfg.domain.grid_name, \
            ep_cfg.db_connection.static_schema, 'ep_tz_world',
            ep_cfg.db_connection.case_schema, 'ep_timezones',
            'ep_grid_env', ep_cfg.domain.grid_timezone,
            ep_cfg.projection_params.projection_srid))
    except Exception as e:
        log.fmt_error("create_timezones: unable to create table 'ep_timezones'. \n Error: {}", e)
        ep_connection.rollback()
        raise e
    finally:
        log.sql_debug(ep_connection)


def ep_create_grid_tz():
    case_schema = ep_cfg.db_connection.case_schema
    conf_schema = ep_cfg.db_connection.conf_schema
    grid_name = ep_cfg.domain.grid_name
    log.debug('Create grid with timezones: ', case_schema, 'ep_grid_tz', conf_schema, grid_name, 'ep_timezones', ep_cfg.projection_params.projection_srid)
    cur = ep_connection.cursor()
    try:
        cur.callproc('ep_create_grid_tz', [case_schema, 'ep_grid_tz', conf_schema, grid_name, 'ep_timezones', ep_cfg.projection_params.projection_srid])
        cur.close()
        ep_connection.commit()
    except Exception as e:
        log.fmt_error("create_grid_tz: unable to create ep_grid_tz table {}. \n Error: {}", 'ep_grid_tz', e)
        ep_connection.rollback()
        raise e
    finally:
        log.sql_debug(ep_connection)


def prepare_conf():
    """
    This procedure does necessary steps to prepare the case
    to be precesses. It actually reads/creates projection from params
    and creates original grid in conf schema if required
    by configuration. More action might be added in future.
    """
    proj, p_alp, p_bet, p_gam, XCENT, YCENT = ep_projection_params()
    report.record.message('The emissions will be calculated in the projection: \n'\
       'srid: {}, projection: {}, alpha: {}, beta: {}, gamma: {}, x_center: {}, y_center: {}', 
       ep_rtcfg['projection_params']['srid'], proj, p_alp, p_bet, p_gam, XCENT, YCENT)
    report.record.sql('Coordinate system name', 
       'SELECT substring(srtext from \'"(.*?)"\') FROM spatial_ref_sys WHERE srid = {}', ep_rtcfg['projection_params']['srid'])
    #ep_create_grid()
    # Suppose this is wrong - the new grid is created in create_new_case,
    # the grid needs to be converted to ep_grid_tz in case schema and it is NOT done outside create_new_case
    # thus the changed grid is ignored in the case!!! Changed grid definition thus require create_new_case
    # where the grid is created IF REQUIRED (we may supply our own grid
    # TODO check somebody else
    ep_dates_times()
    datestimes = ep_rtcfg['run']['datestimes']
    report.record.message('\n The calculation time span is set from {} to {} {}.',
       datestimes[0].strftime("%d.%m.%Y %H:%M:%S"), 
       datestimes[-1].strftime("%d.%m.%Y %H:%M:%S"),
       datestimes[-1].tzname())  


def collect_meteorology():
    """
    Import met data from met input. At this stage, the ep_rtcfg['required_met']
    is filled with all the meteorology needed, if empty, get_met will do
    nothing.

    The list of meteorogy fields is saved in ep_rtcfg['met']
    """
    init_external_models()
    from met.ep_met import get_met
    get_met(list(ep_rtcfg['required_met']))


def process_case_spec_time():
    """
    Main case procedure:

    - calculate speciation splits & apply
    - calculate time dissaggregation factors
    """
    itzone_out = ep_cfg.run_params.time_params.itzone_out
    conf_schema = ep_cfg.db_connection.conf_schema
    source_schema = ep_cfg.db_connection.source_schema
    case_schema = ep_cfg.db_connection.case_schema

    ep_model_id()  # save model_id to runtime ConfigObject
    ep_mechanism_ids()  # save mechanism ids to runtime ConfigObject

    cur = ep_connection.cursor()
    # chemical speciation
    cur.callproc('ep_speciation_splits', [ep_rtcfg['model_id'],
                                          ep_rtcfg['mechanism_ids'],
                                          conf_schema, case_schema])
    log.sql_debug(ep_connection)
    
    # check whether all categories in the domain have speciation factors
    report.check.sql('\n Checking if all categories/species have chemical specations assigned.', 
       'Following (category, specie) combinations do not have assigned a chemical speciation', 
       'SELECT DISTINCT se.cat_id, name FROM "{case_schema}".ep_sg_emissions se ' \
       'JOIN "{conf_schema}".ep_in_species USING (spec_in_id) ' \
       'LEFT JOIN "{case_schema}".ep_mod_spec_factors_all msfa USING (cat_id, spec_in_id) ' \
       'WHERE msfa.cat_id IS NULL ORDER BY se.cat_id', case_schema=case_schema, conf_schema=conf_schema)                           
    
    # Calculate time dissaggregation factors
    tzone_out = timezone(timedelta(hours=itzone_out))
    time_start = ep_cfg.run_params.time_params.dt_init.replace(tzinfo=tzone_out)
    interval = ep_cfg.run_params.time_params.num_time_int
    timestep = timedelta(seconds=ep_cfg.run_params.time_params.timestep)
    log.debug('ep_calc_emiss_time_series:', time_start, timestep, interval, conf_schema, case_schema)
    cur.callproc('ep_calc_emiss_time_series', [time_start, timestep, interval, conf_schema, case_schema])
    log.sql_debug(ep_connection)
    ep_connection.commit()

    # check whether all categories in the domain have (some) time factors
    report.check.sql('\n Checking if all categories have assigned time factors.', 
       'Following categories do not have assigned any time factor/serie', 
       'SELECT DISTINCT se.cat_id, tf.cat_id FROM "{case_schema}".ep_sg_emissions se ' \
       'LEFT JOIN "{case_schema}".ep_time_factors tf using (cat_id) ' \
       'WHERE tf.cat_id IS NULL ORDER BY se.cat_id ', 
       case_schema=case_schema)   

    # Proceed with speciation profiles
    cur.callproc('ep_apply_spec_factors', [conf_schema, case_schema])
    log.sql_debug(ep_connection)
    ep_connection.commit()
    
    report.sum.sql('Control sums after chemical speciations - emission per specie and eset [gas unit Mmol/year, aerosol unit t/year (depends on gspro file)]', 
       'SELECT eset_name, name, sum(emiss)*31.5576 from "{case_schema}".ep_sg_emissions_spec '\
       'JOIN "{case_schema}".ep_sources_grid using (sg_id) '\
       'JOIN "{case_schema}".ep_out_species using (spec_id) '\
       'JOIN "{source_schema}".ep_in_sources using (source_id) '\
       'JOIN "{source_schema}".ep_emission_sets using (eset_id) '\
       'GROUP BY eset_name, name '\
       'ORDER BY eset_name', 
       case_schema=case_schema, source_schema=source_schema)

    # Time series will be calculated in output module

    cur.close()


def process_vertical_distributions():
    """
    Recalculates the vertical dsitribution factors from {sources_schema}.ep_vdistribution_factors to {case_schema}.ep_vdistribution_factors_out
    """
    if ep_cfg.run_params.vdistribution_params.apply_vdistribution == False:
        log.debug('Vertical distributions are not applied (cfg -> run_params -> vdistribution_params -> apply_vdistribution set to "no"!')
        return

    if "vdist" not in ep_rtcfg.keys() or ep_rtcfg["vdist"] != 1:
        log.debug('Vertical distributions are not applied as non of the eset-s had vdist defined')

    model_levels = ep_cfg.run_params.output_params.model_levels
    nz = ep_cfg.domain.nz
    maxlevel = model_levels[-1]
    num_levels_m = int(float(maxlevel)+1)
    model_levels_m = np.zeros((num_levels_m))
    # get the list of vdistribution
    source_schema =  ep_cfg.db_connection.source_schema
    case_schema = ep_cfg.db_connection.case_schema
    conf_schema = ep_cfg.db_connection.conf_schema

    with ep_connection.cursor() as cur:
        cur.execute('SELECT vdistribution_id FROM "{schema}"."ep_vdistribution_names"'
                        .format(schema=source_schema))
        vdists = [ i[0] for i in cur.fetchall() ]
        ep_connection.commit()

    with ep_connection.cursor() as cur:

        cur.execute('TRUNCATE TABLE "{}"."ep_vdistribution_factors_out" RESTART IDENTITY CASCADE'.format(case_schema))

        for vd in vdists:
            cur.execute('SELECT  cat_id, level, height, factor FROM "{schema}"."ep_vdistribution_factors" WHERE vdistribution_id = %s'
                        .format(schema=source_schema), (vd,))
            vdist_data = cur.fetchall()
            # get all categories for this vd
            categories = list(set([i[0] for i in vdist_data]))
            vd_factors = np.zeros((nz, len(categories)))
            for c in categories:
                model_levels_m = np.zeros((num_levels_m))
                vd_c = [e for e in vdist_data if e[0] == c]
                numlevels = len(vd_c)
                levels = [int(0)] + [int(i[2]) for i in vd_c]
                levels.sort()
                levels = np.array(levels)
                vd_c.sort(key=lambda a: a[2])
                for i,l in enumerate(levels[:-1]):
                    for j in range(levels[i],levels[i+1]):
                        model_levels_m[j] = vd_c[i][3]/(levels[i+1]-levels[i])

                factor_out = np.sum(model_levels_m[0:int(float(model_levels[0]))])
                if factor_out > 1e-6:
                    cur.execute('INSERT INTO "{schema}"."ep_vdistribution_factors_out" (vdistribution_id,cat_id,level,factor) VALUES (%s, %s, %s, %s)'.format(schema=case_schema), (vd,c,0,factor_out))

                for i in range(1,nz):
                    factor_out = np.sum(model_levels_m[int(float(model_levels[i-1])):int(float(model_levels[i]))])
                    if factor_out > 1e-6:
                        cur.execute('INSERT INTO "{schema}"."ep_vdistribution_factors_out" (vdistribution_id,cat_id,level,factor) VALUES (%s, %s, %s, %s)'.format(schema=case_schema), (vd,c,i,factor_out))
        # apply category hierarchy
        cur.callproc('ep_find_missing_vdistribution_factors', [conf_schema, source_schema, case_schema])


        ep_connection.commit()


def register_external_model(ext_mod_name):
    """
    Registers external model, returns id.
    """

    schema = ep_cfg.db_connection.case_schema
    with ep_connection.cursor() as cur:
        try:
            cur.execute('SELECT ext_mod_id FROM "{schema}"."ep_ext_models" WHERE ext_mod_name=%s'
                        .format(schema=schema), (ext_mod_name,))
            ext_mod_id = cur.fetchone()[0]
        except TypeError:
            cur.execute('INSERT INTO "{schema}"."ep_ext_models" (ext_mod_name) '
                        'VALUES (%s) RETURNING ext_mod_id'
                        .format(schema=schema), (ext_mod_name,))
            ext_mod_id = cur.fetchone()[0]
            log.fmt_debug('External model {} registered as ID {}', ext_mod_name, ext_mod_id)
        ep_connection.commit()

    return ext_mod_id


def init_external_models():
    """
    Collect external model configurations.

    Populate ep_rtcfg['external_models'] with the list of external model
    entries: dict(id=model_id, name=model_name, obj=model_module_object,
                  conf=model_config_file_object)
    """
    if 'external_models' in ep_rtcfg:
        return

    ep_rtcfg['external_models'] = []

    for m, c in zip(ep_cfg.run_params.models.models, ep_cfg.run_params.models.model_configs):
        modelconf = ConfigFile(c, ep_internal_path('models', m, 'configspec-{}.conf'.format(m))).values()
        mod_name = "models.{m}.ep_{m}".format(m=m)
        mod_obj = importlib.import_module(mod_name)
        ext_mod_id = register_external_model(m)
        ep_rtcfg['external_models'].append({'id': ext_mod_id, 'name': m,
                                            'obj': mod_obj, 'conf': modelconf})

        # Check if the module requires some meteorological inputs
        try:
            required_met = getattr(mod_obj, '_required_met')
            ep_rtcfg['required_met'] = ep_rtcfg['required_met'].union(required_met)
        except AttributeError:
            continue


def preproc_external_models():
    """
    Run all neccesary preprocessing for external models.
    This is done separately from run_models, as preprocessing is,
    for a particular case, done typically only once.
    """
    init_external_models()
    for mod in ep_rtcfg['external_models']:
        try:
            func_obj = getattr(mod['obj'], 'preproc')
        except AttributeError:
            log.fmt_error('WW: No preproc function defined in model {}, skipping...', mod['name'])
            continue

        try:
            log.fmt_info('Running model "{}" preprocessing', mod['name'])
            func_obj(mod['conf'])
        except:
            log.fmt_error('EE: Error preprocessing the model {}. Check if it is correctly configured and have all the neccesary inputs', mod['name'])
            raise


def run_external_models():
    """
    Run all external models defined in config (like MEGAN)
    """
    init_external_models()
    for mod in ep_rtcfg['external_models']:
        try:
            func_obj = getattr(mod['obj'], 'run')
        except AttributeError:
            log.fmt_error('WW: No run function defined in model {}, skipping...', mod['name'])
            continue

        try:
            log.fmt_info('Running model: {}', mod['name'])
            func_obj(mod['conf'], mod['id'])
        except:
            log.fmt_error('EE: Error running model {}. Check if it is correctly configured and have all the neccesary inputs', mod['name'])
            raise


def fill_out_species_table(con, mod_id, mech_ids=[]):
    """
    Fills the ep_out_species table with output species relevant for selected
    model and mechanisms.
    """

    case_schema = ep_cfg.db_connection.case_schema
    conf_schema = ep_cfg.db_connection.conf_schema

    cur = con.cursor()

    cur.execute('TRUNCATE TABLE "{}"."ep_out_species" RESTART IDENTITY CASCADE'.format(case_schema))

    for mech_id in mech_ids:
        cur.execute('INSERT INTO "{}"."ep_out_species" (spec_id, name) '
                    'SELECT spec_mod_id, name FROM "{}"."ep_mod_species" '
                    'WHERE model_id = %s AND mech_id = %s'.format(case_schema, conf_schema), (mod_id, mech_id))

    con.commit()
    cur.close()


def process_point_sources():
    source_schema = ep_cfg.db_connection.source_schema
    case_schema = ep_cfg.db_connection.case_schema
    conf_schema = ep_cfg.db_connection.conf_schema
    csrid = ep_cfg.projection_params.projection_srid
    ssrid = ep_cfg.projection_params.ep_projection_srid

    create_point_params_table(ep_connection, source_schema, case_schema, csrid, ssrid)
    fill_missing_point_parameters(ep_connection, source_schema, case_schema, conf_schema)


def create_point_params_table(con, source_schema, case_schema, csrid, ssrid):
    """ Fills ep_sources_point_table. Adds coordinates. """

    cur = con.cursor()

    cur.execute('TRUNCATE TABLE "{}"."ep_sources_point" RESTART IDENTITY CASCADE'.format(case_schema))

    cur.execute('INSERT INTO "{case_schema}"."ep_sources_point" (sg_id, xstk, ystk, lon, lat, height, diameter, temperature, velocity) '
                    'SELECT grid.sg_id, '
                    'ST_x(ST_Transform(geom.geom,{csrid})), '
                    'ST_y(ST_Transform(geom.geom,{csrid})), '
                    'ST_x(ST_Transform(geom.geom,{ssrid})), '
                    'ST_y(ST_Transform(geom.geom,{ssrid})), '
                    'src.height, src.diameter, src.temperature, src.velocity '
                    'FROM "{source_schema}".ep_in_sources_point AS src '
                    'JOIN "{case_schema}".ep_sources_grid AS grid USING (source_id) '
                    'JOIN "{source_schema}".ep_in_sources AS src_def USING (source_id) '
                    'JOIN "{source_schema}".ep_in_geometries AS geom USING (geom_id) ORDER BY grid.sg_id'
                .format(case_schema=case_schema, source_schema=source_schema, csrid=csrid, ssrid=ssrid))

    con.commit()
    cur.close()


def fill_missing_point_parameters(con, source_schema, case_schema, conf_schema):
    """ Fills in missing point sources parameters. For GNFR codes.
    Below are default parameters for different categories (hierarchy working). In case any parameter for any point source is missing it is filled based on this table. In case there is 0 value for limit, no specie is needed.
    Fill all four parameters, otherwise the recursive category filling may not work properly.
    We assume that that every source has only one category, otherwise the first one is used."""

    cur = con.cursor()

    # fill in the default parameters table
    cur.execute('TRUNCATE TABLE "{}"."ep_default_point_params" RESTART IDENTITY CASCADE'.format(case_schema))

    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
                        'VALUES (0, NULL, 0, 3, 373.15, 1, 10)'.format(case_schema=case_schema))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 1000000, spec_in_id, 31.69, 176.1, 368.4, 12.5, 6.8 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 1000000, spec_in_id, 0.03, 125.9, 401.2, 10.4, 4.1 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 1000000, spec_in_id, 0, 33.3, 470.7, 8.2, 1.4 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('SO2','SO2','SO2'))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 2000000, spec_in_id, 3.169, 111.2, 417.1, 10.7, 3.5 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 2000000, spec_in_id, 0.3169, 53.3, 458.7, 11.1, 2.0 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 2000000, spec_in_id, 0, 36.2, 412.0, 9.5, 1.9 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('SO2','SO2','SO2'))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 4000000, spec_in_id, 0.3169, 27.7, 344.0, 1.7, 3.1 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 4000000, spec_in_id, 0.000000317, 17.0, 301.3, 7.6, 1.5 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 4000000, spec_in_id, 0, 11.0, 307.7, 8.1, 0.9 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('CH4','CH4','CH4'))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 5000000, spec_in_id, 0.3169, 16.1, 298.8, 9.1, 1.3 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 5000000, spec_in_id, 0.003, 11.0, 307.7, 8.1, 0.9 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 5000000, spec_in_id, 0, 13.6, 347.2, 7.0, 0.9 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('NMVOC','NMVOC','NMVOC'))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 10000000, spec_in_id, 0.3169, 126.2, 398.1, 11.8, 2.2 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 10000000, spec_in_id, 0.03, 39.7, 402.3, 9.0, 1.6 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 10000000, spec_in_id, 0, 22.5, 391.2, 8.1, 1.2 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('PM10','PM10','PM10'))

    cur.callproc('ep_find_missing_point_parameters', [conf_schema, case_schema])
    log.sql_debug(con)

    cur.execute('SELECT * FROM "{case_schema}"."ep_default_point_params_all" order by cat_id, lim DESC'.format(case_schema=case_schema))
    rows = cur.fetchall()
    for row in rows:
        category = row[0]
        spec_id = row[1]
        limit = row[2]
        height = row[3]
        diameter = row[4]
        temperature = row[5]
        velocity = row[6]
        if limit > 0:
            cur.execute('DROP TABLE IF EXISTS new;'
                        'CREATE TEMP TABLE new AS '
                        'SELECT src.sg_id, '
                        ' coalesce(src.height, %s) AS height, '
                        ' coalesce(src.diameter, %s) AS diameter, '
                        ' coalesce(src.temperature, %s) AS temperature, '
                        ' coalesce(src.velocity, %s) AS velocity '
                        'FROM "{case_schema}".ep_sources_point AS src '
                        'JOIN "{case_schema}".ep_sources_grid AS grid USING (sg_id) '
                        'JOIN "{source_schema}".ep_in_emissions USING (source_id) '
                        'WHERE cat_id = %s AND spec_in_id = %s AND emission > %s AND NOT (src IS NOT NULL)'
                        .format(case_schema=case_schema, source_schema=source_schema),
                        (height, diameter, temperature, velocity, category, spec_id, limit))
        else:
            cur.execute('DROP TABLE IF EXISTS new;'
                        'CREATE TEMP TABLE new AS '
                        'SELECT DISTINCT src.sg_id, '
                        ' coalesce(src.height, %s) AS height, '
                        ' coalesce(src.diameter, %s) AS diameter, '
                        ' coalesce(src.temperature, %s) AS temperature, '
                        ' coalesce(src.velocity, %s) AS velocity '
                        'FROM "{case_schema}".ep_sources_point AS src '
                        'JOIN "{case_schema}".ep_sources_grid AS grid USING (sg_id) '
                        'JOIN "{source_schema}".ep_in_emissions USING (source_id) '
                        'WHERE cat_id = %s AND NOT (src IS NOT NULL)'
                        .format(case_schema=case_schema, source_schema=source_schema),
                        (height, diameter, temperature, velocity, category))
        cur.execute('UPDATE "{case_schema}".ep_sources_point orig '
                    'SET '
                    ' height = new.height, '
                    ' diameter = new.diameter, '
                    ' temperature = new.temperature, '
                    ' velocity = new.velocity '
                    'FROM new '
                    'WHERE orig.sg_id = new.sg_id'
                    .format(case_schema=case_schema))
    con.commit()
    cur.close()


def fill_missing_point_parameters_SNAP(con, source_schema, case_schema, conf_schema):
    """ Fills in missing point sources parameters. OLD version for SNAP.
    Below are default parameters for different categories (hierarchy working). In case any parameter for any point source is missing it is filled based on this     table. In case there is 0 value for limit, no specie is needed.
    Fill all four parameters, otherwise the recursive category filling may not work properly.
    We assume that that every source has only one category, otherwise the first one is used."""

    cur = con.cursor()

    # fill in the default parameters table
    cur.execute('TRUNCATE TABLE "{}"."ep_default_point_params" RESTART IDENTITY CASCADE'.format(case_schema))

    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
                        'VALUES (0, NULL, 0, 3, 373.15, 1, 10)'.format(case_schema=case_schema))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 1000000, spec_in_id, 31.69, 176.1, 368.4, 12.5, 6.8 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 1000000, spec_in_id, 0.03, 125.9, 401.2, 10.4, 4.1 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 1000000, spec_in_id, 0, 33.3, 470.7, 8.2, 1.4 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('SO2','SO2','SO2'))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 3000000, spec_in_id, 3.169, 111.2, 417.1, 10.7, 3.5 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 3000000, spec_in_id, 0.3169, 53.3, 458.7, 11.1, 2.0 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 3000000, spec_in_id, 0, 36.2, 412.0, 9.5, 1.9 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('SO2','SO2','SO2'))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
			'SELECT 4000000, spec_in_id, 3.169, 39.0, 365.0, 10.4, 2.3 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 4000000, spec_in_id, 0.03, 30.6, 396.4, 11.5, 1.5 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 4000000, spec_in_id, 0, 22.6, 435.8, 8.9, 1.3 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('CO','CO','CO'))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 5000000, spec_in_id, 0.3169, 27.7, 344.0, 1.7, 3.1 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 5000000, spec_in_id, 0.000000317, 17.0, 301.3, 7.6, 1.5 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 5000000, spec_in_id, 0, 11.0, 307.7, 8.1, 0.9 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('CH4','CH4','CH4'))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 6000000, spec_in_id, 0.3169, 16.1, 298.8, 9.1, 1.3 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 6000000, spec_in_id, 0.003, 11.0, 307.7, 8.1, 0.9 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 6000000, spec_in_id, 0, 13.6, 347.2, 7.0, 0.9 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('NMVOC','NMVOC','NMVOC'))
    cur.execute('INSERT INTO "{case_schema}"."ep_default_point_params" (cat_id, spec_in_id, lim, height, temperature, velocity, diameter) '
    			'SELECT 9000000, spec_in_id, 0.3169, 126.2, 398.1, 11.8, 2.2 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 9000000, spec_in_id, 0.03, 39.7, 402.3, 9.0, 1.6 FROM "{conf_schema}"."ep_in_species" WHERE name = %s UNION '
                        'SELECT 9000000, spec_in_id, 0, 22.5, 391.2, 8.1, 1.2 FROM "{conf_schema}"."ep_in_species" WHERE name = %s'
			.format(case_schema=case_schema, conf_schema=conf_schema), ('PM10','PM10','PM10'))

    cur.callproc('ep_find_missing_point_parameters', [conf_schema, case_schema])
    log.sql_debug(con)

    cur.execute('SELECT * FROM "{case_schema}"."ep_default_point_params_all" order by cat_id, lim DESC'.format(case_schema=case_schema))
    rows = cur.fetchall()
    for row in rows:
        category = row[0]
        spec_id = row[1]
        limit = row[2]
        height = row[3]
        diameter = row[4]
        temperature = row[5]
        velocity = row[6]
        if limit > 0:
            cur.execute('DROP TABLE IF EXISTS new;'
                        'CREATE TEMP TABLE new AS '
                        'SELECT src.sg_id, '
                        ' coalesce(src.height, %s) AS height, '
                        ' coalesce(src.diameter, %s) AS diameter, '
                        ' coalesce(src.temperature, %s) AS temperature, '
                        ' coalesce(src.velocity, %s) AS velocity '
                        'FROM "{case_schema}".ep_sources_point AS src '
                        'JOIN "{case_schema}".ep_sources_grid AS grid USING (sg_id) '
                        'JOIN "{source_schema}".ep_in_emissions USING (source_id) '
                        'WHERE cat_id = %s AND spec_in_id = %s AND emission > %s AND NOT (src IS NOT NULL)'
                        .format(case_schema=case_schema, source_schema=source_schema),
                        (height, diameter, temperature, velocity, category, spec_id, limit))
        else:
            cur.execute('DROP TABLE IF EXISTS new;'
                        'CREATE TEMP TABLE new AS '
                        'SELECT DISTINCT src.sg_id, '
                        ' coalesce(src.height, %s) AS height, '
                        ' coalesce(src.diameter, %s) AS diameter, '
                        ' coalesce(src.temperature, %s) AS temperature, '
                        ' coalesce(src.velocity, %s) AS velocity '
                        'FROM "{case_schema}".ep_sources_point AS src '
                        'JOIN "{case_schema}".ep_sources_grid AS grid USING (sg_id) '
                        'JOIN "{source_schema}".ep_in_emissions USING (source_id) '
                        'WHERE cat_id = %s AND NOT (src IS NOT NULL)'
                        .format(case_schema=case_schema, source_schema=source_schema),
                        (height, diameter, temperature, velocity, category))
        cur.execute('UPDATE "{case_schema}".ep_sources_point orig '
                    'SET '
                    ' height = new.height, '
                    ' diameter = new.diameter, '
                    ' temperature = new.temperature, '
                    ' velocity = new.velocity '
                    'FROM new '
                    'WHERE orig.sg_id = new.sg_id'
                    .format(case_schema=case_schema))
    con.commit()
    cur.close()
