from datetime import timedelta, timezone
import importlib
from lib.ep_libutil import ep_create_grid, ep_create_schema, ep_projection_params, \
     ep_connection, ep_debug, ep_rtcfg, ep_model_id, ep_mechanism_ids, \
     ep_dates_times, ep_internal_path
from lib.ep_config import ep_cfg, ConfigFile


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

    # create specie table for case
    fill_out_species_table(ep_connection, ep_rtcfg['model_id'], ep_rtcfg['mechanism_ids'])

    ep_connection.commit()
    cur.close()


def ep_create_timezones():
    """
    This procedure imports TZ from raw table into case
    only needed timezones are imported/created
    """

    cur = ep_connection.cursor()
    # function is implemented in pgplsql on server from performance reasons
    cur.execute('select ep_case_timezones(%s,%s,%s,%s,%s,%s,%s)', ( \
        ep_cfg.db_connection.conf_schema, ep_cfg.domain.grid_name, \
        ep_cfg.db_connection.static_schema, 'ep_tz_world',
        ep_cfg.db_connection.case_schema, 'ep_timezones', ep_cfg.projection_params.projection_srid))


def ep_create_grid_tz():
    case_schema = ep_cfg.db_connection.case_schema
    conf_schema = ep_cfg.db_connection.conf_schema
    grid_name = ep_cfg.domain.grid_name
    ep_debug('Create grid with timezones: ', case_schema, 'ep_grid_tz', conf_schema, grid_name, 'ep_timezones', ep_cfg.projection_params.projection_srid)
    cur = ep_connection.cursor()
    try:
        cur.callproc('ep_create_grid_tz', [case_schema, 'ep_grid_tz', conf_schema, grid_name, 'ep_timezones', ep_cfg.projection_params.projection_srid])
        cur.close()
        ep_connection.commit()
    except Exception as e:
        print("create_grid_tz: unable to create ep_grid_tz table {}. \n Error: {}".format('ep_grid_tz', e))
        ep_connection.rollback()
        raise e


def prepare_conf():
    """
    This procedure does necessary steps to prepare the case
    to be precesses. It actually reads/creates projection from params
    and creates original grid in conf schema if required
    by configuration. More action might be added in future.
    """
    ep_projection_params()
    ep_create_grid()
    ep_dates_times()
    ep_debug(ep_rtcfg['projection_params'])


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
    
    - import meteorology data
    - calculate speciation splits & apply
    - calculate time dissaggregation factors
    """
    itzone_out = ep_cfg.run_params.time_params.itzone_out
    conf_schema = ep_cfg.db_connection.conf_schema
    case_schema = ep_cfg.db_connection.case_schema

    ep_model_id()  # save model_id to runtime ConfigObject
    ep_mechanism_ids()  # save mechanism ids to runtime ConfigObject

    cur = ep_connection.cursor()
    # speciation
    cur.callproc('ep_speciation_splits', [ep_rtcfg['model_id'],
                                          ep_rtcfg['mechanism_ids'],
                                          conf_schema, case_schema])

    # Calculate time dissaggregation factors
    tzone_out = timezone(timedelta(hours=itzone_out))
    time_start = ep_cfg.run_params.time_params.dt_init.replace(tzinfo=tzone_out)
    interval = ep_cfg.run_params.time_params.num_time_int
    timestep = timedelta(seconds=ep_cfg.run_params.time_params.timestep)
    cur.callproc('ep_calc_emiss_time_series', [time_start, timestep, interval,
                                               itzone_out, conf_schema, case_schema])
    ep_connection.commit()

    # Proceed with speciation profiles
    cur.callproc('ep_apply_spec_factors', [conf_schema, case_schema])
    ep_connection.commit()

    # Time series will be calculated in output module

    cur.close()


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
            ep_debug('External model {} registered as ID {}'.format(ext_mod_name, ext_mod_id))
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
            continue

        try:
            ep_debug('Running model "{}" preprocessing'.format(mod['name']))
            func_obj(mod['conf'])
        except:
            print('EE: Error prerocessing the model {}. Check if it is correctly configured and have all the neccesary inputs'.format(mod['name']))
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
            print('WW: No run_{m} function defined in model {m}, skipping...')
            continue

        try:
            ep_debug('Running model: {}'.format(mod['name']))
            func_obj(mod['conf'], mod['id'])
        except:
            print('EE: Error running model {}. Check if it is correctly configured and have all the neccesary inputs'.format(mod['name']))
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
    """ Fills in missing point sources parameters.
    At this moment this is very stupid function, strictly tied to our case of filling parameters for REZZO, TNO and ATEM.
    Recursive filling of categories missing.
    We assume that that every source has only one category, otherwise the first one is used."""

    parameters = dict()
    parameters[1000000] = ('SO2', {31.69: {'height': 176.1, 'temp': 368.4, 'vel': 12.5, 'diam': 6.8},
                                    0.03: {'height': 125.9, 'temp': 401.2, 'vel': 10.4, 'diam': 4.1},
                                       0: {'height':  33.3, 'temp': 470.7, 'vel':  8.2, 'diam': 1.4}})
    parameters[3000000] = ('SO2', {3.169: {'height': 111.2, 'temp': 417.1, 'vel': 10.7, 'diam': 3.5},
                                  0.3169: {'height':  53.3, 'temp': 458.7, 'vel': 11.1, 'diam': 2.0},
                                       0: {'height':  36.2, 'temp': 412.0, 'vel':  9.5, 'diam': 1.9}})
    parameters[3400000] = parameters[3000000]
    parameters[4000000] = ('CO',  {3.169: {'height':  39.0, 'temp': 365.0, 'vel': 10.4, 'diam': 2.3},
                                    0.03: {'height':  30.6, 'temp': 396.4, 'vel': 11.5, 'diam': 1.5},
                                       0: {'height':  22.6, 'temp': 435.8, 'vel':  8.9, 'diam': 1.3}})
    parameters[5000000] = ('CH4', {0.3169: {'height':  27.7, 'temp': 344.0, 'vel':  1.7, 'diam': 3.1},
                              0.000000317: {'height':  17.0, 'temp': 301.3, 'vel':  7.6, 'diam': 1.5},
                                      0: {'height':  11.0, 'temp': 307.7, 'vel':  8.1, 'diam': 0.9}})
    parameters[6000000] = ('NMVOC',{0.3169: {'height':  16.1, 'temp': 298.8, 'vel':  9.1, 'diam': 1.3},
                                     0.003: {'height':  11.0, 'temp': 307.7, 'vel':  8.1, 'diam': 0.9},
                                         0: {'height':  13.6, 'temp': 347.2, 'vel':  7.0, 'diam': 0.9}})
    parameters[9000000] = ('PM10', {0.3169: {'height': 126.2, 'temp': 398.1, 'vel': 11.8, 'diam': 2.2},
                                      0.03: {'height':  39.7, 'temp': 402.3, 'vel':  9.0, 'diam': 1.6},
                                         0: {'height':  22.5, 'temp': 391.2, 'vel':  8.1, 'diam': 1.2}})

    cur = con.cursor()

    for category, value in parameters.items():
        specie = value[0]
        cur.execute('SELECT spec_in_id FROM "{conf_schema}".ep_in_species WHERE name=%s'.
                    format(conf_schema=conf_schema), (specie,))
        spec_id = cur.fetchone()[0]
        for limit in sorted(value[1], reverse=True):
            height = value[1][limit]['height']
            diameter = value[1][limit]['diam']
            temperature = value[1][limit]['temp']
            velocity = value[1][limit]['vel']
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

    # the rest are ATEM sources, lets just fill something
    height = 3
    diameter = 10
    temperature = 373.15
    velocity = 1
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
                'WHERE NOT (src IS NOT NULL)'
                .format(case_schema=case_schema, source_schema=source_schema),
                (height, diameter, temperature, velocity))
    cur.execute('UPDATE "{case_schema}".ep_sources_point orig '
                'SET '
                ' height = new.height, '
                ' diameter = new.diameter, '
                ' temperature = new.temperature, '
                ' velocity = new.velocity '
                'FROM new '
                'WHERE orig.sg_id = new.sg_id'
                .format(case_schema=case_schema))
    cur.execute('DROP TABLE IF EXISTS new;'
                'CREATE TEMP TABLE new AS '
                'SELECT src.sg_id, '
                ' coalesce(src.height, %s) AS height, '
                ' coalesce(src.diameter, %s) AS diameter, '
                ' coalesce(src.temperature, %s) AS temperature, '
                ' coalesce(src.velocity, %s) AS velocity '
                'FROM "{case_schema}".ep_sources_point AS src '
                'JOIN "{case_schema}".ep_sources_grid AS grid USING (sg_id) '
                'JOIN "{source_schema}".ep_in_activity_data USING (source_id) '
                'WHERE NOT (src IS NOT NULL)'
                .format(case_schema=case_schema, source_schema=source_schema),
                (height, diameter, temperature, velocity))
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
