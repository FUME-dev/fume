#!/usr/bin/env python3

from os import path
import getpass
import psycopg2
import datetime
import configobj
from enum import Enum
from lib.ep_config import ep_cfg
from lib.ep_geo_tools import create_projection,  get_projection_domain_params
from osgeo import osr
from lib.debug import ExecTimer

__all__ = ['ep_connection', 'ep_getconnection', 'ep_rtcfg']

# This module defines basic common function and procedures for emission processor

# returns shared global connection into database
# if it has not been established yet
# it checks connection info and creates a new connection
def ep_getconnection(database, connection_info=None):
    global ep_connection, ep_connection_info
    if 'ep_connection' not in globals() or ep_connection is None or \
            ep_connection.status != 0:
        ep_debug('Connection does not exist - creating')

        if 'ep_connection_info' not in globals():
            ep_connection_info = {}

        try:
            ep_connection_info.update(connection_info)
        except TypeError:
            pass

        ep_connection_info.setdefault('host', 'localhost')
        ep_connection_info.setdefault('port', 5432)
        ep_connection_info.setdefault('user', getpass.getuser())
        if 'password' not in ep_connection_info:
            ep_connection_info['password'] = getpass.getpass()

        ep_connection = psycopg2.connect(database=database,
                                         host=ep_connection_info['host'],
                                         port=ep_connection_info['port'],
                                         user=ep_connection_info['user'],
                                         password=ep_connection_info['password'])
        ep_connection.set_client_encoding('UTF8')
    else:
        ep_debug('Connection exists.')
        ep_connection.status

    return ep_connection


def ep_create_schema(schema, init_file=None, srid=None):
    """
    Create new schema and register init_file
    """
    cur = ep_connection.cursor()

    # create a new schema for case
    if ep_cfg.scratch and schema not in ep_rtcfg['db']['schemas_initialized']:
        ep_debug('DROP SCHEMA IF EXISTS "{}" CASCADE'.format(schema))
        cur.execute('DROP SCHEMA IF EXISTS "{}" CASCADE'.format(schema))

    sqltext = 'CREATE SCHEMA IF NOT EXISTS "{}"'.format(schema)
    ep_debug(sqltext)
    cur.execute(sqltext)

    # grant schema privileges
    sqltext = 'GRANT ALL ON SCHEMA "{}" TO "{}"'.format(schema,
                                                        ep_cfg.db_connection.user)
    cur.execute(sqltext)

    # create schema tables
    ep_debug('Init_file = ', init_file)
    if init_file is not None:
        with open(ep_internal_path('sql', init_file), "r") as schema_sql:
            if srid is None:
                sqltext = schema_sql.read().format(**ep_cfg.db_connection.all_schemas)
            else:
                sqltext = schema_sql.read().format(srid=srid, **ep_cfg.db_connection.all_schemas)
            print('Execute SQL command: ', sqltext)
            cur.execute(sqltext)
            ep_connection.commit()

    ep_rtcfg['db']['schemas_initialized'].append(schema)


def ep_create_srid(p4s,dtype='case'):

    #proj, p_alp, p_bet, p_gam, XCENT, YCENT = ep_projection_params()

    if dtype == 'case':
        ep_debug('Create srid for domain.')
        srid = False
#        if ep_cfg.projection_params.projection_proj4 == '':
#            proj  = ep_rtcfg['projection_params']['proj']
#            p_alp = ep_rtcfg['projection_params']['p_alp']
#            p_bet = ep_rtcfg['projection_params']['p_bet']
#            p_gam = ep_rtcfg['projection_params']['p_gam']
#            XCENT = ep_rtcfg['projection_params']['lon_central']
#            YCENT = ep_rtcfg['projection_params']['lat_central']
#            proj_string = create_projection(proj, XCENT, YCENT, p_alp, p_bet,
#                                            p_gam).srs
#        else:
#            proj_string = ep_cfg.projection_params.projection_proj4
        proj_string = p4s #ep_rtcfg['projection_params']['proj4string']

    elif dtype == 'met':
        ep_debug('Create srid for met domain')
        inputfiles = ep_cfg.input_params.met.met_paths
        mettype = ep_cfg.input_params.met.met_type

        nx_met, ny_met, nz_met, delx_met, dely_met, xorg_met, yorg_met, projection_met, XCENT_met, YCENT_met, p_alp_met, p_bet_met, p_gam_met =  get_projection_domain_params(inputfiles[0], ftype=mettype)

        proj_string = create_projection(projection_met, XCENT_met, YCENT_met, p_alp_met, p_bet_met, p_gam_met).srs

    else:
        print('EE: dtype have to be "case" or "met"')
        raise ValueError

    srs = osr.SpatialReference()
    srs.ImportFromProj4(proj_string)
    wkt = srs.ExportToWkt()
    cur = ep_connection.cursor()
    cur.execute('INSERT INTO spatial_ref_sys '
                '(srid, auth_name, srtext, proj4text) VALUES '
                "(nextval('spatial_ref_sys_srid_seq'), %s, %s, %s) "
                'RETURNING srid',
                ['EP', wkt, proj_string])
    srid = cur.fetchone()[0]
    cur.execute('UPDATE spatial_ref_sys SET auth_srid = %s WHERE '
                'srid = %s',[srid, srid])

    ep_cfg.projection_params.projection_srid = srid
    ep_debug('ep_create_srid: new srid = {}'.format(srid))
    return srid


def ep_create_grid(schema=ep_cfg.db_connection.conf_schema, grid_name=ep_cfg.domain.grid_name, \
                   nx=ep_cfg.domain.nx, ny=ep_cfg.domain.ny, delx=ep_cfg.domain.delx, dely=ep_cfg.domain.dely, \
                   xorg=ep_cfg.domain.xorg, yorg=ep_cfg.domain.yorg, srid=ep_cfg.projection_params.projection_srid ):

    srid = ep_cfg.projection_params.projection_srid # !!!!! set srid default in parameters DOES NOT work as the value ep_cfg.projection_params.projection_srid has changed in ep_projection_params !!!
    ep_debug('Create grid: ', schema, grid_name, srid)
    cur = ep_connection.cursor()
    try:
        cur.callproc('ep_create_grid', [schema, grid_name, nx, ny, delx, dely, xorg, yorg, srid])
        cur.close()
        ep_connection.commit()
    except Exception as e:
        print("create_grid: unable to create grid table {}. \n Error: {}".format(grid_name, e))
        ep_connection.rollback()
        raise e


def ep_debug(*args):
    if ep_cfg.debug:
        print(*args)


def ep_dates_times():
    """ generates list of datetime objects that correspond to congfigured starttime,deltat and number of timesteps.
    To be used in ep_met, ep_write_output etc...
    code to use: from lib.libutil import ep_dates_times
    do_whatever_with(ep_dates_times())
    """
    if 'datestimes' not in ep_rtcfg['run']:
    #itzone_out    = ep_cfg.run_params.time_params.itzone_out
        bdatetime = ep_cfg.run_params.time_params.dt_init
        ntimeint  = ep_cfg.run_params.time_params.num_time_int
        tdelta    = ep_cfg.run_params.time_params.timestep

        tzone     = ep_cfg.run_params.time_params.itzone_out
        tz = datetime.timezone(datetime.timedelta(hours=tzone))
        bdatetime = bdatetime.replace(tzinfo=tz) # make naive datetime to a timezone aware one

        actual_date = bdatetime
        timedelta = datetime.timedelta(seconds=tdelta)
    #global ep_datestimes
        ep_datestimes = []
        ep_datestimes.append(actual_date)
        for i in range(ntimeint-1):
            actual_date = actual_date + timedelta
            ep_datestimes.append(actual_date)

        ep_rtcfg['run']['datestimes'] = ep_datestimes
    else:
        ep_datestimes = ep_rtcfg['run']['datestimes']
         
    return(ep_datestimes)


def ep_projection_params():

    srid       = ep_cfg.projection_params.projection_srid
    proj4str   = ep_cfg.projection_params.projection_proj4

    if srid != -1 or proj4str != '':
        from lib.ep_geo_tools import projection_parameters_from_srid, projection_parameters_from_proj4
        import pyproj
        if srid != -1:
#            print('Parsing projection from srid:',srid)
            proj, XCENT, YCENT, p_alp, p_bet, p_gam = projection_parameters_from_srid(srid)
            proj4str = pyproj.Proj(init="epsg:{}".format(ep_cfg.projection_params.projection_srid)).srs
        else:
#            print('Using projection from proj4 string',proj4str)
            proj, XCENT, YCENT, p_alp, p_bet, p_gam = projection_parameters_from_proj4(proj4str)
    else:
        from lib.ep_geo_tools import create_projection
        proj       = ep_cfg.projection_params.projection
        p_alp      = ep_cfg.projection_params.p_alp
        p_bet      = ep_cfg.projection_params.p_bet
        p_gam      = ep_cfg.projection_params.p_gam
        XCENT      = ep_cfg.projection_params.lon_central
        YCENT      = ep_cfg.projection_params.lat_central
        proj4str =  create_projection(proj, XCENT, YCENT, p_alp, p_bet, p_gam).srs

    # check srid
    if srid == -1:
        cur = ep_connection.cursor()
        try:
            cur.execute('SELECT srid FROM spatial_ref_sys WHERE trim(proj4text)=trim(%s)', [proj4str])
            srid = cur.fetchone()[0]
        except TypeError:
            srid = ep_create_srid(proj4str)
        ep_cfg.projection_params.projection_srid = srid
        ep_debug('ep_projection_params: attached to srid = {}'.format(srid))

    # saving projection params to runtime Config
    ep_rtcfg['projection_params'] = {}
    ep_rtcfg['projection_params']['srid'] = srid
    ep_rtcfg['projection_params']['proj'] = proj
    ep_rtcfg['projection_params']['p_alp'] = p_alp
    ep_rtcfg['projection_params']['p_bet'] = p_bet
    ep_rtcfg['projection_params']['p_gam'] = p_gam
    ep_rtcfg['projection_params']['lon_central'] = XCENT
    ep_rtcfg['projection_params']['lat_central'] = YCENT
    ep_rtcfg['projection_params']['proj4string'] = proj4str # saving projection string to runtime Config
    return(proj, p_alp, p_bet, p_gam, XCENT, YCENT)


def ep_model_id(schema=None, model_name=None, model_version=None):
    """ Based on aq model name and version returns model_id from ep_aq_model table
    and stores it into ep_rtcfg['model_id']. """

    try:
        ep_rtcfg['model_id']
    except KeyError:
        if schema is None:
            schema = ep_cfg.db_connection.conf_schema
        if model_name is None:
            model_name = ep_cfg.run_params.output_params.model
        if model_version is None:
            model_version = ep_cfg.run_params.output_params.model_version

        with ep_connection.cursor() as cur:
            sql = cur.mogrify('SELECT model_id FROM "{}"."ep_aq_models" '
                              'WHERE name = %s AND version = %s'.format(schema), (model_name, model_version))
            ep_debug(sql)
            cur.execute(sql)
            ep_rtcfg['model_id'] = cur.fetchone()[0]

    return ep_rtcfg['model_id']


def ep_mechanism_ids(schema=None, mechanism_names=None):
    """ Based on mechanism name returns mech_id from ep_mechanisms table
    and stores it into ep_rtcfg['mechanism_id']. """

    try:
        ep_rtcfg['mechanism_id_all']
    except KeyError:
        if schema is None:
            schema = ep_cfg.db_connection.conf_schema
        if mechanism_names is None:
            mechanism_names = ep_cfg.run_params.speciation_params.chem_mechanisms

        types = []
        ep_rtcfg['mechanism_ids'] = []
        with ep_connection.cursor() as cur:
            for name in mechanism_names:
                cur.execute('SELECT mech_id, type FROM "{}"."ep_mechanisms" WHERE name = %s'.format(schema), (name, ))
                try:
                    id, mech_type = cur.fetchone()
                except TypeError:
                    raise TypeError ('Mechanism name {} not found in mechanisms table.'.format(name))
                if mech_type in types:
                    raise ValueError('More mechanisms of one type selected.')
                ep_rtcfg['mechanism_ids'].append(id)
                types.append(mech_type)

    return ep_rtcfg['mechanism_ids']


def ep_ResultIter(cursor, arraysize=1000):
#   An iterator that uses fetchmany to keep memory usage down
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


def exec_timer(name):
    return ExecTimer(name, ep_cfg.debug)


def ep_internal_path(*paths):
    return path.join(ep_rtcfg['execdir'], *paths)


# initialize null ep_connection - use function ep_getconnection for real connection initialization
ep_connection = None

# initialize empty ConfigObj for runtime stuff storage
ep_rtcfg = configobj.ConfigObj()
ep_rtcfg['db'] = dict()
ep_rtcfg['db']['schemas_initialized'] = list()
ep_rtcfg['run'] = dict()
ep_rtcfg['required_met'] = set()  # meteorological fields needed
ep_rtcfg['required_met_in_db'] = set()  # only those met fields which are needed in db (maybe will be never used)
