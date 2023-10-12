"""
Description: helper functions for processing the transformatin queue

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

from collections import defaultdict
from psycopg2 import DataError, ProgrammingError
from lib.ep_config import ep_cfg, ConfigFile
from lib.ep_libutil import ep_connection, ep_rtcfg, ep_internal_path

from lib.db import Relation, get_relation, get_geometry_relation
from transformations.builtin import TransformationQueue,\
                                    SRIDTransformation, \
                                    AreaTransformation, \
                                    IntersectTransformation,\
                                    SurrogateTransformation,\
                                    LimitToGridTransformation,\
                                    ToGridTransformation, \
                                    SourcesToGridTransformation,\
                                    MaskTransformation,\
                                    SourceFilterTransformation, \
                                    ScenarioTransformation, \
                                    LevelFilterTransformation
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)


filter_columns = {
        'inventory': 'like',
        'eset': 'like',
        'source_type': 'eq'
        }


def get_filter(k, v):
    op = filter_columns[k]
    if v.strip()[0] == '!':
        op = 'n'+op
        v = v[1:]

    return ('{key}__{op}'.format(key=k, op=op), v)


builtin_transformations_conf = {
        'to_grid': defaultdict(lambda: False, {'type': 'to_grid', 'normalize': True, 'mandatory': True}),
        'scenarios': defaultdict(lambda: False, {'type': 'scenarios'}),
        'level_filter': defaultdict(lambda: False, {'type': 'level_filter'}),
}


def get_builtin_transformation(name):
    class TransformationConfig():
        pass

    if name in builtin_transformations_conf:
        trans = TransformationConfig()
        for k, v in builtin_transformations_conf[name].items():
            setattr(trans, k, v)
    else:
        trans = None

    return trans


def create_default_transformation_queue(queue_id=1, name='default_transformation_chain'):
    case_schema = ep_cfg.db_connection.case_schema
    source_schema = ep_cfg.db_connection.source_schema

    transformation_queue = TransformationQueue(queue_id=queue_id,
                                        name=name,
                                        inrel=Relation(schema=source_schema,
                                                        name='ep_in_geometries',
                                                        fields=['geom_id']),
                                        outrel=Relation(schema=case_schema,
                                                        name='ep_sources_grid',
                                                        pk='sg_id',
                                                        geom_field='geom'),
                                        outsrid=ep_cfg.projection_params.projection_srid)
    transformation_queue.db_connection = ep_connection
    transformation_queue.schema = case_schema
    transformation_queue.cfg = ep_cfg
    transformation_queue.rt_cfg = ep_rtcfg
    transformation_queue.append(SourcesToGridTransformation(srcrel=Relation(schema=source_schema,
                                                                            name='ep_in_sources'),
                                                            case_schema=case_schema))
    return transformation_queue


def prepare():
    global transconf
    transconf = None
    try:
        transconf = ConfigFile(ep_cfg.transformations.source,
                               ep_internal_path('transformations', 'configspec-transformations.conf')).values()
        if ep_cfg.transformations.chains is None:
            chains = [f for f in transconf.transformations if f.mandatory]
        else:
            chains = ep_cfg.transformations.chains
    except AttributeError:
        chains = []

    ep_rtcfg['transformation_queues'] = []
    if len(list(chains)) == 0:
        transformation_queue = create_default_transformation_queue()
        transformation_queue.insert(ToGridTransformation(), index=0)
        ep_rtcfg['transformation_queues'].append(transformation_queue)
    else:
        for idx, chain in enumerate(chains, start=1):
            # build list of mandatory transformation classes
            mandatory_transformations = []
            for transname, trans in builtin_transformations_conf.items():
                if trans['mandatory']:
                    trans = get_builtin_transformation(transname)
                    mandatory_transformations.append(create_transformation(trans))

            # create and build new transformation queue
            transformation_queue = create_default_transformation_queue(queue_id=idx, name=chain)
            ep_rtcfg['transformation_queues'].append(transformation_queue)
            for tidx, transname in enumerate(getattr(chains, chain)):
                log.debug('transname: ', transname)
                transformation = create_transformation(transname)
                transformation_queue.insert(transformation, index=tidx)
                for t in mandatory_transformations:
                    if isinstance(transformation, type(t)):
                        mandatory_transformations.remove(t)

            # ensure all mandatory transformations are included at the end of the chain
            for t in mandatory_transformations:
                transformation_queue.insert(t, -1)


def create_transformation(trans):
    log.debug('create_transformation:',trans)
    filters = []
    transformation = None
    if isinstance(trans, str):
        transname = trans
        if ':' in trans:
            transname, transparams = trans.split(':', maxsplit=1)

        if hasattr(transconf.transformations, transname):
            trans = getattr(transconf.transformations, transname)
        else:
            trans = get_builtin_transformation(transname)

    if trans.type == 'source_filter':
        if hasattr(trans, 'set') and trans.set is not None:
            filter_key, filter_val = get_filter('eset', trans.set)
            filters.append([filter_key, filter_val])
        elif hasattr(trans, 'inventory') and trans.inventory is not None:
            for inv_spec in trans.inventory:
                filter_key, filter_val = get_filter('inventory', inv_spec)
                filters.append([filter_key, filter_val])

        if hasattr(trans, 'filter_by') and hasattr(trans, 'filter_value') and trans.filter_by is not None:
            filter_key, filter_val = get_filter(trans.filter_by, trans.filter_value)
            filters.append([filter_key, filter_val])

        outsrid = None
        if hasattr(trans, 'outsrid') and trans.outsrid is not None:
            outsrid = trans.outsrid

        transformation = SourceFilterTransformation(filters=filters, outsrid=outsrid,
                                                 mask_to_grid=trans.mask_to_grid,
                                                 transform_srid=trans.transform_srid)

    elif trans.type == 'area_multiplicator':
        transformation = AreaTransformation()

    elif trans.type == 'limit_to_grid':
        transformation = LimitToGridTransformation()

    elif trans.type == 'srid_transform':
        transformation = SRIDTransformation()

    elif trans.type == 'mask':
        log.debug('Mask transaction:', trans.mask_file, trans.mask_filters)
        maskrel = get_geometry_relation(trans.mask_file, trans.mask_filters)
        transformation = MaskTransformation(maskrel=maskrel, mask_type=trans.mask_type)

    elif trans.type == 'to_grid':
        if hasattr(trans, 'normalize'):
            normalize = trans.normalize
        else:
            normalize = None
        if hasattr(trans, 'method'):
            method = trans.method
        else:
            method = 'Area'
        transformation = ToGridTransformation(normalize=normalize, method=method)

    elif trans.type == 'intersect' and hasattr(trans, 'intersect') and trans.intersect is not None:
        # TODO this is wrong, it remainded from time of call to_grid as intersect, needs to be generalized here!!!
        case_schema = ep_cfg.db_connection.case_schema
        inrel2 = Relation(schema=case_schema, name=trans.intersect, fields=['grid_id'])
        try:
            outrel = get_relation(trans.target)
            outrel.coef = outrel.fields[0]  # FIXME
        except AttributeError:
            outrel = None
        transformation = IntersectTransformation(inrel2=inrel2, outrel=outrel)

    elif trans.type == 'surrogate' and hasattr(trans, 'surrogate_set') and trans.surrogate_set is not None:
        if hasattr(trans, 'surrogate_type'):
            transformation = SurrogateTransformation(surset=trans.surrogate_set, surtype=trans.surrogate_type)
        else:
            transformation = SurrogateTransformation(surset=trans.surrogate_set)

    elif trans.type == 'scenarios':
        transformation = ScenarioTransformation(transparams)
    elif trans.type == 'level_filter':
        transformation = LevelFilterTransformation(transparams)

    return transformation


def run():
    cur = ep_connection.cursor()
    log.debug('*** Initialize transformation queue...')
    cur.execute('SELECT ep_init_transformation_queue(%s)', [ep_cfg.db_connection.case_schema])
    try:
        for q in ep_rtcfg['transformation_queues']:
            log.debug('Run transformation queue #', q.queue_id)
            q.process()
    except (DataError, ProgrammingError):
        log.sql_debug(ep_connection)
        raise

    log.debug('*** Finalize transformation queue...')
    ftq_placeholders = ['%s']*6
    ftq_args = [ep_cfg.db_connection.source_schema,
                ep_cfg.db_connection.case_schema,
                'ep_in_sources', 'ep_in_emissions', 'ep_in_activity_data',
                'ep_emission_sets']

    if ep_cfg.run_params.scenarios.all_emissions:
        log.debug('*** Global scenario ', ep_cfg.run_params.scenarios.all_emissions, 'will be applied')
        ftq_args.extend(('ep_scenario_list', 'ep_scenario_factors_all', ep_cfg.run_params.scenarios.all_emissions))
        ftq_placeholders.extend(['%s']*3)
    else:
        log.debug('*** No global scenario configured...')

    log.debug(
    cur.mogrify('SELECT ep_finalize_transformation_queue({})'.format(', '.join(ftq_placeholders)),
                ftq_args)
                )
    cur.execute('SELECT ep_finalize_transformation_queue({})'.format(', '.join(ftq_placeholders)),
                ftq_args)

    ep_connection.commit()
