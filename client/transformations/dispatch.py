from psycopg2 import DataError, ProgrammingError
from lib.ep_config import ep_cfg, ConfigFile
from lib.ep_libutil import ep_connection, ep_rtcfg, ep_debug, ep_internal_path

from lib.db import Relation, get_relation, get_geometry_relation
from transformations.builtin import TransformationQueue,\
                                    IntersectTransformation,\
                                    SourcesToGridTransformation,\
                                    MaskTransformation,\
                                    SourceFilterTransformation


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
        'to_grid': {'type': 'intersect', 'intersect': 'ep_grid_tz', 'mandatory': True},
        }

mandatory_transformations = [k for k, v in
                             builtin_transformations_conf.items()
                             if v['mandatory']]

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


def ensure_mandatory_transformations(chains):
    for idx, chain in enumerate(chains):
        for t in mandatory_transformations:
            if t not in getattr(chains, chain):
                getattr(chains, chain).append(t)


def create_default_transformation_queue(queue_id=1):
    case_schema = ep_cfg.db_connection.case_schema
    source_schema = ep_cfg.db_connection.source_schema

    transformation_queue = TransformationQueue(queue_id=queue_id,
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
    case_schema = ep_cfg.db_connection.case_schema
    source_schema = ep_cfg.db_connection.source_schema

    try:
        transconf = ConfigFile(ep_cfg.transformations.source,
                               ep_internal_path('transformations', 'configspec-transformations.conf')).values()
        if ep_cfg.transformations.chains is None:
            chains = [f for f in transconf.transformations if f.mandatory]
        else:
            chains = ep_cfg.transformations.chains
    except AttributeError:
        chains = []

    ensure_mandatory_transformations(chains)
    ep_rtcfg['transformation_queues'] = []
    if len(list(chains))==0:
        transformation_queue = create_default_transformation_queue()
        transformation_queue.insert(IntersectTransformation(inrel2=Relation(schema=case_schema,
                                                                   name='ep_grid_tz',
                                                                   fields=['grid_id'])),
                                    index=0)
        ep_rtcfg['transformation_queues'].append(transformation_queue)
    else:
        for idx, chain in enumerate(chains):
            transformation_queue = create_default_transformation_queue(queue_id=idx)
            ep_rtcfg['transformation_queues'].append(transformation_queue)

            for tidx, transname in enumerate(getattr(chains, chain)):
                filters = []
                trans = get_builtin_transformation(transname)
                if trans is None:
                    trans = getattr(transconf.transformations, transname)

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

                    if len(filters) > 0:
                        transformation_queue.insert(SourceFilterTransformation(filters=filters),
                                                    index=tidx)

                elif trans.type == 'mask':
                    maskrel = get_geometry_relation(trans.mask_file, trans.mask_filter)
                    transformation_queue.insert(MaskTransformation(maskrel=maskrel,
                                                                   mask_type=trans.mask_type),
                                                index=tidx)

                elif trans.type == 'intersect' and hasattr(trans, 'intersect') and trans.intersect is not None:
                    inrel2 = Relation(schema=case_schema, name=trans.intersect, fields=['grid_id'])
                    try:
                        outrel = get_relation(trans.target)
                        outrel.coef = outrel.fields[0]  # FIXME
                    except AttributeError:
                        outrel = None

                    transformation_queue.insert(IntersectTransformation(inrel2=inrel2, outrel=outrel), index=tidx)


def run():
    cur = ep_connection.cursor()
    ep_debug('*** Initialize transformation queue...')
    cur.execute('SELECT ep_init_transformation_queue(%s)', [ep_cfg.db_connection.case_schema])
    try:
        for i, q in enumerate(ep_rtcfg['transformation_queues']):
            ep_debug('Run transformation queue #', i)
            q.process()
    except (DataError, ProgrammingError):
        for notice in ep_connection.notices:
            ep_debug(notice)

        raise

    ep_debug('*** Finalize transformation queue...')
    cur.execute('SELECT ep_finalize_transformation_queue(%s, %s, %s, %s, %s, %s)',
                [ep_cfg.db_connection.source_schema,
                 ep_cfg.db_connection.case_schema,
                 'ep_in_sources', 'ep_in_emissions', 'ep_in_activity_data',
                 'ep_emission_sets']) # FIXME?

    ep_connection.commit()
