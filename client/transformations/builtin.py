"""
Description: builtin transformation classes

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

from lib.db import Relation
from lib.ep_config import ep_cfg
from transformations.base import Transformation, OneToOneTransformation,\
                                 TwoToOneTransformation, \
                                 virtual
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)

_sql_operators = {'eq': '=', 'like': 'like', 'neq': '!=', 'nlike': 'not like',
                  'gt': '>', 'ge': '>=', 'lt': '<', 'le': '<='}


@virtual
class ScenarioTransformation(Transformation):
    """
    Save a mapping between transformation chains and scenarios inside
    the ep_transformation_chains_scenarios table.
    The mapping is then used in the ep_finalize_transformation_queue pl/pgsql
    functions (server/ep_case_proc.sql).
    """
    def __init__(self, scenarios):
        self.scenarios = scenarios
        if isinstance(self.scenarios, str):
            self.scenarios = self.scenarios.split(' ')

    def apply(self):
        with self.db_connection.cursor() as cur:
            cur.execute('INSERT INTO "{case_schema}"."ep_transformation_chains_scenarios" '
                        '(chain_id, scenario_id) '
                        'SELECT %(chain_id)s, scenario_id FROM "{source_schema}"."ep_scenario_list" '
                        'WHERE "scenario_name" = ANY(%(scenarios)s)'.
                        format(case_schema=self.cfg.db_connection.case_schema,
                               source_schema=self.cfg.db_connection.source_schema),
                        {'scenarios': self.scenarios, 'chain_id': self.queue.queue_id})
            
            cur.execute('SELECT * FROM UNNEST(%(scenarios)s) '
                        'EXCEPT '
                        'SELECT scenario_name FROM "{source_schema}"."ep_scenario_list" '.
                        format(source_schema=self.cfg.db_connection.source_schema),
                        {'scenarios': self.scenarios})
            not_scenarios = [scen[0] for scen in cur.fetchall()]  
            if not_scenarios:
                raise ValueError('Scenarios {} are not defined.'.format(', '.join(map(str, not_scenarios))))

    def cleanup(self):
        with self.db_connection.cursor() as cur:
            cur.execute('DELETE FROM "{case_schema}"."ep_transformation_chains_scenarios"'
                        'WHERE chain_id=%s'.format(case_schema=self.cfg.db_connection.case_schema),
                        [self.queue.queue_id])


@virtual
class LevelFilterTransformation(Transformation):
    """
    Save a mapping between transformation chains and vertical levels inside
    the ep_transformation_chains_levels table.
    """
    def __init__(self, level):
        self.level = level

    def apply(self):
        sqltext = 'INSERT INTO "{case_schema}"."ep_transformation_chains_levels" '\
                  '(chain_id, vertical_level) VALUES ({chain_id}, {level}) ' \
                  'ON CONFLICT DO NOTHING'.\
                  format(case_schema=self.cfg.db_connection.case_schema,\
                  level=self.level, chain_id=self.queue.queue_id)
        log.debug('LevelFilterTransformation:', 'level', self.level, 'chain_id', self.queue.queue_id)
        log.debug(sqltext)
        with self.db_connection.cursor() as cur:
            cur.execute(sqltext)

    def cleanup(self):
        with self.db_connection.cursor() as cur:
            cur.execute('DELETE FROM "{case_schema}"."ep_transformation_chains_levels"'
                        'WHERE chain_id=%s'.format(case_schema=self.cfg.db_connection.case_schema),
                        [self.queue.queue_id])


class MaskTransformation(TwoToOneTransformation):
    parameters = ['inrelation', 'inrelation2', 'outrelation', 'outsrid',
                  'mask_filters', 'mask_type']

    _mask_postgis_functions = {'': 'St_Intersection', 'inside': 'St_Intersection', 'outside': 'St_Difference'}
    # remark: ST_Disjoin is not used here according the postgis reference:
    # The opposite of ST_Intersects is ST_Disjoint(geometry A , geometry B). If two geometries are disjoint, they do not intersect, and vice-versa.
    # In fact, it is often more efficient to test "not intersects" than to test "disjoint" because the intersects tests can be spatially indexed, while the disjoint test cannot.
    #_union_postgis_functions = {'': 'St_Union', 'any': 'St_Union', 'all': 'St_Difference'}

    def __init__(self, inrel=None, maskrel=None, outrel=None, outsrid=None,
                 mask_filters=None, mask_type=None):
        super().__init__(outrel=outrel, outsrid=outsrid)
        self.has_coef = True
        self.inrelation = inrel
        self.inrelation2 = maskrel
        self.outrelation = outrel
        self.mask_filters = mask_filters
        self.mask_type = mask_type

    def apply(self):
        self.outrelation.fields = self.inrelation.fields[:]
        cur = self.db_connection.cursor()
        mask_pg_function = self._mask_postgis_functions[self.mask_type]
        log.debug('mask_pg_function: ', mask_pg_function)
        q = cur.mogrify(
            'SELECT * FROM ep_mask('
            '%s, %s, %s, %s, %s, %s, '
            '%s, %s, %s, %s, %s, %s, %s, %s, %s '
            ')', [self.inrelation.schema,
                  self.inrelation.name,
                  '{'+','.join([i for i in self.inrelation.fields])+'}',
                  self.inrelation.coef,
                  self.inrelation2.schema,
                  self.inrelation2.name,
                  self.mask_filters,
                  mask_pg_function,
                  self.outrelation.schema,
                  self.outrelation.name,
                  self.outsrid,
                  self.outrelation.pk,
                  self.outrelation.coef,
                  True,  # FIXME
                  self.outrelation.temp
                  ])
                # pridat do volani funkce filtr a typ intersektu
        
        log.debug(q)
        res = cur.execute(q)
        log.sql_debug(self.db_connection)
        return res


class SRIDTransformation(OneToOneTransformation):
    """
    Transform a table from one SRID to another.
    """
    def apply(self):
        cur = self.db_connection.cursor()
        cur.execute('DROP TABLE IF EXISTS {out_schema}.{out_table}'.format(
                    out_schema=self.outrelation.schema, out_table=self.outrelation.name))
        self.outrelation.fields = self.inrelation.fields[:]
        self.outrelation.coef = self.inrelation.coef
        self.outrelation.srid = self.outsrid
        in_fields = ','.join(self.inrelation.fields)
        if self.inrelation.coef:
            in_fields += (','+self.inrelation.coef)

        sql = 'CREATE TABLE "{out_schema}".{out_table} AS SELECT {in_fields}, '\
              'St_SetSrid(St_Transform({in_geom}, {srid}),{srid}) AS {out_geom} ' \
              'FROM "{in_schema}".{in_table}'.\
              format(in_schema=self.inrelation.schema,
                     out_schema=self.outrelation.schema,
                     in_table=self.inrelation.name,
                     out_table=self.outrelation.name,
                     in_fields=in_fields,
                     in_geom=self.inrelation.geom_field,
                     out_geom=self.outrelation.geom_field,
                     srid=self.outsrid)

        log.debug(sql)
        return cur.execute(sql)


class AreaTransformation(OneToOneTransformation):
    '''
    Multiply the emission by area of geometry object
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.has_coef = True

    def apply(self):
        cur = self.db_connection.cursor()
        cur.execute('DROP TABLE IF EXISTS {out_schema}.{out_table}'.format(
                    out_schema=self.outrelation.schema, out_table=self.outrelation.name))
        self.outrelation.fields = self.inrelation.fields[:]
        self.outrelation.coef = self.inrelation.coef
        self.outrelation.srid = self.inrelation.srid
        q = 'CREATE TABLE "{out_schema}".{out_table} AS SELECT {in_fields}, '
        if not hasattr(self.inrelation, 'coef') or self.inrelation.coef == '' or self.inrelation.coef is None:
            q += 'St_Area({in_geom}) AS {out_coef}, '
        else:
            q += '{in_coef} * St_Area({in_geom}) AS {out_coef}, '
        q += '{in_geom} AS {out_geom} FROM "{in_schema}".{in_table}'
        sql = q.format(in_schema=self.inrelation.schema,
                       out_schema=self.outrelation.schema,
                       in_table=self.inrelation.name,
                       out_table=self.outrelation.name,
                       in_fields=','.join(self.inrelation.fields),
                       in_geom=self.inrelation.geom_field,
                       out_geom=self.outrelation.geom_field,
                       in_coef=self.inrelation.coef,
                       out_coef=self.outrelation.coef,
                       srid=self.outsrid)
        log.debug(sql)
        return cur.execute(sql)

    def cleanup(self):
        with self.db_connection.cursor() as cur:
            cur.execute('DROP TABLE "{}".{}'.format(self.outrelation.schema,
                                                    self.outrelation.name))

class LimitToGridTransformation(MaskTransformation):
    '''
    Limits the emission sources to sources intersecting with the grid envelope
    It can also transforms geometries to output srid.
    These actions can speedup following transaction for large geometry sets
    '''
    parameters = ['inrelation', 'outrelation', 'outsrid']

    def __init__(self, inrel=None, outrel=None, outsrid=None):
        maskrel = Relation(schema=ep_cfg.db_connection.case_schema, name='ep_grid_env')
        super().__init__(inrel=inrel, maskrel=maskrel, outrel=outrel, outsrid=outsrid,
                         mask_type='inside', mask_filters=None)

    def __str__(self):
        return 'Limit to grid: ' + str(self.inrelation) + ' -> ' + str(self.outrelation)


class IntersectTransformation(TwoToOneTransformation):
    """
    Intersects input shapes with the assigned geometry sets and calculates
    the intersect coefficients.
    """
    parameters = ['inrelation', 'inrelation2', 'outrelation', 'outsrid', 'normalize']

    def __init__(self, inrel1=None, inrel2=None, outrel=None, outsrid=None, normalize=True):
        super().__init__(inrel1=inrel1, inrel2=inrel2, outrel=outrel, outsrid=outsrid)
        self.has_coef = True
        self.normalize = normalize

    def __str__(self):
        return 'Intersect: ' + str(self.inrelation) + ' # ' + str(self.inrelation2) + ' -> ' + str(self.outrelation)

    def apply(self):
        cur = self.db_connection.cursor()
        self.outrelation.srid = self.outsrid
        self.outrelation.fields = list(set(self.inrelation.fields) | set(self.inrelation2.fields))
        q = cur.mogrify(
            'SELECT * FROM ep_intersection('
            '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s'
            ')', [self.inrelation.schema,
                  self.inrelation.name,
                  '{'+','.join([i for i in self.inrelation.fields])+'}',
                  self.inrelation.coef,
                  self.inrelation2.schema, self.inrelation2.name,
                  '{'+','.join(i for i in self.inrelation2.fields)+'}',
                  self.inrelation2.coef,
                  self.outrelation.schema, self.outrelation.name,
                  self.outsrid, self.outrelation.pk,
                  self.outrelation.geom_field,
                  self.outrelation.coef,
                  self.normalize,
                  True,  # FIXME
                  self.outrelation.temp])
        log.debug(q)
        res = cur.execute(q)
        log.sql_debug(self.db_connection)
        return res

    def cleanup(self):
        with self.db_connection.cursor() as cur:
            cur.execute('DROP TABLE "{}".{}'.format(self.outrelation.schema,
                                                  self.outrelation.name))

class ToGridTransformation(IntersectTransformation):
    """
    Transform input shapes into the output grid.
    Usually run as the penultimate step in the transformation chain before
    SourcesToGridTransformation.
    """
    def __init__(self, inrel=None, outrel=None, normalize=True, method='Area', mandatory=True):
        self.method = method
        self.mandatory = mandatory
        self.case_schema = ep_cfg.db_connection.case_schema
        inrel2 = Relation(schema=self.case_schema, name='ep_grid_tz', fields=['grid_id'])
        super().__init__(inrel1=inrel, inrel2=inrel2, outrel=outrel, normalize=normalize)

    def __str__(self):
        return 'ToGrid: ' + str(self.inrelation) + ' -> ' + str(self.outrelation) + ' -> ' + str(self.method)

    def apply(self):
        log.debug("ToGridTransformation apply:", self.method)
        if self.method == 'Area':
            super().apply()
        elif self.method == 'Centre':
            self.to_center()
        else:
            log.debug('Unknown method ' + self.method + ' in to_grid transformation!!!')

    def to_center(self):
        cur = self.db_connection.cursor()

        # TODO - check geometry dimmension (only dim=2 is allowed)
        # connect objects to the grids by their centers
        tgtable = '{}_togridcenter'.format(self.outrelation.name)
        cur.execute('DROP TABLE IF EXISTS {tgschema}.{tgtable}'.format(
            tgschema=self.case_schema, tgtable=tgtable))
        infields = ','.join(['r.'+i for i in self.inrelation.fields])
        log.debug('to_center infields:', infields)
        gridfields = ','.join(['g.' + i for i in self.inrelation2.fields])
        log.debug('to_center gridfields:', gridfields)
        # FIXME: is the following code prepared for future? Delete otherwise
        #if self.inrelation.srid == self.inrelation2.srid:
        #    sqltrans = 'r.{geomt}'.format(geomt=self.inrelation.geom_field)
        #else:
        #    sqltrans = format('ST_Transform(r.{geomt},{sridg})',
        #                      geomt=self.inrelation.geom_field, sridg=self.inrelation2.srid)
        ic = '1.0' if self.inrelation.coef == '' else 'r.'+self.inrelation.coef
        oc = self.inrelation.coef if self.outrelation.coef == '' else self.outrelation.coef
        log.debug('inrelation.coef: ', ic)
        sqltext = 'CREATE TABLE {tgschema}.{tgtable} AS ( ' \
                  'SELECT {infields}, {gridfields}, {incoef} AS {outcoef} , g.{geomg} ' \
                  'FROM {inschema}.{intable} r ' \
                  'join {gridschema}.{gridtable} g ' \
                  'on st_intersects(r.{geomt}, ST_Centroid(g.{geomg})))'.format(
                  tgschema=self.case_schema,tgtable=tgtable,
                  infields=infields, gridfields=gridfields, incoef=ic, outcoef=oc,
                  inschema=self.inrelation.schema, intable=self.inrelation.name,
                  gridschema=self.inrelation2.schema, gridtable=self.inrelation2.name, geomt=self.inrelation.geom_field,
                  geomg=self.inrelation2.geom_field)
        log.debug(sqltext)
        res = cur.execute(sqltext)
        log.sql_debug(self.db_connection)

        # calculate number of the grids for every input geometry
        tgntable = '{}_togridnumbers'.format(self.outrelation.name)
        cur.execute('DROP TABLE IF EXISTS {tgnschema}.{tgntable}'.format(
            tgnschema=self.case_schema, tgntable=tgntable))
        sqltext = 'CREATE TABLE {tgnschema}.{tgntable} AS ( ' \
                  'SELECT {infields}, count(*) AS NUMBEROFGRIDS ' \
                  'FROM {tgschema}.{tgtable} r ' \
                  'GROUP BY {infields} )'.format(
            tgnschema=self.case_schema, tgntable=tgntable, infields=infields,
            tgschema=self.case_schema, tgtable=tgtable)
        log.debug(sqltext)
        res = cur.execute(sqltext)
        log.sql_debug(self.db_connection)

        # TODO - solve cases where grid centre lays on border of two or more areas

        # adjust of the grids for every input geometry
        cur.execute('DROP TABLE IF EXISTS {outschema}.{outtable}'.format(
            outschema=self.outrelation.schema, outtable=self.outrelation.name))
        joinfields = ' AND '.join(['r.'+i+'=n.'+i for i in self.inrelation.fields])
        sqltext = 'CREATE TABLE {outschema}.{outtable} AS ( ' \
                  'SELECT {infields}, r.grid_id, r.{outcoef}/n.NUMBEROFGRIDS AS {outcoef}, r.{geomg} ' \
                  'FROM {tgschema}.{tgtable} r ' \
                  'JOIN {tgschema}.{tgntable} n ON {joinfields} )'.format(
            outschema=self.outrelation.schema, outtable=self.outrelation.name,
            infields=infields, outcoef=oc, geomg=self.inrelation2.geom_field,
            tgschema=self.case_schema, tgtable=tgtable, tgntable=tgntable,
            joinfields=joinfields)
        log.debug(sqltext)
        res = cur.execute(sqltext)
        log.debug('Result: ', res)
        log.sql_debug(self.db_connection)
        return res

    def cleanup(self):
        with self.db_connection.cursor() as cur:
            cur.execute('DROP TABLE "{}".{}'.format(self.outrelation.schema,
                                                    self.outrelation.name))


class SimpleMaskTransformation(IntersectTransformation):
    def __init__(self, inrel=None, mask=None, outrel=None, outsrid=None):
        super().__init__(outrel=outrel, outsrid=outsrid)
        self.inrelation = inrel
        self.inrelation2 = mask
        self.outrelation = outrel

    @property
    def inrel(self):
        return self.inrelation

    @inrel.setter
    def inrel(self, rel):
        self.inrelation = rel

    @property
    def mask(self):
        return self.inrelation2

    @mask.setter
    def mask(self, rel):
        self.inrelation2 = rel

    def apply(self):
        super().apply(self)


class SurrogateTransformation(OneToOneTransformation):
    """
    Applies surrogates from the assigned set of the surrogate geometry shapes.
    """
    parameters = ['inrelation', 'surrogate_set', 'surrogate_type', 'outrelation', 'outsrid']

    def __init__(self, inrel=None, surset=None, surtype='limit', outrel=None, outsrid=None):
        super().__init__(inrel=inrel, outrel=outrel, outsrid=outsrid)
        self.has_coef = True
        self.surrogate_set = surset
        self.surrogate_type = surtype

    def __str__(self):
        return 'Surrogate: ' + str(self.inrelation) + ' # ' + str(self.surrogate_set) + ' -> ' + str(self.outrelation)

    def apply(self):
        cur = self.db_connection.cursor()
        source_schema = ep_cfg.db_connection.source_schema
        case_schema = ep_cfg.db_connection.case_schema
        self.outrelation.srid = self.outsrid

        surname = '{}_sur'.format(self.outrelation.name)
        surgeom = '{}_geom'.format(surname)
        if self.outrelation.temp:
            outschema = ''
            outtable = '"{}"'.format(self.outrelation.name)
            surtable = '"{}"'.format(surname)
            surtablex = '{}'.format(surname)
            sqltemp = ' TEMP '
        else:
            outschema = case_schema
            outtable = '"{}"."{}"'.format(case_schema, self.outrelation.name)
            surtable = '"{}"."{}"'.format(case_schema, surname)
            surtablex = '{}.{}'.format(case_schema, surname)
            sqltemp = ''

        # mask surrogate_set to grid from performance reasons (it should be possible to use MaskToGrid transformation)
        # get domain grid envelope and limit and transform surrogate set geometries
        sqltext = 'SELECT geom FROM "{}".ep_grid_env'.format(case_schema)
        cur.execute(sqltext)
        gridenv = cur.fetchone()[0]
        log.sql_debug(self.db_connection)
        # limit and transform surrogate set geometries
        cur.execute('DROP TABLE IF EXISTS {surtable}'.format(surtable=surtable))
        sqltext = 'CREATE {temp} TABLE {surtable} AS '\
                  ' SELECT g.geom_id as geom_id_sur, g.gset_id as gset_id_sur, g.geom_orig_id as geom_orig_id_sur, '\
                  ' St_Transform(g.geom, %s) AS "{surgeom}", g.weight as weight_sur FROM {sources}.ep_geometry_sets s '\
                  ' JOIN {sources}.ep_in_geometries g USING(gset_id) '\
                  ' WHERE s.gset_name = %s AND ST_Intersects(St_Transform(g.geom, %s), %s::geometry)'\
                  ' ORDER BY geom_id'.format(temp=sqltemp, surtable=surtable, surgeom=surgeom, sources=source_schema)
        log.debug(sqltext, self.outsrid, self.surrogate_set, self.outsrid, gridenv)
        cur.execute(sqltext, (self.outsrid, self.surrogate_set, self.outsrid, gridenv, ))
        log.sql_debug(self.db_connection)
        # register geometry of the new surrogate table and create geoetry index
        sqltext = 'SELECT populate_geometry_columns(\'{}\'::regclass)'.format(surtablex)
        log.debug(sqltext)
        cur.execute(sqltext)
        sqltext = 'create index "{}_{}" on {} using gist("{}");'.format(surname, surgeom, surtable, surgeom)
        log.debug(sqltext)
        cur.execute(sqltext)

        # input emission source is limitted to the surrogate areas contained in the emission source
        # intersect input geometries with surrogate geometries
        log.debug('Surrogate apply:')
        surgeomid = 'geom_id_sur'
        self.outrelation.fields = self.inrelation.fields[:] + [surgeomid]
        log.debug('Surrogate outrel fields', self.outrelation.fields)
        log.debug('infields: ', self.inrelation.fields)
        ifields1 = '{' + ','.join([i for i in self.inrelation.fields]) + '}'
        log.debug('ifields1: ', ifields1)
        ifields2 = '{' + surgeomid + '}'
        log.debug('ifields2: ', ifields2)
        log.debug('incoef:', self.inrelation.coef)
        log.debug('outcoef:', self.outrelation.coef)
        #
        sqltext = 'SELECT * FROM ep_intersection(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        sqltext = cur.mogrify(sqltext, [outschema, self.inrelation.name,
                  ifields1, '', #self.inrelation.coef,
                  outschema, surname, ifields2, 'weight_sur',
                  outschema, self.outrelation.name, self.outsrid, self.outrelation.pk,
                  self.outrelation.geom_field, self.outrelation.coef, False, True, self.outrelation.temp])
        log.debug(sqltext)
        cur.execute(sqltext)
        log.sql_debug(self.db_connection)
        # normalize surrogate output coefficients to original amount of emission
        sqltext = 'UPDATE {table} u SET {coef} = {coef}/(SELECT sum(s.{coef}) '\
                  'FROM {table} s WHERE s.geom_id = u.geom_id)'\
                  .format(coef=self.outrelation.coef, table=outtable)
        log.debug(sqltext)
        cur.execute(sqltext)
        log.sql_debug(self.db_connection)
        # normalize surrogate output coefficients from previous transactions
        if self.inrelation.coef != '':
            if self.inrelation.schema == '':
                intable = '"{}"'.format(self.inrelation.name)
            else:
                intable = '"{}"."{}"'.format(self.inrelation.schema, self.inrelation.name)
            sqltext = 'UPDATE {table2} t2 SET {coef2} = t2.{coef2}*t1.{coef1} ' \
                      'FROM {table1} t1 WHERE t1.geom_id = t2.geom_id' \
                      .format(table2=outtable, coef2=self.outrelation.coef, coef1=self.inrelation.coef, table1=intable)
            log.debug(sqltext)
            cur.execute(sqltext)
            log.sql_debug(self.db_connection)

        if self.surrogate_type == 'spread':
            # all input emission sources belonging to one surrogate are sumarized and spread to all this surrogate
            # -> replace source geometry with surrogate geometry
            # first get the transaction and surrogate geometry names and types
            sqltext = 'select f_geometry_column, srid, type from public.geometry_columns '\
                      ' where f_table_schema = %s and f_table_name = %s'
            log.debug(sqltext, outschema, self.outrelation.name)
            cur.execute(sqltext, (outschema, self.outrelation.name,))
            outgeom, outgeom_srid, outgeom_type = cur.fetchone()[:]
            log.sql_debug(self.db_connection)
            log.debug(sqltext, outschema, surname)
            cur.execute(sqltext, (outschema, surname,))
            geomsur, geomsur_srid, geomsur_type = cur.fetchone()[:]
            log.sql_debug(self.db_connection)
            # drop old geometry column
            sqltext = 'alter table {} drop column "{}"'.format(outtable, outgeom)
            log.debug(sqltext)
            cur.execute(sqltext)
            log.sql_debug(self.db_connection)
            # add new geometry column of the correct geometry type
            sqltext = 'alter table {} add {} geometry({},{})'.format(outtable, outgeom, geomsur_type, geomsur_srid)
            log.debug(sqltext)
            cur.execute(sqltext)
            log.sql_debug(self.db_connection)
            # replace source geometry with surrogate geometry
            sqltext = 'UPDATE {outtable} u SET "{outgeom}" = (SELECT s."{surgeom}" FROM {surtable} s '\
                      ' WHERE s.geom_id_sur = u.geom_id_sur)'.format(outtable=outtable, outgeom=outgeom, surgeom=surgeom,\
                                                                 surtable=surtable)
            log.debug(sqltext)
            cur.execute(sqltext)
            log.sql_debug(self.db_connection)
        # end of surrogates


class SourcesToGridTransformation(Transformation):
    def __init__(self, inrel=None, emisrel=None, srcrel=None, actrel=None, esetrel=None, case_schema=None, filters=None):
        super().__init__()
        self.inrelation = inrel
        self.sources_relation = srcrel
        self.activity_relation = actrel
        self.eset_relation = esetrel
        self.case_schema = case_schema
        if filters is None:
            self.filters = []
        else:
            self.filters = filters

    def __str__(self):
        return 'Sources to grid transformation: ' + str(self.inrelation)+' -> ' +\
                str(self.sources_relation)

    def apply(self):
        cur = self.db_connection.cursor()
        filters, filter_values, joins = SourceFilterTransformation.parse_filters(self.filters, fqn=False)
        join_texts = ''
        for j in joins:
            if j == 'eset':
                join_texts += (' JOIN "{sch}".ep_emission_sets AS es USING (eset_id)'.format(
                                sch=self.cfg.db_connection.source_schema))
            elif j == 'inventory':
                join_texts += (' JOIN "{sch}".ep_emission_sets AS es USING (eset_id)'
                               ' JOIN "{sch}".ep_source_files AS sf USING (file_id)'
                               ' JOIN "{sch}".ep_inventories AS inv USING (inv_id)'.format(
                                sch=self.cfg.db_connection.source_schema))

        if filters != '' and len(filter_values)!=0:
            filters = cur.mogrify(filters, filter_values).decode('UTF-8')
        log.debug('SourcesToGridTransformation apply filters:', filters)
        q = cur.mogrify('SELECT * FROM ep_sources_to_grid'
                        '(%s, %s, %s, %s, %s, %s, %s, %s)',
                        [self.sources_relation.schema, self.case_schema,
                         self.sources_relation.name,
                         self.inrelation.name, self.inrelation.coef,
                         self.queue.queue_id,
                         join_texts, filters])
        log.debug('SourcesToGridTransformation:', q)
        cur.execute(q)
        log.sql_debug(self.db_connection)


class SourceFilterTransformation(OneToOneTransformation):
    """
    Filter sources by one or more of `_available_filters`
    Example:
    SourceFilterTransformation(inrel=Relation(schema='sources',
                                              name='ep_in_sources'),
                               outrel=Relation(schema='sources',
                                               name='my_filter', temp=True),
                               eset__like='ATEM_1.0*', source_type__eq='P')

    Available filter operators: defined by _sql_operators dictionary
    """
    parameters = ['inrelation', 'outrelation', 'outsrid','filters','mask_to_grid','transform_srid']
    _available_filters = {'inventory': 'ep_inventories.inv_name',
                          'eset': 'ep_emission_sets.eset_name',
                          'source_type': 'ep_in_sources.source_type'}

    def __init__(self, inrel=None, outrel=None, outsrid=None, filters=None,
                 mask_to_grid=True, transform_srid=True):
        super().__init__(inrel=inrel, outrel=outrel, outsrid=outsrid)
        if filters is not None:
            self.filters = [f for f in filters
                            if f[0].rsplit('__', 1)[0] in
                            SourceFilterTransformation._available_filters]
        self.outsrid = outsrid
        self.mask_to_grid = mask_to_grid
        self.transform_srid = transform_srid

    def __str__(self):
        return 'Source filter: ' + str(self.inrelation) + ' -> ' + str(self.outrelation)

    @staticmethod
    def parse_filters(filters, fqn=True):
        filter_sql = ''
        filter_values = []
        joins = set()
        for k, v in filters:
            if filter_sql != '':
                filter_sql += ' AND '

            col, op = k.rsplit('__', 1)
            try:
                sql_op = _sql_operators[op]
            except KeyError:
                sql_op = '='

            try:
                db_col = SourceFilterTransformation._available_filters[col]
                if not fqn:
                    db_col = db_col.split('.')[1]
            except KeyError:
                continue

            filter_sql += '{col} {op} %s'.format(col=db_col, op=sql_op)
            filter_values.append(v)

            if col in ('eset', 'inventory'):
                joins.add(col)

        return (filter_sql, filter_values, joins)

    def apply(self):
        cur = self.db_connection.cursor()

        # prepare select columns
        sqltext = 'SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name=%s'
        cur.execute(sqltext, [self.cfg.db_connection.source_schema, 'ep_in_geometries'])
        select_cols = ''
        for row in cur.fetchall():
            if select_cols != '':
                select_cols += ','
            if row[0] == 'geom':
                geom_field = 'i.geom'
                if self.transform_srid:
                    geom_field = 'ST_Transform({},{})'.format(geom_field, self.outsrid)
                    self.outrelation.srid = self.outsrid
                if self.mask_to_grid:
                    geom_field = 'ST_Intersection({}, %s::geometry)'.format(geom_field)
                select_cols += 'ST_Multi({}) as geom'.format(geom_field)
            else:
                select_cols += 'i.' + row[0]

        # prepare filters and joins
        filters, filter_values, joins = SourceFilterTransformation.parse_filters(self.filters)
        join_texts = ''
        for j in joins:
            if j == 'eset':
                select_cols = 'DISTINCT {} '.format(select_cols)
                join_texts += (' JOIN "{sch}".ep_in_sources USING (geom_id)'
                               ' JOIN "{sch}".ep_emission_sets USING (eset_id)'.format(
                                sch=self.cfg.db_connection.source_schema))
            elif j == 'inventory':
                select_cols = 'DISTINCT {} '.format(select_cols)
                join_texts += (' JOIN "{sch}".ep_in_sources USING (geom_id)'
                               ' JOIN "{sch}".ep_emission_sets USING (eset_id)'
                               ' JOIN "{sch}".ep_source_files USING (file_id)'
                               ' JOIN "{sch}".ep_inventories USING (inv_id)'.format(
                                sch=self.cfg.db_connection.source_schema))

        # add part of the where clausule to limit geometries to grid envelope
        if self.mask_to_grid:
            # get domain grid envelope
            sqltext = 'SELECT geom FROM "{}".ep_grid_env'.format(self.cfg.db_connection.case_schema)
            log.debug(sqltext)
            cur.execute(sqltext)
            gridenv = cur.fetchone()[0]
            log.sql_debug(self.db_connection)
            #  limit geometries where clausule
            if filters != '':
                filters += ' AND '
            filters += 'ST_Intersects(St_Transform(i.geom, {}), %s::geometry)'.format(self.outsrid)
            filter_values.insert(0, gridenv)
            filter_values.append(gridenv)
            # output table has to contain coef, add it if needed
            if self.outrelation.coef is None or self.outrelation.coef == '':
                self.outrelation.coef = 'coef'

        if self.outrelation.temp:
            self.outrelation.schema = ''
            outrelation_schema = ''
            self.fullname = '"' + self.outrelation.name + '"'
        else:
            outrelation_schema = self.outrelation.schema
            self.fullname = '"' + outrelation_schema + '"' + '.' + '"' + self.outrelation.name + '"'
        # test/drop output table
        cur.execute('DROP TABLE IF EXISTS {outtable}'.format(outtable=self.fullname))
        # create filtered table
        q = 'CREATE {lifetime} TABLE {fullname} AS '\
            ' SELECT {select_cols} FROM "{inschema}".{inname} AS i {joins}'.format(
                select_cols=select_cols,
                lifetime=self.outrelation.lifetime,
                fullname=self.fullname,
                inschema=self.inrelation.schema, inname=self.inrelation.name,
                joins=join_texts)

        if filters != '':
            q += ' WHERE ' + filters

        self.outrelation.fields = self.inrelation.fields[:]
        self.outrelation.coef = self.inrelation.coef
        sql = cur.mogrify(q, filter_values)
        log.debug(sql)
        cur.execute(sql)
        log.sql_debug(self.db_connection)
        # register geometry of the new table and create geoetry index
        sqltext = 'SELECT populate_geometry_columns(\'"{}"."{}"\'::regclass)'.format(outrelation_schema, self.outrelation.name)
        log.debug(sqltext)
        cur.execute(sqltext)
        sqltext = 'create index "' + self.outrelation.name + '_geom" on ' + self.fullname + ' using gist(geom);'
        log.debug(sqltext)
        cur.execute(sqltext)

    def cleanup(self):
        with self.db_connection.cursor() as cur:
            cur.execute('SELECT EXISTS( SELECT FROM pg_views WHERE schemaname=\'{}\' AND viewname=\'{}\')' \
                        .format(self.outrelation.schema, self.outrelation.name))
            ex = cur.fetchone()[0]
            if ex:
                cur.execute('DROP VIEW IF EXISTS "{}"."{}"' \
                            .format(self.outrelation.schema, self.outrelation.name))


class TransformationQueue():
    def __init__(self, queue_id, name, inrel, outrel, outsrid=None, outid=None, outgeom=None,
                 **kwargs):
        self.queue_id=queue_id
        self.name = name
        self.queue = []
        self.inrelation = inrel
        self.outrelation = outrel
        self.outsrid = outsrid
        self.outid = outid
        self.outgeom = outgeom
        self.schema = 'transformations'
        self.filters = []

    def __str__(self):
        return "->\n".join(map(str, self.queue))

    def insert(self, transformation, index=None, *args, **kwargs):
        if isinstance(transformation, str):
            transformation = globals()[transformation]()
            for param in Transformation.parameters:
                if param in kwargs:
                    setattr(transformation, param, kwargs[param])

        transformation.queue = self

        if not hasattr(transformation, 'db_connection'):
            transformation.db_connection = self.db_connection

        if not hasattr(transformation, 'cfg'):
            transformation.cfg = self.cfg

        if not hasattr(transformation, 'rt_cfg'):
            transformation.rt_cfg = self.rt_cfg

        if index is None: # or index == -1:
            self.queue.append(transformation)
        else:
            self.queue.insert(index, transformation)

        if hasattr(transformation, 'filters'):
            self.filters.extend(transformation.filters)

    def append(self, transformation, *args, **kwargs):
        self.insert(transformation, index=None, *args, **kwargs)

    def process(self):
        cur = self.db_connection.cursor()
        ### HACK ###
        # table ep_transformation_chains_levels needs to be cleaned before new transformations
        # can be stored else key violation occures
        #cur.execute('DELETE FROM "{case_schema}"."ep_transformation_chains_levels"'.
        #            format(case_schema=self.cfg.db_connection.case_schema),)
        # fill transformation chains
        cur.execute('INSERT INTO "{case_schema}"."ep_transformation_chains" AS ch '
                    '(chain_id, name) VALUES (%(chain_id)s, %(name)s) '
                    'ON CONFLICT (chain_id) DO '
                    '    UPDATE SET name=%(name)s WHERE ch.chain_id=%(chain_id)s'.
                    format(case_schema=self.cfg.db_connection.case_schema),
                    {'chain_id': self.queue_id, 'name': self.name})

        numtrans = len(self.queue)
        filters = None
        for i, trans in enumerate(self.queue):
            log.debug('*** Transformation {:4d}'.format(i), trans)

            # Virtual transformations don't create new tables in the chain,
            # so we apply them and skip all the rest of the processing
            if trans.virtual:
                trans.apply()
                self.db_connection.commit()
                continue

            if i == 0:
                trans.inrelation = self.inrelation
            else:
                trans.inrelation = transmm.outrelation

            if i == numtrans-1:
                trans.outrelation = self.outrelation
                trans.outsrid = self.outsrid
                trans.outgeom = self.outgeom
                trans.outid = self.outid
            else:
                if self.inrelation.coef != '' or trans.has_coef:
                    mycoef = 'coef'
                else:
                    mycoef = ''

                if not hasattr(trans, 'outrelation') or trans.outrelation is None:
                    myrelname = self.inrelation.name + '_q{}_trans{}'.format(self.queue_id, i+1)
                    trans.outrelation = Relation(schema=self.schema,
                                                 name=myrelname,
                                                 pk='id{}'.format(i+1),
                                                 fields=trans.inrelation.fields,
                                                 geom_field='geom',
                                                 coef=mycoef,
                                                 temp=False)  # False for debug, True for oprational runs
                elif not hasattr(trans.outrelation,'coef') or trans.outrelation.coef is None:
                    trans.outrelation.coef = mycoef

                if not hasattr(trans, 'outsrid') or trans.outsrid is None:
                    trans.outsrid = self.outsrid

            log.fmt_debug('*** Transformation {:4d} {}', i, trans)

            if isinstance(trans, SourceFilterTransformation):
                filters = trans.filters
            elif isinstance(trans, SourcesToGridTransformation) and filters is not None:
                trans.filters = filters

            if i < numtrans-1 and trans.outrelation is not None:
                # delete possible outrel table or view which remainded from previous runs if outrel.temp was disabled
                # test if target table exists
                # TODO not sure it is necessary, check!!
                cur.execute('SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname=\'{}\' AND tablename=\'{}\')'\
                            .format(trans.outrelation.schema, trans.outrelation.name))
                ex = cur.fetchone()[0]
                if ex:
                    cur.execute('DROP TABLE IF EXISTS "{}"."{}"'\
                                .format(trans.outrelation.schema, trans.outrelation.name))
                else:
                    cur.execute('SELECT EXISTS( SELECT FROM pg_views WHERE schemaname=\'{}\' AND viewname=\'{}\')'\
                                .format(trans.outrelation.schema, trans.outrelation.name))
                    ex = cur.fetchone()[0]
                    if ex:
                        cur.execute('DROP VIEW IF EXISTS "{}"."{}"'\
                                    .format(trans.outrelation.schema, trans.outrelation.name))

            trans.apply()
            self.db_connection.commit()

            # in the next iteration we will need the current transformation, so save it as transmm
            transmm = trans

        if ep_cfg.transformations.cleanup:
            for trans in self.queue[:-1]:
                try:
                    trans.cleanup()
                except AttributeError:
                    pass

            cur.execute('DELETE FROM "{case_schema}"."ep_transformation_chains_scenarios"'
                        'WHERE chain_id=%s'.
                        format(case_schema=self.cfg.db_connection.case_schema),
                        [self.queue_id])
            cur.execute('DELETE FROM "{case_schema}"."ep_transformation_chains_levels"'
                        'WHERE chain_id=%s'.
                        format(case_schema=self.cfg.db_connection.case_schema),
                        [self.queue_id])
            cur.execute('DELETE FROM "{case_schema}"."ep_transformation_chains"'
                        'WHERE chain_id=%s'.
                        format(case_schema=self.cfg.db_connection.case_schema),
                        [self.queue_id])

        self.db_connection.commit()
