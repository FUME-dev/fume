from lib.db import Relation
from transformations.base import Transformation, OneToOneTransformation,\
                                 TwoToOneTransformation

try:
    from lib.ep_libutil import ep_debug
except RuntimeError:
    ep_debug = print

_sql_operators = {'eq': '=', 'like': 'like', 'neq': '!=', 'nlike': 'not like',
                  'gt': '>', 'ge': '>=', 'lt': '<', 'le': '<='}


class MaskTransformation(TwoToOneTransformation):
    parameters = ['inrelation', 'inrelation2', 'outrelation', 'outsrid',
                  'mask_filters', 'mask_type']

    _mask_postgis_functions = {'': 'St_Intersection', 'inside': 'St_Intersection', 'outside': 'St_Difference'}
    _union_postgis_functions = {'': 'St_Union', 'any': 'St_Union', 'all': 'St_Difference'}
    def __init__(self, inrel=None, maskrel=None, outrel=None, outsrid=None,
                 mask_filters=None, mask_type=None):
        super().__init__(outrel=outrel, outsrid=outsrid)
        self.inrelation = inrel
        self.inrelation2 = maskrel
        self.outrelation = outrel
        self.mask_filters = mask_filters
        self.mask_type = mask_type

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
        self.outrelation.fields = self.inrelation.fields
        #self.outrelation.coef = self.inrelation.coef
        cur = self.db_connection.cursor()
        #condition = ''
        #if self.mask_filters != '':
        #    condition = 'WHERE '+self.mask_filters

        #q = 'SELECT ST_UNION(geom) FROM "{}"."{}" {}'.format(self.inrelation2.schema, self.inrelation2.name, condition)
        #ep_debug('mask geom:', q)
        #cur.execute(q)
        #geom = cur.fetchone()[0]

        pg_function = self._mask_postgis_functions[self.mask_type]
        q = cur.mogrify(
            'SELECT * FROM ep_mask('
            '%s, %s, %s, %s, %s, %s, '
            '%s, %s, %s, %s, %s, %s, %s, %s '
            ')', [self.inrelation.schema,
                  self.inrelation.name,
                  '{'+','.join([i for i in self.inrelation.fields])+'}',
                  self.inrelation.coef,
                  self.inrelation2.schema,
                  self.inrelation2.name,
                  self.mask_filters,
                  pg_function,
                  self.outrelation.schema,
                  self.outrelation.name,
                  self.outrelation.pk,
                  self.outrelation.coef,
                  True,  # FIXME
                  self.outrelation.temp
                  ])
                # pridat do volani funkce filtr a typ intersektu

        ep_debug(q)
        return cur.execute(q)


class SRIDTransformation(OneToOneTransformation):
    def apply(self):
        cur = self.db_connection.cursor()
        sql = 'CREATE TABLE "{out_schema}".{out_table} AS SELECT {in_fields}, '\
              'St_SetSrid(St_Transform(geom, {srid}),{srid}) AS {out_geom} ' \
              'FROM "{in_schema}".{in_table}'.\
              format(in_schema=self.inrelation.schema,
                     out_schema=self.outrelation.schema,
                     in_table=self.inrelation.name,
                     out_table=self.outrelation.name,
                     in_fields=','.join(self.inrelation.fields),
                     out_geom=self.outrelation.geom_field,
                     srid=self.outsrid)

        ep_debug(sql)
        return cur.execute(sql)


class IntersectTransformation(TwoToOneTransformation):
    def __str__(self):
        return 'Intersect: ' + str(self.inrelation) + ' # ' + str(self.inrelation2) + ' ->\n    ' + str(self.outrelation)

    def apply(self):
        cur = self.db_connection.cursor()
        q = cur.mogrify(
            'SELECT * FROM ep_intersection('
            '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s'
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
                  True,  # FIXME
                  self.outrelation.temp])

        ep_debug(q)
        res = cur.execute(q)
        ep_debug('Intersect:', res)
        for n in self.db_connection.notices:
            ep_debug('Intersect message:',n)
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


class SourcesToGridTransformation(Transformation):
    def __init__(self, inrel=None, emisrel=None, srcrel=None, actrel=None, esetrel=None, case_schema=None, filters=None):
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
        filters, filter_values, joins = SourceFilterTransformation.parse_filters(self.filters,
                                                                                 fqn=False)
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

        q = cur.mogrify('SELECT * FROM ep_sources_to_grid'
                        '(%s, %s, %s, %s, %s, %s, %s)',
                        [self.sources_relation.schema, self.case_schema,
                         self.sources_relation.name,
                         self.inrelation.name, self.inrelation.coef,
                         join_texts, filters])
        ep_debug(q)
        return cur.execute(q)


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
    _available_filters = {'inventory': 'ep_inventories.inv_name',
                          'eset': 'ep_emission_sets.eset_name',
                          'source_type': 'ep_in_sources.source_type'}

    def __init__(self, inrel=None, outrel=None, filters=None):
        super().__init__(inrel=inrel, outrel=outrel)
        if filters is not None:
            self.filters = [f for f in filters
                            if f[0].rsplit('__', 1)[0] in
                            SourceFilterTransformation._available_filters]

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
        select_cols = 'i.*'
        filters, filter_values, joins = SourceFilterTransformation.parse_filters(self.filters)
        join_texts = ''

        for j in joins:
            if j == 'eset':
                select_cols = 'DISTINCT({})'.format(select_cols)
                join_texts += (' JOIN "{sch}".ep_in_sources USING (geom_id)'
                               ' JOIN "{sch}".ep_emission_sets USING (eset_id)'.format(
                                sch=self.cfg.db_connection.source_schema))
            elif j == 'inventory':
                select_cols = 'DISTINCT({})'.format(select_cols)
                join_texts += (' JOIN "{sch}".ep_in_sources USING (geom_id)'
                               ' JOIN "{sch}".ep_emission_sets USING (eset_id)'
                               ' JOIN "{sch}".ep_source_files USING (file_id)'
                               ' JOIN "{sch}".ep_inventories USING (inv_id)'.format(
                                sch=self.cfg.db_connection.source_schema))

        if self.outrelation.temp:
            self.outrelation.schema = ''
            outrelation_schema = ''
        else:
            outrelation_schema = '"' + self.outrelation.schema + '"' + '.'

        self.fullname = outrelation_schema + self.outrelation.name
        q = 'CREATE OR REPLACE {lifetime} VIEW {fullname} AS SELECT {select_cols} FROM "{inschema}".{inname} AS i {joins}'.format(
                select_cols=select_cols,
                lifetime=self.outrelation.lifetime,
                fullname=self.fullname,
                inschema=self.inrelation.schema, inname=self.inrelation.name,
                joins=join_texts)

        if filters != '':
            q += ' WHERE ' + filters

        self.outrelation.fields = self.inrelation.fields
        self.outrelation.coef = self.inrelation.coef
        sql = cur.mogrify(q, filter_values)
        ep_debug(sql)
        return cur.execute(sql)

    def cleanup(self):
        with self.db_connection.cursor() as cur:
            cur.execute('DROP VIEW {}'.format(self.fullname))


class TransformationQueue():
    def __init__(self, queue_id, inrel, outrel, outsrid=None, outid=None, outgeom=None,
                 **kwargs):
        self.queue_id=queue_id
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

        if not hasattr(transformation, 'db_connection'):
            transformation.db_connection = self.db_connection

        if not hasattr(transformation, 'cfg'):
            transformation.cfg = self.cfg

        if not hasattr(transformation, 'rt_cfg'):
            transformation.rt_cfg = self.rt_cfg

        if index is None or index == -1:
            self.queue.append(transformation)
        else:
            self.queue.insert(index, transformation)

        if hasattr(transformation, 'filters'):
            self.filters.extend(transformation.filters)

    def append(self, transformation, *args, **kwargs):
        self.insert(transformation, -1, *args, **kwargs)

    def process(self):
        numtrans = len(self.queue)
        filters = None
        for i, trans in enumerate(self.queue):
            if i == 0:
                trans.inrelation = self.inrelation
            else:
                transmm = self.queue[i-1]
                trans.inrelation = transmm.outrelation

            # if issubclass(trans, TwoToOneTransformation):
            #     trans.inrelation2 = trans.inrelation2

            if i == numtrans-1:
                trans.outrelation = self.outrelation
                trans.outsrid = self.outsrid
                trans.outgeom = self.outgeom
                trans.outid = self.outid
            else:
                if isinstance(trans, IntersectTransformation) or isinstance(trans, MaskTransformation):
                    mycoef = 'coef{}'.format(i+1)
                else:
                    mycoef = ''

                if not hasattr(trans, 'outrelation') or trans.outrelation is None:
                    myrelname = self.inrelation.name + '_q{}_trans{}'.format(self.queue_id, i+1)
                    trans.outrelation = Relation(schema=self.schema,
                                                 name=myrelname,
                                                 pk='id{}'.format(i+1),
                                                 geom_field='geom',
                                                 coef=mycoef,
                                                 temp=False)

                if not hasattr(trans, 'outsrid') or trans.outsrid is None:
                    trans.outsrid = self.outsrid

            ep_debug('*** Transformation {:4d}'.format(i), trans)
            if isinstance(trans, SourceFilterTransformation):
                filters = trans.filters
            elif isinstance(trans, SourcesToGridTransformation) and filters is not None:
                trans.filters = filters

            trans.apply()
            self.db_connection.commit()

        for trans in self.queue[:-1]:
            try:
                trans.cleanup()
            except AttributeError:
                pass

        self.db_connection.commit()
