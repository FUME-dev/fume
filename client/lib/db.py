import re
import uuid
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_connection, ep_debug


class AmbiguousRelationName(Exception):
    pass


class RelationNotFound(Exception):
    pass


class Relation():
    def __str__(self):
        return 'Relation: ' + self.schema + '.' + self.name + '(' +\
                ', '.join(map(str, self.fields)) + ', pk=' + self.pk +\
                ', geom=' + self.geom_field + ')'

    def __init__(self, schema, name, fields=None, coef='', pk='id',
                 geom_field='geom', temp=False):
        self.schema = schema
        self.name = name
        if fields is None:
            fields = []

        self.fields = fields
        self.coef = coef
        self.pk = pk
        self.geom_field = geom_field
        self.temp = temp
        if self.temp:
            self.lifetime = 'TEMPORARY'
        else:
            self.lifetime = ''


def get_geometry_relation(filename, mask=''):
    cur = ep_connection.cursor()
    viewname = str(uuid.uuid1())
    cur.execute('SELECT gset_table FROM "{source_schema}"."ep_geometry_sets" WHERE gset_name=%s'.format(source_schema=ep_cfg.db_connection.source_schema), [filename])
    row = cur.fetchone()
    raw_table = row[0]

    if mask != '':
        mask = 'WHERE '+mask
    cur.execute('CREATE TABLE "{schema}"."{name}" AS SELECT * FROM "{source_schema}".ep_in_geometries JOIN "{source_schema}".ep_geometry_sets USING(gset_id) WHERE gset_name=%s AND geom_orig_id IN (SELECT geom_orig_id FROM "{source_schema}"."{raw_table}" {mask})'.format(schema=ep_cfg.db_connection.case_schema, name=viewname, source_schema=ep_cfg.db_connection.source_schema, raw_table=raw_table, mask=mask), [filename])
    return Relation(name=viewname, schema=ep_cfg.db_connection.case_schema)


def get_relation(relname):
    relname_re = re.compile('(([^.]*)\.)?([^(]*)(\((.*)\))?')
    _, schema, relname, _, column = relname_re.search(relname).groups()
    cur = ep_connection.cursor()
    if schema is None:
        allschemas = tuple(set(ep_cfg.db_connection.all_schemas.values()))
        ep_debug('Searching for relation {}.{} in schemas {}'.format(schema, relname, allschemas))
        cur.execute('SELECT schemaname, tablename AS relname FROM pg_tables '
                    'WHERE tablename=%s AND schemaname IN %s UNION '
                    'SELECT schemaname, viewname AS relname FROM pg_views '
                    'WHERE viewname=%s AND schemaname IN %s', [relname, allschemas, relname, allschemas])

        if cur.rowcount > 1:
            raise AmbiguousRelationName(relname)
        elif cur.rowcount == 0:
            raise RelationNotFound(relname)

        row = cur.fetchone()
        schema = row[0]
        relname = row[1]
        ep_debug('Found relation {}.{}'.format(schema, relname))

    fields = []
    if column is not None:
        fields.append(column)
    rel = Relation(schema=schema, name=relname, fields=fields)
    sqltext = 'SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS ' \
              'data_type FROM pg_index i ' \
              'JOIN pg_attribute a ON a.attrelid = i.indrelid ' \
              '                    AND a.attnum = ANY(i.indkey) ' \
              'WHERE  i.indrelid = %s::regclass ' \
              'AND    i.indisprimary'
    cur.execute(sqltext, ['"{}"."{}"'.format(schema, relname)])

    if cur.rowcount == 1:
        row = cur.fetchone()
        rel.pk = row[0]

    return rel
