CREATE SEQUENCE IF NOT EXISTS spatial_ref_sys_srid_seq MINVALUE 100000;


/************************
* It creates grid of name gridtable and extension nx,ny.
* The resolution of the grid ix dx,dy, origin is ox,oy
* and coordinate system srid.
*************************/
drop function if exists ep_create_grid(
    gridschema text,
    gridtable text,
    nx integer,
    ny integer,
    dx double precision,
    dy double precision,
    xo double precision,
    yo double precision,
    srid integer);

create or replace function ep_create_grid(
    gridschema text,
    gridtable text,
    nx integer,
    ny integer,
    dx double precision,
    dy double precision,
    xo double precision,
    yo double precision,
    srid integer)
  returns boolean as
$$
declare
    ret boolean;
    res text;
    sqltext text;
    sqlinsert text;
    i integer;
    j integer;
    xmin double precision;
    xmax double precision;
    ymin double precision;
    ymax double precision;
    geomtext text;
    geomgrid geometry;
begin
    ret = false;
    -- tn = quote_ident(gridschema) || '.' || quote_ident(gridtable);
    -- drop old grid table
    --sqltext = 'drop table if exists ' || tn || ';';
    sqltext = format('drop table if exists %I.%I ', gridschema, gridtable);
    execute sqltext;
    -- create new grid table
    sqltext = format('create table %I.%I ( ' ||
        'grid_id serial, ' ||
        'i integer, ' ||
        --'j integer ' ||
        'j integer, ' ||
        'xmi double precision, ' ||
        'xma double precision, ' ||
        'ymi double precision, ' ||
        'yma double precision  ' ||
        ' )', gridschema, gridtable);
    execute sqltext;
    sqltext = format('alter table %I.%I add primary key (grid_id)', gridschema, gridtable);
    execute sqltext;
    -- create geometry column
    perform AddGeometryColumn(gridschema, gridtable, 'geom', srid, 'POLYGON', 2);

    -- create particular gridboxes and intersect them with timezones
    sqlinsert = format('insert into %I.%I (' ||
              'i, j, xmi, xma, ymi, yma, geom) values (' ||
              '$1, $2, $3, $4, $5, $6, $7 )', gridschema, gridtable);
    raise notice 'sqlinsert = %', sqlinsert;
    for i in 1 .. nx loop
        for j in 1 .. ny loop
            --raise notice 'Add gridbox i,j = %,%', i, j;
            xmin = xo+dx*(i-1-nx/2.0);
            xmax = xo+dx*(i-nx/2.0);
            ymin = yo+dy*(j-1-ny/2.0);
            ymax = yo+dy*(j-ny/2.0);
            geomtext = format('POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))',
                       xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin);
            --raise notice 'geomtext=%', geomtext;
            geomgrid = ST_GeometryFromText(geomtext,srid);
            execute sqlinsert using i, j, xmin, xmax, ymin, ymax, geomgrid;
        end loop;
    end loop;

    -- create geometry index
    execute format('create index if not exists %I on %I.%I using gist(geom)',  gridtable||'_geom', gridschema, gridtable);
    execute format('create index if not exists %I on %I.%I (i,j)',  gridtable||'_i_j', gridschema, gridtable);

    -- recompile statistics
    sqltext = format('analyze %I.%I', gridschema, gridtable);
    execute sqltext;

    ret = true;
    return ret;
end
$$
language plpgsql volatile
cost 100;



/************************
* It creates grid of name gridtable and extension nx,ny.
* The resolution of the grid ix dx,dy, origin is ox,oy
* and coordinate system srid.
*************************/
drop function if exists ep_create_grid_tz(
    case_schema text,
    ep_grid_tz text,
    conf_schema text,
    ep_grid text,
    ep_timezones text,
    srid integer);

create or replace function ep_create_grid_tz(
    case_schema text,
    ep_grid_tz text,
    conf_schema text,
    ep_grid text,
    ep_timezones text,
    srid integer)
  returns boolean as
$$
declare
    ret boolean;
    res text;
    sqltext text;
    sqlloop text;
    sqlinsert text;
    i integer;
    j integer;
    ag double precision;
    geomgrid geometry;
    tzid integer;
    geomi geometry;
begin
    ret = false;
    raise notice 'ep_create_grid_tz = %, %, %, %, %, %', case_schema,ep_grid_tz,conf_schema,ep_grid,ep_timezones,srid;

    -- create particular gridboxes and intersect them with timezones
    sqlloop = format('select i, j, geom from %I.%I',conf_schema, ep_grid);
    raise notice 'sqlloop = %', sqlloop;
    sqltext = format('select ST_Intersection(geom,$1),tz_id from %I.%I where ST_Intersects(geom,$2)',
                      case_schema, ep_timezones);
    raise notice 'sqltext = %', sqltext;
    sqlinsert = format('insert into %I.%I (' ||
              'i, j, tz_id, geom) values (' ||
              '$1, $2, $3, $4 )', case_schema, ep_grid_tz);
    raise notice 'sqlinsert = %', sqlinsert;

    for i,j,geomgrid in execute sqlloop
    loop
        for geomi,tzid in execute sqltext using geomgrid,geomgrid
        loop
            --execute format('select ST_GeometryFromText(%L,%L)', geomtext, srid) into geombin;
            --raise notice 'geombin=%', geombin;
            execute sqlinsert using i, j, tzid, ST_Multi(geomi);
            --raise notice 'Added gridbox %, %, %, %, %', i, j, tzid, ag, ST_Area(geomi), ST_Area(geomi)/ag, ST_AsText(geomi);
        end loop;
    end loop;

    -- recompile statistics
    execute format('analyze %I.%I', case_schema, ep_grid_tz);

    ret = true;
    return ret;
end
$$
language plpgsql volatile
cost 100;




--select ep_create_grid('test', 'ep_grid', 600, 400, 3000, 3000, 0, 0, 100000);
