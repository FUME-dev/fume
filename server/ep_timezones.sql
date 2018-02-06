drop function if exists ep_case_timezones(text,text,text,text,text,text,integer);

/***************************************************************
* This function creates and fills out the table
* tz_table (usually ep_timezones) for case tz_schema
* it imports/creates only needed country/generic timezones
***************************************************************/
create or replace function ep_case_timezones (
    grid_schema text,
    grid_table text,
    tz_world_schema text,
    tz_world_table text,
    tz_schema text,
    tz_table text,
    srid integer )
    returns boolean as
$$
declare
    xmin double precision;
    xmax double precision;
    ymin double precision;
    ymax double precision;
    domainpoly text;
    sqltext text;
    sqlinsert text;
    lonmin double precision;
    lonmax double precision;
    latmin double precision;
    latmax double precision;
    allcountries geometry;
    srid_wgs integer;
    --grid_gid integer;
    grid_geom geometry;
    geomls geometry;
    geomp1 geometry;
    geomp2 geometry;
    geoml1 geometry;
    geoml2 geometry;
    geomp geometry;
    lon1 double precision;
    lon2 double precision;
    lp1 double precision;
    lp2 double precision;
    iz1 integer;
    iz2 integer;
    izp integer;
    x1 double precision;
    x2 double precision;
    y1 double precision;
    y2 double precision;
    xp double precision;
    yp double precision;
    il integer;
    ib integer;
    ie integer;
    r double precision;
    tzname text;
    tzformat text;

--    grid_i integer;
--    grid_j integer;
begin
    srid_wgs = 4326;
--   raise notice 'Point1';
    -- create table ep_timezones
    execute format('create table if not exists  %I.%I (
                      tz_id serial,
                      tz_name text not null,
                      geom geometry(multipolygon, %L),
                      primary key (tz_id)
                     )', tz_schema, tz_table, srid);
--   raise notice 'Point2';
    -- create geom index
    execute format('create index if not exists %I on %I.%I using gist(geom)',
                    tz_table||'_geom', tz_schema, tz_table);
    --execute format('create index if not exists %I on %I.%I using gist(geom)',
     --               tz_table||'_geom', tz_schema, tz_table);
--   raise notice 'Point3';
    -- delete possible old rows
    execute format('delete from %I.%I', tz_schema, tz_table);
-- execute format('select count(*) from %I.%I', tz_schema, tz_table) into izp;
--   raise notice 'Point4 %', izp;

    -- calculate domain bounding box
    execute format('select ST_Multi(ST_ConvexHull(ST_Collect(geom))) from %I.%I', grid_schema, grid_table)
        into domainpoly;
--   raise notice 'Point 4a % %', ST_AsText(domainpoly), ST_SRID(domainpoly);

--    execute format('select min(ST_XMin("geom")), max(ST_XMax("geom")), min(ST_YMin("geom")), max(ST_YMax("geom"))
--                      from %I.%I', grid_schema, grid_table)
--        into xmin, xmax, ymin, ymax;
--  raise notice 'Point5';
--    domainpoly = format('POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s ))',
--                     xmin, ymin, xmin, ymax, xmax, ymax, xmax, ymin, xmin, ymin);
--  raise notice 'Point6 %', domainpoly;

    -- import relevant country data from tz_world_mp and transform into case srid
    -- and cut to domain extend
    execute format(
             'insert into %I.%I (tz_id, tz_name, geom)
                select "gid", "tz_id", ST_Multi(ST_Intersection(ST_Transform("geom",%s),%L)) from %I.%I
                  where "tz_id" <> %L and
                        ST_Intersects(ST_Transform(geom,%s), %L)',
           tz_schema,tz_table, srid, domainpoly, tz_world_schema, tz_world_table, 'uninhabited', srid, domainpoly);
           --ST_Intersects(ST_Transform(geom,%L),ST_PolygonFromText(%L,%L))
--   raise notice 'Point7';
    -- create generic timezones for
--    -- 1. create domain bounding box
--    execute format('select min(ST_XMin(ST_Transform("geom",%L))), max(ST_XMax(ST_Transform("geom",%L))),
--                           min(ST_YMin(ST_Transform("geom",%L))), max(ST_YMax(ST_Transform("geom",%L)))
--                       from %I.%I', srid_wgs, srid_wgs, srid_wgs, srid_wgs, grid_schema, grid_table)
--        into lonmin, lonmax, latmin, latmax;
--  raise notice 'Point8 %, %, %, %', lonmin, lonmax, latmin, latmax;

    -- 2. create union allcountries geometry
    execute format('select ST_Union(array(select geom from %I.%I))', tz_schema, tz_table)
        into allcountries;
--  raise notice 'Point9 % ', allcountries;

    -- 3. create generric timezones relevant for modelled domain
    -- 3a. create temporary table for transformed gridpoints
    --execute 'create table test.ep_grid_points (
    execute 'create temporary table ep_grid_points (
                  geom geometry(point),
                  tz integer,
                  gid integer)
                on commit drop';
                --  i integer,
                --  j integer)';
--   raise notice 'Point10';
    --sqlinsert = 'insert into ep_grid_points (geom, tz, gid,i,j) values ( $1, $2, $3, $4, $5 )';
    sqlinsert = 'insert into ep_grid_points (geom, tz) values ( $1, $2 )';
    --for grid_gid, grid_geom, grid_i,grid_j in execute format('select "gid", "geom", i,j from %I.%I', grid_schema, grid_table ) loop
    for grid_geom in execute format('select "geom" from %I.%I', grid_schema, grid_table ) loop
        geomls = (ST_Dump(ST_Boundary(grid_geom))).geom;
        for il in 2..ST_NPoints(geomls) loop
            geomp1 = ST_SetSRID(ST_PointN(geomls,il-1),srid);
            geomp2 = ST_SetSRID(ST_PointN(geomls,il),srid);
            geoml1 = ST_Transform(geomp1,srid_wgs);
            geoml2 = ST_Transform(geomp2,srid_wgs);
            lon1 = ST_X(geoml1);
            lon2 = ST_X(geoml2);
            iz1 = round(lon1/15.0);
            iz2 = round(lon2/15.0);
            lp1 = lon1/15.0-0.5;
            lp2 = lon2/15.0-0.5;
--   raise notice 'Geomp %, %, %, %, %, %, %, %, %, %', grid_gid, grid_i,grid_j, il, lon1, iz1, lp1, lon2, iz2, lp2;

            -- insert points p1,iz1 and p2,iz2
--   raise notice 'Point10b %, %, %, %, %, %', lon1, iz1, lp1, lon2, iz2, lp2;
            --execute sqlinsert using geomp1, iz1, grid_gid,grid_i,grid_j;
            execute sqlinsert using geomp1, iz1;

            -- if p1 or p2 lays on border oz tz, insert it with both tz
            -- round transforms 1.5 to 2 and -1.5 to -1 ...
            -- we need add asb lower value iz
            if lp1=round(lp1) then
--   raise notice 'Geomp1 %, %, %, %, %, %, %, %, %, %', grid_gid, grid_i,grid_j, il, lon1, iz1, lp1, lon2, iz2, lp2;
                --execute sqlinsert using geomp1, iz1-sign(iz1), grid_gid,grid_i,grid_j;
                execute sqlinsert using geomp1, iz1-sign(iz1);
            end if;

            -- if p1 and p2 belong to different tz insert also border points
            -- of tz on line connecting p1 and p2
            if iz1 <> iz2 then
--   raise notice 'Geompn1 %, %, %, %, %, %, %, %, %, %', grid_gid, grid_i,grid_j, il, lon1, iz1, lp1, lon2, iz2, lp2;

                for izp in least(ceil(lp1),ceil(lp2)) .. greatest(floor(lp1),floor(lp2)) loop
                    -- calculate longitude ratio
                    r = (izp-lp1)/(lp2-lp1);
                    -- calculate xp, yp
                    x1 = ST_X(geomp1);
                    y1 = ST_Y(geomp1);
                    x2 = ST_X(geomp2);
                    y2 = ST_Y(geomp2);
                    xp = x1 + r*(x2-x1);
                    yp = y1 + r*(y2-y1);
                    geomp = ST_SetSRID(ST_Point(xp,yp), srid);
--   raise notice 'Insert %, %, %, %, %, %', izp, lp1, lp2, r, xp, yp;
                    -- execute sqlinsert using geomp, izp, grid_gid,grid_i,grid_j;
                    execute sqlinsert using geomp, izp;
--   raise notice 'Insert %, %, %, %, %, %', izp, lp1, lp2, r, xp, yp;
                    -- execute sqlinsert using geomp, izp+1, grid_gid,grid_i,grid_j;
                    execute sqlinsert using geomp, izp+1;
                end loop;
            end if;

        end loop;
    end loop;

--    for geomp in select ST_Collect(geom) from ep_grid_points loop
--      raise notice 'Point11 %', ST_AsText(geomp);
--    end loop;

    -- create tz polygons wraping point which belongs to particular tz
    sqlinsert = 'insert into ep_grid_points (geom, tz) values ( $1, $2 )';
    tzformat = 'SG000';
    geomp = ST_AsText(ST_GeomFromText('POLYGON EMPTY'));
    for izp in select distinct tz from ep_grid_points order by tz loop
        for geomp1 in select ST_Multi(ST_ConvexHull(ST_Collect(geom))) from ep_grid_points where tz = izp loop
            if not ST_IsEmpty(geomp) then
                geomp1 = ST_MakeValid(ST_Difference(geomp1,geomp));
            end if;
            -- save geomp1 for next loop
            geomp = geomp1;
-- raise notice 'Multipoint % %', izp, ST_AsText(geomp1);
            geomp2 = ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_Difference(geomp1, allcountries)), 3));
            if not ST_IsEmpty(geomp2) then
                -- allcountries = ST_Union(allcountries,geomp2);
                if izp = 0 then
                    tzname = 'UTC';
                else
                    tzname = 'UTC'||trim(to_char(izp,tzformat));
                end if;
                execute format('insert into %I.%I (tz_id, tz_name, geom) values ( %L, %L, %L )', tz_schema, tz_table, izp+1000, tzname, geomp2);
            end if;
        end loop;
    end loop;



return true;


end
$$
language plpgsql volatile
cost 100;
commit;

