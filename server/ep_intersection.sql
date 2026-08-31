/*
Description: It creates ep_intersection FUME sql function.
*/

/*
This file is part of the FUME emission model.

FUME is free software: you can redistribute it and/or modify it under the terms of the GNU General
Public License as published by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

FUME is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

Information and source code can be obtained at www.fume-ep.org

Copyright 2014-2026 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
Copyright 2014-2026 Charles University, Faculty of Mathematics and Physics, Prague, Czech Republic
Copyright 2014-2026 Czech Hydrometeorological Institute, Prague, Czech Republic
Copyright 2014-2017 Czech Technical University in Prague, Czech Republic
*/

--drop function ep_intersection(text,text,text,text,text,text);

/*********************************************************************
* This function creates intersects of table1 and table2
* The key fields listed in arrays fields1/fields2 are copied from
* table1/table2 to tablei. The field named coefi contains portion
* of area/length of intersect to area/length of geometry in table1
* multiplied by optional coefficients stored in field of name
* coef1 and coef2. The intersect geometry is stored in field
* of name geomcoli. The output coefi is calculated from coef1 and coef2
* and if scale is true, it is scaled by area/length of the intersect geometry
* and in case of normaliz=true, also normalized by area/length of the
* original geometry. The value createtable denotes if a new table
* tablei shall be created and tempi if this table is created as temporary.
*********************************************************************/
create or replace function ep_intersection (
    schema1 text,
    table1 text,
    fields1 text[],
    coef1 text,
    schema2 text,
    table2 text,
    fields2 text[],
    coef2 text,
    schemai text,
    tablei text,
    sridi integer,
    idi text,
    geomcoli text,
    coefi text,
    scale boolean,
    normaliz boolean,
    createtable boolean,
    tempi boolean)
    returns boolean as
$$
declare
    tablename1 text;
    tablename2 text;
    tablenamei text;
    geomcol1 text;
    geomcol2 text;
    srid1 integer;
    srid2 integer;
    sridir integer;
    geomtype1 text;
    geomtype2 text;
    geomdim1 integer;
    geomdim2 integer;
    geomtypei text;
    geomdimi integer;

    field  text;
    ftype  text;
    flen   integer;

    geom1 geometry;
    geom2 geometry;
    geomext geometry;
    geomi geometry;
    gid1 integer;
    gid2 integer;
    gidi integer;

    i integer;
    j integer;
    sqltext text;
    sqltrans1 text;
    sqltrans2 text;
    sqlcoef text;
    sqldim text;
    sqlfields1 text;
    sqlcond1 text;
    sqlnorma text;
    sqlnorml text;

    rec1 text[];
    rec2 text[];
    firstrec boolean;

    k1 double precision;
    k2 double precision;
    ci double precision;

begin
    raise notice 'ep_intersection: %, %, %, %, %, %, %, %, %, %, %, %, %, %, %, %, %, %', schema1,table1,fields1,coef1,schema2,table2,fields2,coef2,schemai,tablei,sridi,idi,geomcoli,coefi,scale,normaliz,createtable,tempi;
    -- construct table full names
    if schema1 = '' then
        tablename1 = format('%I',table1);
    else
        tablename1 = format('%I.%I',schema1,table1);
    end if;
    if schema2 = '' then
        tablename2 = format('%I',table2);
    else
        tablename2 = format('%I.%I',schema2,table2);
    end if;
    if schemai = '' then
        tablenamei = format('%I',tablei);
    else
        tablenamei = format('%I.%I',schemai,tablei);
    end if;

    -- read geometry property for table1 and table2
    execute 'select f_geometry_column, coord_dimension, srid, type
        from public.geometry_columns
        where f_table_schema = $1 and f_table_name = $2'
        into geomcol1, geomdim1, srid1, geomtype1
        using schema1, table1;

    raise notice 'Geom1 %, %, %, %, %, %', schema1, table1, geomcol1, geomdim1, srid1, geomtype1;

    execute 'select f_geometry_column, coord_dimension, srid, type
        from public.geometry_columns
        where f_table_schema = $1 and f_table_name = $2'
        into geomcol2, geomdim2, srid2, geomtype2
        using schema2, table2;

    raise notice 'Geom2 %, %, %, %, %, %', schema2, table2, geomcol2, geomdim2, srid2, geomtype2;

    if geomtype2 <> 'POLYGON' and geomtype2 <> 'MULTIPOLYGON' then
        raise notice 'Second geometry have to be POLYGON or MULTIPOLYGON!';
        return false;
    end if;
    geomdimi = geomdim1;

    if sridi = 0 then
        sridi = srid1; -- sridi still can be 0
    end if;
    if sridi = 0 then
        raise notice 'Either the first geometry column needs to have assigned srid or the output srid must be set!';
        return false;
    end if;
    if srid2 = 0 then
        raise notice 'Second geometry column needs to have assigned srid!';
        return false;
    end if;

    if createtable then
        -- drop old intersect table
        sqltext = format('drop table if exists %s ', tablenamei);
        execute sqltext;

        -- create a new table for intersect
        if tempi then
            sqltext = 'create temp table';
        else
            sqltext = 'create table';
        end if;
        sqltext = sqltext||format(' %s (%I serial', tablenamei, idi);

        -- add id fields from table1 and table2
        foreach field in array fields1
        loop
            execute 'select data_type from information_schema.columns where table_schema = $1 and table_name = $2 and column_name = $3'
                using schema1, table1, field
                into ftype;
            raise notice 'Fields1: %, %, %, %', schema1, table1, field, ftype;
            sqltext = sqltext||format(', %I %s', field, ftype);
        end loop;
        foreach field in array fields2
        loop
            execute 'select data_type from information_schema.columns where table_schema = $1 and table_name = $2 and column_name = $3'
                using schema2, table2, field
                into ftype;
            raise notice 'Fields2: %, %, %, %', schema2, table2, field, ftype;
            sqltext = sqltext||format(', %I %s', field, ftype);
        end loop;

        -- add the new coefficient
        if coefi <> '' then
            sqltext = sqltext || format(', %I double precision', coefi);
        end if;

        -- add primary key
        sqltext = sqltext || format(', primary key (%I)', idi);

        if tempi then
            sqltext = sqltext || ' ) on commit drop';
        else
            sqltext = sqltext || ')';
        end if;
        raise notice 'Create: %', sqltext;
        execute sqltext;

        -- add geometry column
        if left(geomtype1,5) = 'MULTI' then
            geomtypei = geomtype1;
        elsif left(geomtype1,8) = 'GEOMETRY' then
            geomtypei = 'GEOMETRY';
        else
            geomtypei = 'MULTI'||geomtype1;
        end if;
        geomdimi = geomdim1;
        -- create geometry column
        raise notice 'Create geom: %, %, %, %, %, %', schemai, tablei, geomcoli, sridi, geomtypei, geomdimi;
        perform AddGeometryColumn(schemai, tablei, geomcoli, sridi, geomtypei, geomdimi);
    end if;

    -- construct sql commands for insert rows into intersect table
    sqltext = format('insert into %s (', tablenamei);

    -- insert geom column
    sqltext = sqltext||format('%I', geomcoli);
    -- insert field columns
    foreach field in array fields1
    loop
        sqltext = sqltext||format(',%I', field);
    end loop;
    foreach field in array fields2
    loop
        sqltext = sqltext||format(',%I', field);
    end loop;
    -- insert coefficient column
    if coefi <> '' then
        sqltext = sqltext||format(',%I', coefi);
    end if;

    -- select part
    sqltext = sqltext || ') select ';
    -- prepare transformation strings
    if srid1 = sridi then
        sqltrans1 = format('t1.%I', geomcol1);
    else
        sqltrans1 = format('ST_Transform(t1.%I,%s)', geomcol1, sridi);
    end if;
    if sridi = srid2 then
        sqltrans2 = format('t2.%I',geomcol2);
    else
        sqltrans2 = format('ST_Transform(t2.%I,%s)',geomcol2,sridi);
    end if;

    -- intersect geometry
    sqltext = sqltext ||' ST_Multi(ST_CollectionExtract(ST_Intersection('||sqltrans1||','||sqltrans2||'), 
    least(ST_Dimension('||sqltrans1||')+1, ST_Dimension('||sqltrans2||')+1))) ';

    -- field values
    foreach field in array fields1
    loop
        sqltext = sqltext||format(',t1.%I',field);
    end loop;

    foreach field in array fields2
    loop
        sqltext = sqltext||format(',t2.%I',field);
    end loop;

    -- coefficient
    if coefi <> '' then
        -- coef1 and coef2 multiplication string
        sqlcoef = '';
        if coef1 <> '' then
            sqlcoef = sqlcoef||format(' * t1.%I',coef1);
        end if;
        if coef2 <> '' then
            sqlcoef = sqlcoef||format(' * t2.%I',coef2);
        end if;

        -- coefi calculation
        if scale then
            -- dimmension of geometry
            sqldim = format('ST_Dimension(t1.%I)', geomcol1);
            -- normalization formulas
            if normaliz then
              sqlnorma = '/ST_Area('||sqltrans1||')';
              sqlnorml = '/ST_Length('||sqltrans1||')';
            else
              sqlnorma = '';
              sqlnorml = '';
            end if;
            sqltext = sqltext ||
                ', CASE
                     WHEN '||sqldim||' = 2 THEN
                       CASE
                         WHEN ST_Area('||sqltrans1||') > 0 THEN
                       ST_Area(ST_Intersection('||sqltrans1||','||sqltrans2||'))'||sqlnorma||sqlcoef||'
                         ELSE
                           0.0
                         END
                     WHEN '||sqldim||' = 1 THEN
                       CASE
                         WHEN ST_Length('||sqltrans1||') > 0 THEN
                       ST_Length(ST_Intersection('||sqltrans1||','||sqltrans2||'))'||sqlnorml||sqlcoef||'
                         ELSE
                           0.0
                       END
                     ELSE 1.0'||sqlcoef||'
                   END ';
        else
            -- case without area/length scaling
            sqltext = sqltext ||', 1.0'||sqlcoef;
        end if;
    end if;

    sqltext = sqltext || ' FROM ' || tablename1 || ' AS t1 ';
    sqltext = sqltext || ' JOIN ' || tablename2 || ' AS t2 ';
    sqltext = sqltext || ' ON ST_IsValid('||sqltrans1||') and ST_Intersects('||sqltrans1||','||sqltrans2||')';
    sqltext = sqltext || ' where ST_Dimension(ST_Multi(ST_Intersection('||sqltrans1||','||sqltrans2||'))) = ';
    sqltext = sqltext || ' least(ST_Dimension('||sqltrans1||'), ST_Dimension('||sqltrans2||'))';

    raise notice 'Intersect: %', sqltext;
    execute sqltext;

    -- normalize points on border of more table2 geometries -
    -- devide coefficient by number of intersecting geometries
    /*
    !!!!!!!!!!!!!!!!!!!
    It is neseccary to do it better - two adjacent polygon geometries can share
    one or more point from one multipoint geometry while they do not share other.
    We need to find common intersection points of new intersection geometries...
    !!!!!!!!!!!!!!!!!!!
    sqlfields1 = '';
    sqlcond1 = '';
    foreach field in array fields1
    loop
        if sqlfields1 <> '' then
            sqlfields1 = sqlfields1 || ',';
            sqlcond1 = sqlcond1 || ' AND ';
        end if;
        sqlfields1 = sqlfields1||format('%I', field);
        sqlcond1 = sqlcond1||format('bp.%I = %I', field, field);
    end loop;
    sqltext = '
    WITH bp AS (
        SELECT '||sqlfields1||' , count(*) as num
          FROM '||tablenamei||' i
          where ST_Dimension('||format('%I', geomcoli)||') = 1
          group by '||sqlfields1||'
          having count(*) > 1
    )
    UPDATE '||tablenamei||'
      SET '||format('%I',coefi)||' = '||format('%I',coefi)||'
          / (SELECT num FROM bp WHERE '||sqlcond1||')
    WHERE EXISTS (SELECT geom_id FROM bp WHERE '||sqlcond1||')';
    raise notice 'Normalize points: %', sqltext;
    execute sqltext;
    */

    if createtable then
        -- create geometry index
        sqltext = format('create index if not exists %I on %s using gist (%I)', tablei||'_'||geomcoli, tablenamei, geomcoli);
        execute sqltext;

        -- create fields indices
        /*
        foreach field in array fields1 loop
            sqltext = format('create index if not exists %I on %I (%I)', tablei||'_1_'||field, tablenamei, field);
        execute sqltext;
        foreach field in array fields2 loop
            sqltext = format('create index if not exists %I on %I (%I)', tablei||'_2_'||field, tablenamei, field);
        execute sqltext;
        */
    end if;

    -- recompile statistics
    sqltext = format('analyze %s', tablenamei);
    execute sqltext;

    return true;

end
$$
language plpgsql volatile
cost 100;

commit;
/******************************************/

