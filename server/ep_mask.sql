/*
Description: It creates ep_mask FUME sql function.
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

Copyright 2014-2023 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
Copyright 2014-2023 Charles University, Faculty of Mathematics and Physics, Prague, Czech Republic
Copyright 2014-2023 Czech Hydrometeorological Institute, Prague, Czech Republic
Copyright 2014-2017 Czech Technical University in Prague, Czech Republic
*/

--drop function (text,text,text[],text,text,text,text,text,text,text,text,text,boolean,boolean);

/*********************************************************************
* This function masks table1 by union of geometries from table2
* which satisfy the conditions given by mask_filter. The parameter
* mask_function can have values "ST_Union" or "ST_Difference".
* The geometries are masked to the interior or the exterior of
* union of mask geometries respectively.
*********************************************************************/
create or replace function ep_mask (
    schema1 text,
    table1 text,
    fields1 text[],
    coef1 text,
    schema2 text,
    table2 text,
    mask_filter text,
    mask_function text,
    schemai text,
    tablei text,
    sridi integer,
    idi text,
    coefi text,
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
    geomtype1 text;
    geomtype2 text;
    geomdim1 integer;
    geomdim2 integer;
    geomtypei text;
    maskdim integer;

    field  text;
    ftype  text;

    geommask geometry;

    sqltext text;
    sqltext2 text;
    sqlcoef text;
    sqldim text;
    sqlfields1 text;
    ex boolean;

begin
    -- construct table full names
    if schema1 = '' then
        tablename1 = format('%I',table1);
    else
        tablename1 = format('%I.%I',schema1,table1);
    end if;
    raise notice 'tablename1: %', tablename1;
    if schema2 = '' then
        tablename2 = format('%I',table2);
    else
        tablename2 = format('%I.%I',schema2,table2);
    end if;
    raise notice 'tablename2: %', tablename2;
    if schemai = '' then
        tablenamei = format('%I',tablei);
    else
        tablenamei = format('%I.%I',schemai,tablei);
    end if;
    raise notice 'tablenamei: %', tablenamei;

    -- read geometry property for table1 and table2
    execute 'select f_geometry_column, coord_dimension, srid, type
        from public.geometry_columns
        where f_table_schema = $1 and f_table_name = $2'
        into geomcol1, geomdim1, srid1, geomtype1
        using schema1, table1;

    -- geomdim1 is incorrect, it is cardiality of coordinates
    -- read it as dimension of the first geometry in table
    sqltext = format('select exists(select %I from %s limit 1)', geomcol1, tablename1);
    execute sqltext into ex;
    if ex then
      sqltext = format('select ST_Dimension(%I) from %s limit 1', geomcol1, tablename1);
      execute sqltext into geomdim1;
    end if;
    raise notice 'Geom1 %, %, %, %, %, %', schema1, table1, geomcol1, geomdim1, srid1, geomtype1;

    execute 'select f_geometry_column, coord_dimension, srid, type
        from public.geometry_columns
        where f_table_schema = $1 and f_table_name = $2'
        into geomcol2, geomdim2, srid2, geomtype2
        using schema2, table2;

    raise notice 'Geom2 %, %, %, %, %, %', schema2, table2, geomcol2, geomdim2, srid2, geomtype2;

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

        -- add primary key field
        sqltext = sqltext||format(' %s (%I serial', tablenamei, idi);

        -- add id fields from table1
        foreach field in array fields1
        loop
            execute 'select data_type from information_schema.columns where table_schema = $1 and table_name = $2 and column_name = $3'
                using schema1, table1, field
                into ftype;
            sqltext = sqltext||format(', %I %s', field, ftype);
        end loop;

        -- add the coefficient field
        sqltext = sqltext || format(', %I double precision', coefi);

        -- add primary key
        sqltext = sqltext || format(', primary key (%I)', idi);

        -- get primary key fields from table1
        /*
        sqltext2 = format('SELECT string_agg(a.attname, %L)
                            FROM   pg_index i
                            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                              AND a.attnum = ANY(i.indkey)
                            WHERE  i.indrelid = %L::regclass AND i.indisprimary',
                          ',', tablename1);
        raise notice 'Mask geom: %', sqltext2;
        execute sqltext2 into sqlfields1;
        if ( sqlfields1 is not NULL ) then
            raise notice 'PK sqlfields1: %', sqlfields1;
            sqltext = sqltext || format(', primary key (%I)', sqlfields1);
        end if;
        */

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

        -- sridi
        if sridi = 0 then
            sridi = srid1;
        end if;

        -- create geometry column
        -- raise notice 'AddGeometryColumn: %, %, %, %, %, %', schemai, tablei, geomcol1, sridi, geomtypei, geomdim1;
        -- perform AddGeometryColumn(schemai, tablei, geomcol1, sridi, geomtypei, geomdim1);
        sqltext = format('ALTER TABLE %s ADD COLUMN %I geometry(%s,%s)', tablenamei, geomcol1, geomtypei, sridi);
        raise notice 'Add geometry: %', sqltext;
        execute sqltext;

    end if;

    -- retrieve mask geometry
    if (sridi <> 0 and sridi <> srid1) then
        sqltext = format('SELECT ST_Transform(ST_UNION(%I), %s) FROM %s', geomcol2, sridi, tablename2 );
    else
        sqltext = format('SELECT ST_UNION(%I) FROM %s', geomcol2, tablename2 );
    end if;
    if ( mask_filter is not NULL ) and ( mask_filter <> '' ) then
        sqltext =  sqltext || format(' WHERE %s', mask_filter);
    end if;
    raise notice 'geommask sqltext: %', sqltext;
    execute sqltext into geommask;
    --raise notice 'geommask %', geommask;
    -- check dimension of the mask geometry
    execute 'SELECT ST_Dimension($1)' into maskdim using geommask;
    if ( maskdim < 2 ) then
        raise notice 'Retrieved mask is not multipolygon and has dimension %. Do you really want this?', maskdim;
    end if;

    -- construct sql commands for insert rows into intersect table
    sqltext = format('insert into %s (', tablenamei);

    -- insert geom column
    sqltext = sqltext||format('%I', geomcol1);
    -- insert field columns
    foreach field in array fields1
    loop
        sqltext = sqltext||format(',%I', field);
    end loop;
    -- insert coefficient column
    sqltext = sqltext||format(',%I', coefi);

    -- select part of insert query
    sqltext = sqltext || ') select ';

    -- intersect geometry
    sqltext = sqltext || format('ST_Multi(ST_CollectionExtract(%s(%I,$1),$2)) ',  mask_function, geomcol1);

    -- field values
    foreach field in array fields1
    loop
        sqltext = sqltext||format(',%I',field);
    end loop;

    -- coefficient
    -- dimmension of geometry
    --sqldim = format('ST_Dimension(%I)', geomcol1);
    -- geomdim1

    -- coef1 multiplication string
    sqlcoef = '';
    if coef1 is not NULL and coef1 <> '' then
        sqlcoef = sqlcoef||format(' * %I', coef1);
    end if;

    -- coefi calculation
    sqltext = sqltext || format(
        ', CASE
             WHEN %s = 2 THEN
               CASE
                 WHEN ST_Area(%I) > 0 THEN
                   ST_Area(%s(%I,$1))/ST_Area(%I) %s
                 ELSE
                   0.0
                 END
             WHEN %s = 1 THEN
               CASE
                 WHEN ST_Length(%I) > 0 THEN
                   ST_Length(%s(%I,$1))/ST_Length(%I) %s
                 ELSE
                   0.0
                 END
             ELSE 1.0 %s
        END ',
        geomdim1, geomcol1, mask_function, geomcol1, geomcol1, sqlcoef,
        geomdim1, geomcol1, mask_function, geomcol1, geomcol1, sqlcoef, sqlcoef);
    
    sqltext = sqltext || format(' FROM %s ', tablename1);
    sqltext = sqltext || format(' WHERE %s(%I,$1) IS NOT NULL ', mask_function, geomcol1);

    raise notice 'Mask sqltext: %', sqltext;
    -- parameter type in ST_CollectionExtract corresponds to geomdim+1
    execute sqltext using geommask, geomdim1+1;

    if createtable then
        -- create geometry index
        sqltext = format('create index %I on %s using gist (%I)', tablei||'_'||geomcol1, tablenamei, geomcol1);
        execute sqltext;

        -- create fields indices
        /*
        foreach field in array fields1 loop
            sqltext = format('create index %I on %I (%I)', tablei||'_1_'||field, tablenamei, field);
        execute sqltext;
        foreach field in array fields2 loop
            sqltext = format('create index %I on %I (%I)', tablei||'_2_'||field, tablenamei, field);
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

