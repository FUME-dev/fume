/*
Description: It creates ep_limit_to_mask FUME sql function.
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

--drop function ep_limit_to_mask(text,text,text[],text,text,text,text,text,text,text,text,text,boolean,boolean);

/*********************************************************************
* This function limits table1 to geometries which intersects with
* union of geometries from table2.
*********************************************************************/
create or replace function ep_limit_to_mask (
    schema1 text,
    table1 text,
    fields1 text[],
    coef1 text,
    schema2 text,
    table2 text,
    schemai text,
    tablei text,
    sridi integer,
    idi text,
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

    field  text;
    ftype  text;

    maskenv geometry;

    sqltext text;
    sqltext2 text;
    pkfields text;
    pkfields_array text[];
    pkf text;

begin
    raise notice 'ep_limit_to_mask: %, %, %, %, %, %, %, %, %, %, %, %', schema1,table1,fields1,coef1,schema2,table2,schemai,tablei,sridi,idi,createtable,tempi;
    -- construct table full names
    if schema1 = '' then
        tablename1 = format('"%I"',table1);
    else
        tablename1 = format('"%I"."%I"',schema1,table1);
    end if;
    raise notice 'tablename1: %', tablename1;
    if schema2 = '' then
        tablename2 = format('"%I"',table2);
    else
        tablename2 = format('"%I"."%I"',schema2,table2);
    end if;
    raise notice 'tablename2: %', tablename2;
    if schemai = '' then
        tablenamei = format('"%I"',tablei);
    else
        tablenamei = format('"%I"."%I"',schemai,tablei);
    end if;
    raise notice 'tablenamei: %', tablenamei;

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
        sqltext = sqltext||format(' %s ("%I" serial', tablenamei, idi);

        -- add id fields from table1
        foreach field in array fields1
        loop
            execute 'select data_type from information_schema.columns where table_schema = $1 and table_name = $2 and column_name = $3'
                using schema1, table1, field
                into ftype;
            sqltext = sqltext||format(', "%I" %s', field, ftype);
        end loop;

        -- add the coefficient
        if coef1 <> '' then
            sqltext = sqltext || format(', "%I" double precision', coef1);
        end if;

        -- add primary key
        sqltext = sqltext || format(', primary key ("%I")', idi);

        if tempi then
            sqltext = sqltext || ' ) on commit drop';
        else
            sqltext = sqltext || ')';
        end if;
        raise notice 'Create: %', sqltext;
        execute sqltext;

        -- create geometry column
        perform AddGeometryColumn(schemai, tablei, geomcol1, sridi, geomtype1, geomdim1);

    end if;

    -- retrieve grid envelope geometry
    sqltext = format('SELECT ST_Transform(ST_UNION("%I"), %s) FROM %s', geomcol2, sridi, tablename2);
    raise notice 'grid envelope sqltext: %', sqltext;
    execute sqltext into maskenv;
    raise notice 'maskenv %', maskenv;

    -- construct sql commands for insert rows into intersect table
    sqltext = format('insert into %s (', tablenamei);

    -- insert geom column
    sqltext = sqltext||format('"%I"', geomcol1);

    -- insert field columns
    foreach field in array fields1
    loop
        sqltext = sqltext||format(',"%I"', field);
    end loop;

    if coef1 <> '' then
        sqltext = sqltext||format(',"%I"', coef1);
    end if;

    -- select part of insert query
    sqltext = sqltext || ') select ';

    -- object geometry
    if sridi <> srid1 then
        sqltext = sqltext || format('ST_Transform("%I", %s)',  geomcol1, sridi);
    else
        sqltext = sqltext || format('"%I"',  geomcol1);
    end if;

    -- field values
    foreach field in array fields1
    loop
        sqltext = sqltext||format(',"%I"',field);
    end loop;

    if coef1 <> '' then
        sqltext = sqltext||format(',"%I"', coef1);
    end if;

    sqltext = sqltext || format(' FROM %s ', tablename1);

    -- where clausule
    if sridi <> srid1 then
        sqltext = sqltext || format('WHERE ST_Intersects(ST_Transform("%I", %s), $1::geometry)',  geomcol1, sridi);
    else
        sqltext = sqltext || format('WHERE ST_Intersects("%I", $1::geometry)',  geomcol1);
    end if;

    raise notice 'Mask sqltext: %, %', sqltext, maskenv;
    execute sqltext using maskenv;

    if createtable then
        -- create geometry index
        sqltext = format('create index "%I" on %s using gist ("%I")', tablei||'_'||geomcol1, tablenamei, geomcol1);
        execute sqltext;
    end if;

    -- recompile statistics
    sqltext = format('analyze %s', tablenamei);
    execute sqltext;

    return true;

end
$$
language plpgsql volatile
cost 100;

