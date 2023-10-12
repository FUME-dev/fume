/*
Description: It creates ep_surrogates FUME sql function.
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

--drop function ep_surrogates(text,text);

/*********************************************************************
* This function checks raw surrogates geometries for overlappings and
* corrects them.
*********************************************************************/
create or replace function ep_surrogates_check (
    schema1 text,
    table1 text)
    returns boolean as
$$
declare
    tablename text;
    geomcol text;
    pk1 integer;
    geom1 geometry;
    geom2 geometry;
    geomu geometry;
    sqltext1 text;
    sqltext2 text;
    sqltextu text;
    sqltextd text;
    isempty boolean;
    sqltextn text;
    t1 text;
    t2 text;

begin
    raise notice 'ep_surrogates_check: %, %', schema1,table1;
    -- construct table full names
    if schema1 = '' then
        tablename = format('%I',table1);
    else
        tablename = format('%I.%I',schema1,table1);
    end if;

    -- read geometry property for table1
    execute 'select f_geometry_column
        from public.geometry_columns
        where f_table_schema = $1 and f_table_name = $2'
        into geomcol
        using schema1, table1;

    raise notice 'Geom column %, %, %', schema1, table1, geomcol;

    sqltext1 = format('select gid, %I from %s', geomcol, tablename);
    raise notice 'Sqltext1: %', sqltext1;
    sqltext2 = format('select %I from %s where ST_Intersects(%I, $1) and gid<>$2',
                                geomcol, tablename, geomcol);
    raise notice 'Sqltext2: %', sqltext2;
    sqltextu = format('update %s set %I = $1 where gid = $2', tablename, geomcol);
    raise notice 'Sqltextu: %', sqltextu;
    sqltextd = 'select ST_MakeValid(ST_Multi(ST_CollectionExtract(ST_Difference($1, $2))))';
    raise notice 'Sqltextd: %', sqltextd;
    for pk1, geom1 in execute sqltext1
    loop
        geomu = geom1;
        for geom2 in execute sqltext2 using geom1, pk1
        loop
            execute sqltextd  using geomu, geom2 into geomu;
            execute 'select ST_IsEmpty($1)' using geomu into isempty;
            if isempty then
                execute 'select ST_GeomFromText(''MULTIPOLYGON(EMPTY)'')' into geomu;
                exit;
            end if;
        end loop;
        execute sqltextu using geomu, pk1;
    end loop;

    -- recompile statistics
    execute format('analyze %s', tablename);
    return true;
end
$$
language plpgsql volatile
cost 100;

