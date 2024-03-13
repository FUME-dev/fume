/*
Description: It creates FUME output sql functions.
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

CREATE EXTENSION IF NOT EXISTS intarray;

/* This may not get used in the end... */

DROP FUNCTION IF EXISTS ep_emissions_for_species_and_category(integer, integer, integer, integer, bigint, text);
 
CREATE OR REPLACE FUNCTION ep_emissions_for_species_and_category(
    nx INTEGER,
    ny INTEGER,
    nz INTEGER,
    spec_id INTEGER,
    cat_id BIGINT,
    case_schema TEXT DEFAULT 'case')
    RETURNS FLOAT[][][] AS 
$ep_emissions_for_species_and_category$
DECLARE
    emis FLOAT[][][];
    i INTEGER;
    j INTEGER;
    k INTEGER;
    e FLOAT;
begin
    -- prepare a zero array that will be filled up
    emis = ARRAY_FILL(0.0::FLOAT, ARRAY[nz, ny, nx]);

    FOR i, j, k, e IN
       EXECUTE  FORMAT('SELECT i, j, k, sum(em.emiss) e 
                        FROM %I.ep_sg_emissions_spec em 
                        JOIN %I.ep_sources_grid sg USING(sg_id) 
                        JOIN %I.ep_grid_tz g USING(grid_id) 
                        WHERE em.spec_id=$1 AND em.cat_id=$2
                        GROUP BY i, j, k',
                        case_schema, case_schema, case_schema)
                    USING spec_id, cat_id
    LOOP
        emis[k][j][i] = e;
    END LOOP;
    RETURN emis;
END;
$ep_emissions_for_species_and_category$ LANGUAGE plpgsql;

/* Most likely will drop this altogether, with large domains or many species/categories won't fit within PostgreSQL memory limits */

DROP FUNCTION IF EXISTS ep_emissions_by_species_and_category(integer, integer, integer, integer[], bigint[], text);
 
CREATE OR REPLACE FUNCTION ep_emissions_by_species_and_category(
    nx INTEGER,
    ny INTEGER,
    nz INTEGER,
    spec INTEGER[],
    cat BIGINT[],
    case_schema TEXT DEFAULT 'case')
    RETURNS FLOAT[][][][] AS 
$ep_emissions_by_species_and_category$
DECLARE
    nspec INTEGER;
    ncat INTEGER;
    emis FLOAT[][][][][];
    i INTEGER;
    j INTEGER;
    k INTEGER;
    s INTEGER;
    c BIGINT;
    e FLOAT;
    spec_idx INTEGER;
    cat_idx BIGINT;
begin
    nspec = icount(spec);
    ncat = icount(cat::INTEGER[]);
    -- prepare a zero array that will be filled up
    emis = ARRAY_FILL(0.0::FLOAT, ARRAY[nx, ny, nz, nspec, ncat]);

    FOR i, j, k, s, c, e IN
       EXECUTE  FORMAT('SELECT i, j, k, em.spec_id s, em.cat_id c, sum(em.emiss) e 
                        FROM %I.ep_sg_emissions_spec em 
                        JOIN %I.ep_sources_grid sg USING(sg_id) 
                        JOIN %I.ep_grid_tz g USING(grid_id) 
                        GROUP BY i, j, k, em.spec_id, em.cat_id', 
                        case_schema, case_schema, case_schema)
    LOOP
        spec_idx = idx(spec, s);
        cat_idx = idx(cat::INTEGER[], c::INTEGER);
        emis[i][j][k][spec_idx][cat_idx] = e;
    END LOOP;
    RETURN emis;
END;
$ep_emissions_by_species_and_category$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS ep_total_emissions(integer,integer,integer,integer[],text);
 
CREATE OR REPLACE FUNCTION ep_total_emissions(
    nx INTEGER,
    ny INTEGER,
    nz INTEGER,
    spec INTEGER[],
    case_schema TEXT DEFAULT 'case')
    RETURNS FLOAT[][][][] AS 
$ep_total_emissions$
DECLARE
    nspec INTEGER;
    emis FLOAT[][][][];
    i INTEGER;
    j INTEGER;
    k INTEGER;
    s INTEGER;
    e FLOAT;
    pos INTEGER;
begin
    nspec = icount(spec);
    -- prepare a zero array that will be filled up
    emis = ARRAY_FILL(0.0::FLOAT, ARRAY[nx,ny,nz,nspec]);
    
    FOR i, j, k, s, e IN
       EXECUTE  FORMAT('SELECT i, j, k, em.spec_id s, sum(em.emiss) e 
                        FROM %I.ep_sg_emissions_spec em 
                        JOIN %I.ep_sources_grid sg USING(sg_id) 
                        JOIN %I.ep_grid_tz g USING(grid_id) 
                        GROUP BY i, j, k, em.spec_id', 
                        case_schema, case_schema, case_schema)
    LOOP
        pos = idx(spec, s);
        emis[i][j][k][pos] = e;
    END LOOP;
    RETURN emis;
END;
$ep_total_emissions$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS ep_emiss_time_series(integer,integer,integer,integer[],timestamptz,text,boolean);
 
CREATE OR REPLACE FUNCTION ep_emiss_time_series(
    nx INTEGER,
    ny INTEGER,
    nz INTEGER,
    spec INTEGER[],
    t TIMESTAMPTZ,
    case_schema TEXT DEFAULT 'case',
    save_to_db BOOLEAN DEFAULT false)
    RETURNS FLOAT[][][][] AS 
$ep_emiss_time_series$
DECLARE
    nspec INTEGER;
    emis FLOAT[][][][];
    i INTEGER;
    j INTEGER;
    k INTEGER;
    s INTEGER;
    e FLOAT;
    pos INTEGER;
begin
    nspec = icount(spec);
    -- prepare a zero array that will be filled up (time is not in dimension, this function will be called for each timestep)
    emis = ARRAY_FILL(0.0::FLOAT, ARRAY[nx,ny,nz,nspec]);
    
    FOR i, j, k, s, e IN
       EXECUTE  FORMAT('SELECT i, j, k, em.spec_id s, sum(em.emiss*t.tv_factor) e 
                        FROM %I.ep_sg_emissions_spec em 
                        JOIN %I.ep_time_factors t USING(cat_id) 
                        JOIN %I.ep_sources_grid sg USING(sg_id) 
                        JOIN %I.ep_grid_tz g USING(grid_id)
                        JOIN %I.ep_timezones z USING (tz_id)
                        JOIN %I.ep_time_zone_shifts s USING (ts_id, time_loc)
                        WHERE s.time_out = $1 AND sg.source_type IN (''A'', ''L'')  
                        GROUP BY i, j, k, em.spec_id', 
                        case_schema, case_schema, case_schema, case_schema, case_schema, case_schema) USING t
    LOOP
        pos = idx(spec, s);
        emis[i][j][k][pos] = e;
    END LOOP;
    IF save_to_db THEN
        EXECUTE FORMAT('INSERT INTO %I.ep_out_emissions_array (time_out, emissions) VALUES ($1, $2)', case_schema) USING t, emis;
    END IF;
    RETURN emis;
END;
$ep_emiss_time_series$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS ep_emiss_time_series_1(integer,integer,integer,integer,timestamptz,text);
 
CREATE OR REPLACE FUNCTION ep_emiss_time_series_1(
    nx INTEGER,
    ny INTEGER,
    nz INTEGER,
    spec INTEGER,
    t TIMESTAMPTZ,
    case_schema TEXT DEFAULT 'case')
    RETURNS FLOAT[][][] AS 
$ep_emiss_time_series_1$
DECLARE
    emis FLOAT[][][];
    i INTEGER;
    j INTEGER;
    k INTEGER;
    s INTEGER;
    e FLOAT;
begin
    
    -- prepare a zero array that will be filled up (time is not in dimension, this function will be called for each timestep)
    emis = ARRAY_FILL(0.0::FLOAT, ARRAY[nx,ny,nz]);
    
    FOR i, j, k, e IN
       EXECUTE  FORMAT('SELECT i, j, k, sum(em.emiss*t.tv_factor) e 
                        FROM %I.ep_sg_emissions_spec em 
                        JOIN %I.ep_time_factors t USING(cat_id) 
                        JOIN %I.ep_sources_grid sg USING(sg_id) 
                        JOIN %I.ep_grid_tz g USING(grid_id)
                        JOIN %I.ep_timezones z USING (tz_id)
                        JOIN %I.ep_time_zone_shifts s USING (ts_id, time_loc)
                        WHERE s.time_out = $1 AND em.spec_id = $2 AND sg.source_type IN (''A'', ''L'')   
                        GROUP BY i, j, k', 
                        case_schema, case_schema, case_schema, case_schema, case_schema) USING t, spec
    LOOP
        
        emis[i][j][k] = e;
    END LOOP;
    RETURN emis;
END;
$ep_emiss_time_series_1$ LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS ep_pemiss_time_series(integer[],integer[],timestamp,text);

CREATE OR REPLACE FUNCTION ep_pemiss_time_series(
    stacks INTEGER[],
    spec INTEGER[],
    t TIMESTAMP,
    case_schema TEXT DEFAULT 'case')
    RETURNS FLOAT[][] AS 
$$
DECLARE
    nspec INTEGER;
    numstk INTEGER;
    emis FLOAT[][];
    i INTEGER;
    s INTEGER;
    e FLOAT;
    pos_spec INTEGER;
    pos_stk INTEGER;
begin
    nspec = icount(spec);
    numstk = icount(stacks);

    -- prepare a zero array that will be filled up (time is not in dimension, this function will be called for each timestep)
    emis = ARRAY_FILL(0.0::FLOAT, ARRAY[numstk,nspec]);
    
    --create temporary table nazev_tabulky (sg_id integer, pos integer, primary key (sg_id));
    FOR i, s, e IN
        EXECUTE FORMAT('SELECT sg.sg_id i, em.spec_id s , sum(em.emiss*t.tv_factor) e 
                        FROM %I.ep_sg_emissions_spec em 
                        JOIN %I.ep_time_factors t  USING(cat_id) 
                        JOIN %I.ep_sources_grid sg USING(sg_id) 
                        JOIN %I.ep_grid_tz g       USING(grid_id)
                        JOIN %I.ep_timezones z     USING (tz_id)
                        JOIN %I.ep_time_zone_shifts s USING (ts_id, time_loc)
                        WHERE s.time_out = $1 AND sg.source_type=''P'' 
                        GROUP BY sg.sg_id, em.spec_id
                        ORDER BY sg.sg_id, em.spec_id',
                        case_schema,  case_schema, case_schema, case_schema, case_schema, case_schema ) USING t
    LOOP
        pos_spec =  idx(spec, s);
        pos_stk  =  idx(stacks, i);
        emis[pos_stk][pos_spec] = e;
    END LOOP;
    return emis;

END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS ep_pemiss_time_series_ij(integer[],integer[],timestamp,text);

CREATE OR REPLACE FUNCTION ep_pemiss_time_series_ij(
    cat INTEGER[],
    spec INTEGER[],
    t TIMESTAMP,
    case_schema TEXT DEFAULT 'case')
    RETURNS FLOAT[][][][] AS
$$
DECLARE
    ncat INTEGER;
    nspec INTEGER;
    i1 INTEGER;
    i2 INTEGER;
    j1 INTEGER;
    j2 INTEGER;
    m INTEGER;
    n INTEGER;
    emis FLOAT[][][][];
    i INTEGER;
    j INTEGER;
    s INTEGER;
    c INTEGER;
    e FLOAT;
    pos_cat INTEGER;
    pos_spec INTEGER;
begin
    --raise notice 'ep_pemiss_time_series_ij(%,%, %)', spec, t, case_schema;
    -- find extend of the domain and number of species
    execute format('select min(i), max(i), min(j), max(j) from %I.ep_grid_tz', case_schema) into i1, i2, j1, j2;
    m = i2-i1+1;
    n = j2-j1+1;
    ncat = icount(cat);
    nspec = icount(spec);
    --raise notice 'm, n, nspec: %, %, %', m, n, nspec;
    -- prepare a zero array that will be filled up (time is not in dimension, this function will be called for each timestep)
    emis = ARRAY_FILL(0.0::FLOAT, ARRAY[m,n,ncat,nspec]);

    --create temporary table nazev_tabulky (sg_id integer, pos integer, primary key (sg_id));
    FOR i, j, s, c, e IN
        EXECUTE FORMAT('SELECT g.i-1 i, g.j-1 j, em.spec_id s, em.cat_id c, sum(em.emiss * t.tv_factor) e
                        FROM %I.ep_sg_emissions_spec em
                        JOIN %I.ep_sources_grid sg USING(sg_id)
                        JOIN %I.ep_time_factors t USING(cat_id)
                        JOIN %I.ep_grid_tz g USING(grid_id)
                        JOIN %I.ep_timezones z USING (tz_id)
                        JOIN %I.ep_time_zone_shifts s USING(ts_id, time_loc)
                        LEFT OUTER JOIN %I.ep_transformation_chains_levels chl ON sg.transformation_chain=chl.chain_id
                        WHERE s.time_out = $1 AND sg.source_type = ''P'' AND chl.vertical_level IS NULL
                        GROUP BY g.i, g.j, em.spec_id, em.cat_id
                        ORDER BY g.i, g.j, em.spec_id, em.cat_id',
                        case_schema,  case_schema, case_schema, case_schema, case_schema, case_schema, case_schema ) USING t
    LOOP
        pos_cat =  idx(cat, c);
        pos_spec =  idx(spec, s);
        --raise notice 'i,j,spec,category,pos_spec,emiss: %, %, %, %, %',i,j,s,c,e;
        emis[i][j][pos_cat][pos_spec] = e;
    END LOOP;
    return emis;

END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS ep_pemiss_time_series_vsrc(integer[],integer[],text);

CREATE OR REPLACE FUNCTION ep_pemiss_time_series_vsrc(
    cat INTEGER[],
    spec INTEGER[],
    case_schema TEXT DEFAULT 'case')
    RETURNS FLOAT[][][][] AS
$$
DECLARE
    ncat INTEGER;
    nspec INTEGER;
    emis FLOAT[][][][];
    i INTEGER;
    j INTEGER;
    s INTEGER;
    c INTEGER;
    e FLOAT;
    pos_cat INTEGER;
    pos_spec INTEGER;
begin
    --raise notice 'ep_pemiss_time_series_vsrc(%,%, %)', spec, case_schema;
    -- prepare a zero array that will be filled up (time is not in dimension, this function will be called for each timestep)
    emis = ARRAY_FILL(0.0::FLOAT, ARRAY[m,n,ncat,nspec]);

    --create temporary table nazev_tabulky (sg_id integer, pos integer, primary key (sg_id));
    FOR i, j, s, c, e IN
        EXECUTE FORMAT('SELECT g.i i, g.j j, em.spec_id s, em.cat_id c, sum(em.emiss) e
                        FROM %I.ep_sg_emissions_spec em
                        JOIN %I.ep_sources_grid sg USING(sg_id)
                        JOIN %I.ep_grid_tz g USING(grid_id)
                        WHERE sg.source_type = ''P''
                        GROUP BY g.i, g.j, em.spec_id, em.cat_id
                        ORDER BY g.i, g.j, em.spec_id, em.cat_id',
                        case_schema,  case_schema, case_schema )
    LOOP
        pos_cat =  idx(cat, c);
        pos_spec =  idx(spec, s);
        --raise notice 'i,j,spec,category,pos_spec,emiss: %, %, %, %, %',i,j,s,c,e;
        emis[i][j][pos_cat][pos_spec] = e;
    END LOOP;
    return emis;

END;
$$ LANGUAGE plpgsql;
