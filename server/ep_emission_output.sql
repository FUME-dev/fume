CREATE EXTENSION IF NOT EXISTS intarray;

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

DROP FUNCTION IF EXISTS ep_emiss_time_series(integer,integer,integer,integer[],timestamptz,text,boolean,boolean);
 
CREATE OR REPLACE FUNCTION ep_emiss_time_series(
    nx INTEGER,
    ny INTEGER,
    nz INTEGER,
    spec INTEGER[],
    t TIMESTAMPTZ,
    case_schema TEXT DEFAULT 'case',
    save_to_db BOOLEAN DEFAULT false,
    from_aggregated BOOLEAN DEFAULT false)
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
    sql TEXT;
begin
    nspec = icount(spec);
    -- prepare a zero array that will be filled up (time is not in dimension, this function will be called for each timestep)
    emis = ARRAY_FILL(0.0::FLOAT, ARRAY[nx,ny,nz,nspec]);
    
    IF from_aggregated THEN
       sql = FORMAT('SELECT i, j, em.k, em.spec_id s, sum(em.emiss*t.tv_factor) e 
                     FROM %I.ep_grid_emissions_spec em 
                     JOIN %I.ep_time_factors t USING(cat_id) 
                     JOIN %I.ep_grid_tz g USING(grid_id) 
                     JOIN %I.ep_time_zone_shifts s USING (tz_id, time_loc) 
                     WHERE s.time_out = $1
                     GROUP BY i, j, em.k, em.spec_id', 
                     case_schema, case_schema, case_schema, case_schema);
    ELSE
       sql = FORMAT('SELECT i, j, k, em.spec_id s, sum(em.emiss*t.tv_factor) e 
                     FROM %I.ep_sg_emissions_spec em 
                     JOIN %I.ep_time_factors t USING(cat_id) 
                     JOIN %I.ep_sources_grid sg USING(sg_id) 
                     JOIN %I.ep_grid_tz g USING(grid_id) 
                     JOIN %I.ep_time_zone_shifts s USING (tz_id, time_loc) 
                     WHERE s.time_out = $1 AND sg.source_type IN (''A'', ''L'')  
                     GROUP BY i, j, k, em.spec_id', 
                     case_schema, case_schema, case_schema, case_schema, case_schema);
    END IF;

    FOR i, j, k, s, e IN EXECUTE sql USING t
    LOOP
        pos = idx(spec, s);
        raise notice '%', s ;
        raise notice '%', spec;
        emis[i][j][k][pos] = e;
    END LOOP;

    IF save_to_db THEN
        EXECUTE FORMAT('INSERT INTO %I.ep_out_emissions_array (time_out, emissions) VALUES ($1, $2)', case_schema) USING t, emis;
    END IF;

    RETURN emis;
END;
$ep_emiss_time_series$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS ep_pemiss_time_series(integer[],integer[],timestamp,text, text);

CREATE OR REPLACE FUNCTION ep_pemiss_time_series(
    stacks INTEGER[],
    spec INTEGER[],
    t TIMESTAMP,
    source_schema TEXT DEFAULT 'sources',
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
                        JOIN %I.ep_time_zone_shifts s USING (tz_id, time_loc) 
                        WHERE s.time_out = $1 AND sg.source_type=''P'' 
                        GROUP BY sg.sg_id, em.spec_id', 
                        case_schema,  case_schema, case_schema, case_schema, case_schema ) USING t
    LOOP
        pos_spec =  idx(spec, s);
        pos_stk  =  idx(stacks, i);
        emis[pos_stk][pos_spec] = e;
    END LOOP;
    return emis;

END;
$$ LANGUAGE plpgsql;
