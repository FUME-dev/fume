/*
Description: SQL functions related to processing of the case.
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

DROP AGGREGATE IF EXISTS mul(float);
CREATE AGGREGATE mul(float) ( SFUNC = float8mul, STYPE=float );

CREATE OR REPLACE FUNCTION ep_calc_emiss_time_series(time_start TIMESTAMP WITH TIME ZONE,
                                                  time_increment INTERVAL,
                                                  num_times INTEGER,
                                                  --itzone_out INTEGER DEFAULT 0,
                                                  conf_schema VARCHAR DEFAULT 'conf',
                                                  case_schema VARCHAR DEFAULT 'case')
                                              RETURNS VOID AS
$$
DECLARE
   cat_id BIGINT;
   tv_cat_id BIGINT;
   par_id BIGINT;
   time_cur TIMESTAMP WITH TIME ZONE;
   time_step INTEGER := 0;
   num_records INTEGER := 0;
   year INTEGER;
   month INTEGER;
   dow INTEGER; -- day of week
   hour INTEGER;
   tz_id INTEGER;
   tz_name TEXT;
   --tzone_out TEXT;
   time_out TIMESTAMP WITH TIME ZONE;
   time_loc TIMESTAMP;
   ts_id INTEGER;
   time_out_array TIMESTAMP WITH TIME ZONE ARRAY;
   sql_text TEXT;
BEGIN
    -- First, prepare a cache table of all mappings:
    --        copy existing time_var_mappings to the cache table time_var_mapping_all
    --        find all categories for which no time_var_mappings_exists
    --            recursively look up a category for which a time_var_mapping exists
    --            copy the ancestor mappings to the cache table for the descendant category

    EXECUTE format('SELECT ep_create_table_like(%L, %L, %L, %L)', conf_schema, 'ep_time_var_mapping', case_schema, 'ep_time_var_mapping_all');
    EXECUTE format('INSERT INTO %I.ep_time_var_mapping_all SELECT * FROM %I.ep_time_var_mapping', case_schema, conf_schema); 

    FOR cat_id IN EXECUTE format('SELECT c.cat_id FROM %I.ep_emission_categories c LEFT JOIN %I.ep_time_var_mapping t USING(cat_id) WHERE t.cat_id IS NULL', conf_schema, conf_schema)
    LOOP
        -- RAISE NOTICE '%', cat_id;
        EXECUTE format('WITH RECURSIVE find_parent(p_id, cat_id, tv_id) AS (
            SELECT parent as p_id, cat_id, tv_id FROM %I.ep_emission_categories LEFT JOIN %I.ep_time_var_mapping USING (cat_id) WHERE cat_id=$1
            UNION ALL
            SELECT c.parent as p_id, c.cat_id, tv.tv_id FROM %I.ep_emission_categories c, find_parent f LEFT JOIN %I.ep_time_var_mapping tv ON f.cat_id=tv.cat_id WHERE c.cat_id = f.p_id AND tv.tv_id IS NULL
        )
        SELECT cat_id FROM find_parent ORDER BY cat_id LIMIT 1', conf_schema, conf_schema, conf_schema, conf_schema) INTO par_id USING cat_id;

        -- RAISE NOTICE '%', format('INSERT INTO %I.ep_time_var_mapping_all SELECT %s, tv_id from %I.ep_time_var_mapping WHERE cat_id=%s', case_schema, par_id, conf_schema, cat_id);
        EXECUTE format('INSERT INTO %I.ep_time_var_mapping_all SELECT $1, tv_id from %I.ep_time_var_mapping WHERE cat_id=$2', case_schema, conf_schema) USING cat_id, par_id;
    END LOOP;
    -- recompile statistics
    execute format('ANALYZE %I.ep_time_var_mapping_all', case_schema);

    -- Generate timezone shifts
    -- tzone_out = 'UTC'||to_char(itzone_out, 'SG000');
    --SELECT abbrev INTO tzone_out FROM pg_timezone_abbrevs WHERE utc_offset=(to_char(itzone_out, '99')||' hour')::interval AND is_dst='f' LIMIT 1;
    --RAISE NOTICE 'Output timezone is %',tzone_out;
    --raise notice 'itzone_out is %', itzone_out;
    EXECUTE format('TRUNCATE %I.ep_time_zone_shifts RESTART IDENTITY CASCADE',case_schema);
    -- reset time shift id in timezone table and fill array of time_out values
    EXECUTE format('UPDATE %I.ep_timezones SET ts_id = 0',case_schema);
    time_out = time_start;
    sql_text = 'SELECT ARRAY['''||time_out||'''';
    time_step = 0;
    WHILE time_step < num_times
    LOOP
        time_out := time_out + time_increment;
        sql_text := sql_text||','''||time_out||'''';
        time_step := time_step + 1;
    END LOOP;
    sql_text := sql_text||']::timestamp[]';
    EXECUTE sql_text INTO time_out_array;

    -- fill the time shift
    FOR tz_id, tz_name IN EXECUTE format('SELECT tz_id, tz_name FROM %I.ep_timezones',case_schema)
    LOOP
        -- look into time_zone_shifts and
        sql_text = 'SELECT min(ts_id) from (
                      SELECT ts_id, min(ts.ts_id) as tsm, count(*) as tsc
                      FROM (
                          SELECT s.ts_id, s.time_out tso, s.time_loc tsl, tso2,
                                 tso2 AT TIME ZONE $1 tsl2, tso2 AT TIME ZONE $1 - s.time_loc AS td
                             FROM %I.ep_time_zone_shifts s
                             JOIN unnest($2) AS tso2 ON tso2 = s.time_out
                             WHERE tso2 AT TIME ZONE $1 = s.time_loc) ts
                             GROUP BY ts_id
                      ) tzm
                      WHERE tzm.tsc = $3 + 1';
        EXECUTE format(sql_text,case_schema) USING tz_name, time_out_array, num_times INTO ts_id;
        IF (ts_id is null) THEN
            -- new time shift
            EXECUTE format('SELECT MAX(ts_id)+1 FROM %I.ep_timezones', case_schema) INTO ts_id;
            sql_text = 'INSERT INTO %I.ep_time_zone_shifts (ts_id, time_out, time_loc)
                        SELECT $1, tso, tso AT TIME ZONE $2 AS tsl FROM unnest($3) tso
                        ON CONFLICT DO NOTHING';
            EXECUTE format(sql_text,case_schema,case_schema) USING ts_id, tz_name, time_out_array;
        END IF;
        -- assign tz_id to ts_id
        EXECUTE format('UPDATE %I.ep_timezones SET ts_id = $1 WHERE tz_id = $2',case_schema) USING ts_id, tz_id;
    END LOOP;


    -- recompile statistics
    execute format('ANALYZE %I.ep_time_zone_shifts', case_schema);

    -- Generate time_factors for all categories and time_var_mappings
    EXECUTE format('TRUNCATE %I.ep_time_factors RESTART IDENTITY CASCADE',case_schema);
    FOR time_cur IN EXECUTE format('SELECT DISTINCT time_loc FROM %I.ep_time_zone_shifts order by time_loc', case_schema)
    LOOP
        year = EXTRACT(year FROM time_cur);
        month = EXTRACT(month FROM time_cur);
        dow = EXTRACT(isodow FROM time_cur);
        hour = EXTRACT(hour FROM time_cur);

        EXECUTE format('INSERT INTO %I.ep_time_factors (cat_id, time_loc, tv_factor)
                            SELECT cat_id, $1, mul(tv_factor)
                                FROM %I.ep_time_var_mapping_all
                                JOIN %I.ep_time_var USING(tv_id)
                                JOIN %I.ep_time_var_values USING(tv_id)
                                WHERE resolution = 3 AND period = $2 OR resolution = 2 AND period = $3 OR
                                      resolution = 1 AND period = $4 OR resolution = 4 AND period = $5
                                GROUP BY cat_id', case_schema, case_schema, conf_schema, conf_schema)
                 USING time_cur, month, dow, hour, year;
    END LOOP;

    -- update time_factors according to user defined time series
    EXECUTE format('INSERT INTO %I.ep_time_factors (cat_id, time_loc, tv_factor) 
			SELECT cat_id, time_loc, tv_factor FROM %I.ep_time_var_series
			ON CONFLICT ON CONSTRAINT ep_time_factors_pkey DO UPDATE 
  				SET tv_factor = excluded.tv_factor;', case_schema, conf_schema);
    -- recompile statistics
    execute format('ANALYZE %I.ep_time_factors', case_schema);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ep_speciation_splits(
    mod_id INTEGER,
    mech_ids INTEGER[],
    conf_schema VARCHAR DEFAULT 'conf',
    case_schema VARCHAR DEFAULT 'case')
  RETURNS VOID AS
-- evaluates speciation split_factors which are moles in kg of specie
$$
BEGIN

    -- first populate table ep_sp_factors that maps inventory species to speciation profile species
    -- calculate split factors from speciation compound profiles
    execute format('TRUNCATE %I.ep_sp_factors RESTART IDENTITY CASCADE',case_schema);
    execute format('INSERT INTO %I.ep_sp_factors (cat_id, spec_in_id, spec_sp_id, split_factor, mol_weight)
					SELECT cat_id, spec_in_id, ma.spec_sp_id, sum((react_fact*fraction)/(mol_weight)), sum(fraction*mol_weight) AS split_factor
					FROM %I.ep_comp_cat_profiles
					JOIN %I.ep_chem_compounds USING(chem_comp_id)
					JOIN (SELECT * FROM %I.ep_comp_mechanisms_assignment WHERE mech_id = ANY (%L)) AS ma USING(chem_comp_id)
					GROUP BY cat_id, spec_in_id, ma.spec_sp_id', case_schema, conf_schema, conf_schema, conf_schema, mech_ids);

    -- add split factors from gspro files
    execute format('INSERT INTO %I.ep_sp_factors (cat_id, spec_in_id, spec_sp_id, split_factor, mol_weight)
                SELECT cat_id, spec_in_id, spec_sp_id, mole_split_factor/mol_weight, mol_weight as split_factor
                FROM %I.ep_gspro_sp_factors WHERE mech_id = ANY (%L)',
                case_schema, conf_schema, mech_ids);
    -- recompile statistics
    execute format('ANALYZE %I.ep_sp_factors', case_schema);

	-- then populate table ep_mod_spec_factors that maps inventory species to output model species
	execute format('TRUNCATE %I.ep_mod_spec_factors RESTART IDENTITY CASCADE',case_schema);
    execute format('INSERT INTO %I.ep_mod_spec_factors (cat_id, spec_in_id, spec_mod_id, split_factor, mol_weight)
                SELECT cat_id, spec_in_id, spec_id, new_split_factor, mol_weight FROM (
                    SELECT cat_id, spec_in_id,
                        COALESCE(spec_mod_name, sp_nam.name) as mod_name,
                        COALESCE(map_fact*split_factor, split_factor) as new_split_factor, mol_weight
                    FROM %I.ep_sp_factors sp_fact
                    JOIN (SELECT spec_sp_id, name FROM %I.ep_sp_species WHERE mech_id = ANY (%L)) sp_nam USING (spec_sp_id)
                    LEFT JOIN (SELECT * FROM %I.ep_sp_mod_specie_mapping WHERE model_id = %L AND mech_id = ANY (%L)) AS map
                        ON sp_nam.name=map.spec_sp_name
                ) AS mapped_split
                JOIN %I.ep_out_species AS os ON mod_name=os.name',
        case_schema, case_schema, conf_schema, mech_ids, conf_schema, mod_id, mech_ids, case_schema);
    /*
    execute format('INSERT INTO %I.ep_mod_spec_factors (cat_id, spec_in_id, spec_mod_id, split_factor)
                        SELECT sp_fact.cat_id, sp_fact.spec_in_id, sp_fact.spec_sp_id,
                               COALESCE(map.map_fact*sp_fact.split_factor, sp_fact.split_factor)
                            FROM %I.ep_sp_factors sp_fact
                            JOIN %I.ep_sp_species sp_nam ON sp_nam.mech_id = ANY (%L) AND sp_nam.spec_sp_id=sp_fact.spec_sp_id
                            LEFT JOIN %I.ep_sp_mod_specie_mapping map
                                 ON map.model_id = %L AND map.mech_id = sp_nam.mech_id AND map.spec_sp_id = sp_nam.spec_sp_id',
                    case_schema, case_schema, conf_schema, mech_ids, conf_schema, mod_id);
    */
    -- recompile statistics
    execute format('ANALYZE %I.ep_mod_spec_factors', case_schema);

    perform ep_find_missing_speciation_splits(conf_schema, case_schema);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ep_find_missing_speciation_splits(
    conf_schema VARCHAR DEFAULT 'conf',
    case_schema VARCHAR DEFAULT 'case')
RETURNS VOID AS
$$
DECLARE
    spec_in_id BIGINT;
    cat_id BIGINT;
    par_id BIGINT;
BEGIN
    -- Fill in blanks in category-speciation mapping by copying profiles to the descendant categories without speciation profile
    EXECUTE format('SELECT ep_create_table_like(%L, %L, %L, %L)', case_schema, 'ep_mod_spec_factors', case_schema, 'ep_mod_spec_factors_all');
    EXECUTE format('INSERT INTO %I.ep_mod_spec_factors_all SELECT * FROM %I.ep_mod_spec_factors', case_schema, case_schema); 

    FOR spec_in_id IN EXECUTE format('SELECT distinct spec_in_id FROM %I.ep_mod_spec_factors', case_schema) LOOP
        FOR cat_id IN EXECUTE format('SELECT c.cat_id FROM %I.ep_emission_categories c LEFT JOIN (SELECT cat_id FROM %I.ep_mod_spec_factors WHERE spec_in_id=$1) s USING(cat_id) WHERE s.cat_id IS NULL', conf_schema, case_schema) USING(spec_in_id) LOOP
            EXECUTE format('WITH RECURSIVE find_parent(p_id, cat_id, split_factor) AS (
                SELECT parent as p_id, cat_id, split_factor FROM %I.ep_emission_categories LEFT JOIN (SELECT cat_id, split_factor FROM %I.ep_mod_spec_factors WHERE spec_in_id=$2) s USING (cat_id) WHERE cat_id=$1
                UNION ALL
                SELECT c.parent as p_id, c.cat_id, s.split_factor FROM %I.ep_emission_categories c, find_parent f LEFT JOIN (SELECT cat_id, split_factor FROM %I.ep_mod_spec_factors WHERE spec_in_id=$2) s ON f.cat_id=s.cat_id
                WHERE c.cat_id = f.p_id AND s.split_factor IS NULL
            )
            SELECT cat_id FROM find_parent ORDER BY cat_id LIMIT 1', conf_schema, case_schema, conf_schema, case_schema) INTO par_id USING cat_id, spec_in_id;            
---            RAISE NOTICE '%', format('INSERT INTO %I.ep_mod_spec_factors SELECT %s, split_factor from %I.ep_mod_spec_factors WHERE cat_id=%s AND spec_in_id=%s', case_schema, cat_id, case_schema, par_id, spec_in_id);
            EXECUTE format('INSERT INTO %I.ep_mod_spec_factors_all SELECT $1, spec_in_id, spec_mod_id, split_factor, mol_weight from %I.ep_mod_spec_factors WHERE cat_id=$2 AND spec_in_id=$3', case_schema, case_schema) USING cat_id, par_id, spec_in_id;
        END LOOP;
    END LOOP;
    -- recompile statistics
    execute format('ANALYZE %I.ep_mod_spec_factors_all', case_schema);
END;
$$ LANGUAGE plpgsql;


create or replace function ep_apply_spec_factors(
    conf_schema varchar default 'config',
    case_schema varchar default 'case')
  returns void as
$$
begin
    -- speciation
    execute format('ALTER TABLE %I.ep_sg_emissions_spec DROP CONSTRAINT IF EXISTS ep_sg_emissions_spec_pkey',case_schema);
    execute format('ALTER TABLE %I.ep_sg_emissions_spec DROP CONSTRAINT IF EXISTS ep_sg_emissions_spec_cat_id_fkey',case_schema);
    execute format('ALTER TABLE %I.ep_sg_emissions_spec DROP CONSTRAINT IF EXISTS ep_sg_emissions_spec_sg_id_fkey',case_schema);
    execute format('ALTER TABLE %I.ep_sg_emissions_spec DROP CONSTRAINT IF EXISTS ep_sg_emissions_spec_spec_id_fkey',case_schema);
    EXECUTE format('TRUNCATE %I.ep_sg_emissions_spec RESTART IDENTITY CASCADE',case_schema);
    EXECUTE format('INSERT INTO %I.ep_sg_emissions_spec (sg_id, spec_id, cat_id, emiss)
	                    SELECT e.sg_id, s.spec_mod_id, e.cat_id, e.emiss*s.split_factor
                        FROM %I.ep_sg_emissions e
                        JOIN %I.ep_mod_spec_factors_all s USING (cat_id, spec_in_id)', case_schema, case_schema, case_schema);
    -- recompile statistics
    execute format('ANALYZE %I.ep_sg_emissions_spec', case_schema);
    execute format('ALTER TABLE %I.ep_sg_emissions_spec ADD CONSTRAINT ep_sg_emissions_spec_pkey PRIMARY KEY (sg_id, spec_id, cat_id)',case_schema);
    execute format('ALTER TABLE %I.ep_sg_emissions_spec ADD CONSTRAINT ep_sg_emissions_spec_cat_id_fkey FOREIGN KEY (cat_id) REFERENCES %I."ep_emission_categories"',case_schema, conf_schema);
    execute format('ALTER TABLE %I.ep_sg_emissions_spec ADD CONSTRAINT ep_sg_emissions_spec_sg_id_fkey FOREIGN KEY (sg_id) REFERENCES %I.ep_sources_grid',case_schema, case_schema);
    execute format('ALTER TABLE %I.ep_sg_emissions_spec ADD CONSTRAINT ep_sg_emissions_spec_spec_id_fkey FOREIGN KEY (spec_id) REFERENCES %I.ep_out_species',case_schema, case_schema);
end;
$$ LANGUAGE plpgsql;

-- time dissagregation
create or replace function ep_apply_time_factors(
    conf_schema varchar default 'config',
    case_schema varchar default 'case')
  returns void as
$$
begin
    raise notice 'ep_apply_time_factors start: %', clock_timestamp();
    execute format('ALTER TABLE %I.ep_sg_out_emissions DROP CONSTRAINT IF EXISTS ep_sg_out_emissions_pkey',case_schema);
    execute format('ALTER TABLE %I.ep_sg_out_emissions DROP CONSTRAINT IF EXISTS ep_sg_out_emissions_cat_id_fkey',case_schema);
    execute format('ALTER TABLE %I.ep_sg_out_emissions DROP CONSTRAINT IF EXISTS ep_sg_out_emissions_sg_id_fkey',case_schema);
    execute format('ALTER TABLE %I.ep_sg_out_emissions DROP CONSTRAINT IF EXISTS ep_sg_out_emissions_spec_id_fkey',case_schema);
    EXECUTE format('TRUNCATE %I.ep_sg_out_emissions RESTART IDENTITY CASCADE',case_schema);
    EXECUTE format('INSERT INTO %I.ep_sg_out_emissions (sg_id, spec_id, cat_id, time_out, emiss)
                    SELECT e.sg_id, e.spec_id, e.cat_id, s.time_out, e.emiss*t.tv_factor
                    FROM %I.ep_sg_emissions_spec e
                    JOIN %I.ep_time_factors t USING (cat_id)
                    JOIN %I.ep_sources_grid sg USING (sg_id)
                    JOIN %I.ep_grid_tz g USING (grid_id)
                    JOIN %I.ep_timezones z USING (tz_id)
                    JOIN %I.ep_time_zone_shifts s USING (ts_id,time_loc)',
                    case_schema, case_schema, case_schema, case_schema, case_schema, case_schema);
    -- recompile statistics
    execute format('ANALYZE %I.ep_sg_out_emissions', case_schema);
    execute format('ALTER TABLE %I.ep_sg_out_emissions ADD CONSTRAINT ep_sg_out_emissions_pkey PRIMARY KEY (sg_id, spec_id, cat_id, time_out)',case_schema);
    execute format('ALTER TABLE %I.ep_sg_out_emissions ADD CONSTRAINT ep_sg_out_emissions_cat_id_fkey FOREIGN KEY (cat_id) REFERENCES %I.ep_emission_categories',case_schema, conf_schema);
    execute format('ALTER TABLE %I.ep_sg_out_emissions ADD CONSTRAINT ep_sg_out_emissions_sg_id_fkey FOREIGN KEY (sg_id) REFERENCES %I.ep_sources_grid',case_schema, case_schema);
    execute format('ALTER TABLE %I.ep_sg_out_emissions ADD CONSTRAINT ep_sg_out_emissions_spec_id_fkey FOREIGN KEY (spec_id) REFERENCES %I.ep_out_species',case_schema, case_schema);
    raise notice 'ep_apply_time_factors end: %', clock_timestamp();
end;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ep_init_transformation_queue(case_schema varchar default 'case')
    RETURNS void AS
$$
BEGIN
    EXECUTE format('TRUNCATE %I.ep_sources_grid RESTART IDENTITY CASCADE', case_schema);
    EXECUTE format('TRUNCATE %I.ep_sg_emissions RESTART IDENTITY CASCADE', case_schema);
    EXECUTE format('TRUNCATE %I.ep_sg_activity_data RESTART IDENTITY CASCADE',case_schema);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ep_finalize_transformation_queue(
    source_schema varchar default 'sources',
    case_schema varchar default 'case',
    sources_table varchar default 'ep_in_sources',
    emissions_table varchar default 'ep_in_emissions',
    activity_table varchar default 'ep_in_activity_data',
    eset_table varchar default 'ep_emission_sets',
    scenario_list_table varchar default 'ep_scenario_list',
    scenario_factors_table varchar default 'ep_scenario_factors_all',
    scenarios varchar[] default ARRAY[]::VARCHAR[]
)
    RETURNS void AS
$$
DECLARE
--scenarios VARCHAR[];
scenario_ids INTEGER[];
sql text;
scen_id INTEGER;
BEGIN
    EXECUTE format('CREATE INDEX IF NOT EXISTS "ep_sources_grid_source_type" ON %I.ep_sources_grid (source_type)',case_schema);
    -- calculate emisions in grids
    -- this has to be done once after all sources have been transformed to the grid

    IF cardinality(scenarios) <> 0 THEN
        --scenarios = regexp_split_to_array(scenarios_str, '\s*,\s*');
        EXECUTE format('SELECT ARRAY_AGG(scenario_id) FROM %I.%I WHERE scenario_name = ANY($1)',
                        source_schema, scenario_list_table) INTO scenario_ids USING scenarios;
        IF array_length(scenario_ids,1)=0 IS NOT FALSE THEN
            RAISE EXCEPTION 'Scenarios % are not defined in scenario list.', scenarios;
        END IF;

        /*
        EXECUTE format('UPDATE transformation_chains_scenarios_factors tcsf SET mul_factor = mul_factor * sfa_mul_factor
                            FROM
                                (SELECT mul(factor) AS sfa_mul_factor, cat_id, spec_in_id FROM %I.%I WHERE scenario_id = ANY($1)
                                    GROUP BY cat_id, spec_in_id) AS sfa
                            WHERE tcsf.cat_id = sfa.cat_id AND tcsf.spec_in_id = sfa.spec_in_id', source_schema, scenario_factors_table)
                        USING scenario_ids;
        */

        sql = format('INSERT INTO %I.ep_transformation_chains_scenarios (chain_id, scenario_id) SELECT chain_id, $1 FROM %I.ep_transformation_chains ON CONFLICT DO NOTHING', case_schema, case_schema);
        FOREACH scen_id IN ARRAY scenario_ids
        LOOP
            EXECUTE sql USING scen_id;
        END LOOP;

        RAISE NOTICE '%', 'Scenario IDS selected for application in transformation queue finalization:';
        RAISE NOTICE '%', scenario_ids;
    END IF;

    -- temporary view that prepares scenario multiplicative factors for chains:
    BEGIN
          EXECUTE format('SELECT * FROM %I.ep_scenario_factors_all LIMIT 1', source_schema);
    EXCEPTION
       WHEN undefined_table THEN
          EXECUTE format('SELECT ep_create_table_like(%L, %L, %L, %L)', source_schema, 'ep_scenario_factors', source_schema, 'ep_scenario_factors_all');
    END;
    EXECUTE format('CREATE TEMPORARY VIEW transformation_chains_scenarios_factors AS SELECT chain_id, mul(factor) mul_factor, cat_id, spec_in_id FROM %I.ep_transformation_chains_scenarios tcs JOIN %I.ep_scenario_factors_all sfa USING(scenario_id) GROUP BY cat_id, spec_in_id, chain_id', case_schema, source_schema);

    EXECUTE format('INSERT INTO %I.ep_sg_emissions (sg_id, spec_in_id, cat_id, emiss)
                    SELECT sgs.sg_id, ie.spec_in_id, ie.cat_id, ie.emission*sgs.sg_factor*coalesce(scen.mul_factor, 1)
                        FROM %I.ep_sources_grid sgs
                        JOIN %I.%I ie USING(source_id)
                        JOIN %I.%I s USING(source_id)
                        JOIN %I.%I eset USING(eset_id)
                        LEFT JOIN transformation_chains_scenarios_factors scen
                                ON scen.cat_id=ie.cat_id AND scen.spec_in_id=ie.spec_in_id
                                AND scen.chain_id = sgs.transformation_chain
                        WHERE ie.source_id=sgs.source_id AND data_type=''E''',
                    case_schema, case_schema, source_schema, emissions_table,
                    source_schema, sources_table, source_schema, eset_table);

    EXECUTE format('ANALYZE %I.ep_sg_emissions', case_schema);

    -- calculate activity data in grids
    EXECUTE format('INSERT INTO %I.ep_sg_activity_data (sg_id, act_unit_id, cat_id, act_intensity)
                    SELECT sgs.sg_id, ia.act_unit_id, ia.cat_id,
                           CASE WHEN data_type=''C'' THEN ia.act_intensity*sgs.sg_factor
                                WHEN data_type=''D'' THEN ia.act_intensity
                           END
                           FROM %I.ep_sources_grid sgs
                           JOIN %I.%I ia USING(source_id)
                           JOIN %I.%I s USING(source_id)
                           JOIN %I.%I eset USING(eset_id)
                           WHERE ia.source_id=sgs.source_id AND (data_type=''C'' OR data_type=''D'')',
                    case_schema, case_schema, source_schema, activity_table, source_schema, sources_table, source_schema, eset_table);
    -- recompile statistics
    EXECUTE format('ANALYZE %I.ep_sg_activity_data', case_schema);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ep_sources_to_grid(
    source_schema varchar default 'sources',
    case_schema varchar default 'case',
    sources_table varchar default 'ep_in_sources',
    factors_table varchar default 'ep_intersect_factors',
    factor_field varchar default 'sg_factor',
    transformation_chain integer default null,
    joins varchar default '',
    filters varchar default ''
)
  returns void as
$$
DECLARE
    q text;
BEGIN
    -- assign sources to grid
    q = 'INSERT INTO %I.ep_sources_grid (source_type, source_id, grid_id, k, sg_factor, transformation_chain)
                    SELECT s.source_type, s.source_id, f.grid_id, 1, %s, '
                    || quote_literal(transformation_chain)
                    || ' FROM %I.%I s
                         JOIN %I.%I f USING(geom_id)';

    IF factor_field = '' THEN
        factor_field = '1';
    ELSE
        factor_field = format('f.%I', factor_field);
    END IF;

    IF joins != '' THEN
        q = q || joins;
    END IF;
    IF filters != '' THEN
        q = q || ' WHERE ' || filters;
    END IF;
    q = q || ' ON CONFLICT DO NOTHING';
    RAISE NOTICE 'ep_sources_to_grid: %, %, %, %, %, %, %, %', q, case_schema, factor_field, source_schema, sources_table, case_schema, factors_table, transformation_chain;
    EXECUTE format(q, case_schema, factor_field, source_schema, sources_table, case_schema, factors_table);
    -- recompile statistics
    EXECUTE format('ANALYZE %I.ep_sources_grid', case_schema);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ep_find_missing_point_parameters(
    conf_schema VARCHAR DEFAULT 'conf',
    case_schema VARCHAR DEFAULT 'case')
RETURNS VOID AS
$$
DECLARE
    spec_in_id BIGINT;
    cat_id BIGINT;
    par_id BIGINT;
BEGIN
    -- Fill in blanks in category-point parameters mapping by copying parameters to the descendant categories without parameters
    EXECUTE format('SELECT ep_create_table_like(%L, %L, %L, %L)', case_schema, 'ep_default_point_params', case_schema, 'ep_default_point_params_all');
    EXECUTE format('INSERT INTO %I.ep_default_point_params_all SELECT * FROM %I.ep_default_point_params', case_schema, case_schema); 

    FOR cat_id IN EXECUTE format('SELECT c.cat_id FROM %I.ep_emission_categories c LEFT JOIN %I.ep_default_point_params p USING(cat_id) WHERE p.cat_id IS NULL', conf_schema, case_schema)
    LOOP
        EXECUTE format('WITH RECURSIVE find_parent(p_id, cat_id, height) AS (
            SELECT parent as p_id, cat_id, height FROM %I.ep_emission_categories LEFT JOIN %I.ep_default_point_params USING (cat_id) WHERE cat_id=$1
            UNION ALL
            SELECT c.parent as p_id, c.cat_id, p.height FROM %I.ep_emission_categories c, find_parent f LEFT JOIN %I.ep_default_point_params p ON f.cat_id=p.cat_id WHERE c.cat_id = f.p_id AND p.height IS NULL
        )
        SELECT cat_id FROM find_parent ORDER BY cat_id LIMIT 1', conf_schema, case_schema, conf_schema, case_schema) INTO par_id USING cat_id;

        EXECUTE format('INSERT INTO %I.ep_default_point_params_all (cat_id, spec_in_id, lim, height, diameter, temperature, velocity) SELECT $1, spec_in_id, lim, height, diameter, temperature, velocity FROM %I.ep_default_point_params WHERE cat_id=$2', case_schema, case_schema) USING cat_id, par_id;
    END LOOP;
    -- recompile statistics
    execute format('ANALYZE %I.ep_default_point_params_all', case_schema);

END;
$$ LANGUAGE plpgsql;
