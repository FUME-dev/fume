/*
Description: It creates ep_process_sources FUME sql function.
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

create or replace function ep_process_sources(
    schema text,
    table_raw text,
    eset_id integer,
    scen_ids integer[],
    specie_input_type text,
    specie_def text,
    specie_val text,
    category_input_type text,
    category_def text,
    point_par text[][])
  returns void as 
-- this processes one eset from raw format to inner fume format, i.e. fills tables ep_in_sources (or ep_in_sources_point in case of point sources) and
-- ep_in_emissions in case of emission data or ep_in_activity_data in case of activity data. Scenario factors are applied.  
$$
declare
    inv_id integer;
    gset_id integer;
    null_cats text[];
    data_type char;
    i integer;
    scen_idx integer;
    filter_idx integer;
    par_names_s text;
    par text;
    sqltext text;
    sqlcond text;
    sql_tab_raw text;
    cond_cols text[];
    cond_vals text[];
    data_table text;
    specmap_table text;
    spec_id text;
    scen_id integer;
    value_col text;
    filter_id integer;
    filter_ids integer[];
    filters text[];
    filter text;
begin

    raise notice 'ep_process_sources start';
    -- find inventory and geometry set id
    execute format('SELECT f.inv_id, s.gset_id
                      FROM %I.ep_emission_sets s
                      JOIN %I.ep_source_files f ON f.file_id = s.file_id
                      WHERE eset_id = $1', schema, schema) into inv_id, gset_id using eset_id;
    -- populate ep_in_sources table
    execute format('DROP TABLE IF EXISTS %I.sid_map;',schema);
    sqltext = format('CREATE TABLE %I.sid_map AS WITH s AS
                      (INSERT INTO %I.ep_in_sources (eset_id, source_type, source_orig_id, geom_id)
				        SELECT DISTINCT ON (source_orig_id) $1, source_type, source_orig_id, in_geom.geom_id FROM %I AS orig
				          LEFT OUTER JOIN %I."ep_in_geometries" as in_geom
				            ON in_geom.gset_id = $2 and in_geom.geom_orig_id = orig.geom_orig_id
					    RETURNING source_id, source_orig_id)
				      SELECT source_id, source_orig_id FROM s',
				      schema, schema, table_raw, schema);
	raise notice 'sqltext 1: %, %, %', sqltext, eset_id, gset_id;
	execute sqltext using eset_id, gset_id;

    -- find out whether we have emission or activity data to import
    execute format('SELECT data_type
                  FROM %I.ep_emission_sets
                  WHERE eset_id = $1', schema) into data_type using eset_id;
    -- prepare table and column names based on data type
    if data_type = 'E' then
        data_table = 'ep_in_emissions';
        specmap_table = 'ep_in_specie_mapping';
        spec_id = 'spec_in_id';
        value_col = 'emission';
    else
        data_table = 'ep_in_activity_data';
        specmap_table = 'ep_activity_units_mapping';
        spec_id = 'act_unit_id';
        value_col = 'act_intensity';
    end if;
    
    -- create temp views for convert_to_row function
    execute 'DROP VIEW IF EXISTS spec_mapping';
    execute format('CREATE TEMP VIEW spec_mapping AS SELECT * FROM %I.%I WHERE inv_id = %s', schema, specmap_table, inv_id);
    execute 'DROP VIEW IF EXISTS cat_mapping';
    execute format('CREATE TEMP VIEW cat_mapping AS 
                        SELECT * FROM %I.ep_classification_mapping WHERE inv_id = %s', schema, inv_id);

    -- convert raw_table view to row format for both specie and category 
    execute 'SELECT ep_convert_to_row_table($1, $2, $3, $4, $5, $6, $7)' using
                  schema, table_raw, specie_input_type, specie_def, specie_val, category_input_type, category_def;
    
    -- populate emission table
    -- test if category and specie names are defined in mapping tables
    sqltext = format('CREATE TEMP TABLE emis_before_scenario ON COMMIT DROP AS 
                      SELECT source_id, m.%I, cat.cat_id, e.source_orig_id, (replace(emiss_orig::text, '','', ''.'')::double precision)*conv_factor AS %s '
                      'FROM row_raw_emiss AS e
                       JOIN (SELECT * FROM %I.%I WHERE inv_id = $1) AS m USING (orig_name)
                       JOIN %I.sid_map AS s ON s.source_orig_id = e.source_orig_id
                       JOIN (SELECT * FROM %I.ep_classification_mapping WHERE inv_id = $1) AS cat ON cat.orig_cat_id::text=e.orig_cat_id::text ' ||
                       'WHERE (emiss_orig::text = '''') IS FALSE AND replace(emiss_orig::text, '','', ''.'')::double precision > 0',
                    spec_id, value_col, schema, specmap_table, schema, schema);
    raise notice 'sqltext 3: %, %', sqltext, inv_id; 
    execute sqltext using inv_id;
    
    execute 'SELECT COUNT(*) FROM emis_before_scenario' into i;
    if i = 0 then
        raise exception E'\nError: The emission set do not contain any known specie/category combination. Please check the source file configuration.\n';
    end if;

   -- apply scenario factors
   for scen_idx in 1..coalesce(array_length(scen_ids, 1), 0) loop
     scen_id = scen_ids[scen_idx];
     --raise notice 'scen: %', scen_id;
     execute format('SELECT ARRAY(SELECT distinct(filter_id) FROM emis_before_scenario AS ebs JOIN (SELECT * FROM %I.ep_scenario_factors_all WHERE scenario_id = $1) AS scen 
                      ON scen.spec_in_id=ebs.spec_in_id AND scen.cat_id=ebs.cat_id)', schema) into filter_ids using scen_id; 

     for filter_idx in 1..coalesce(array_length(filter_ids, 1), 0) loop
        filter_id = filter_ids[filter_idx];
        cond_cols = array[]::text[];
        cond_vals = array[]::text[];
        
        execute format('SELECT regexp_split_to_array(filter_definition,'','') FROM %I.ep_scenario_filters WHERE filter_id=$1 LIMIT 1', schema) into filters using filter_id;		
        
        --raise notice 'filter: % %', filter_id, filters;
	if (filters = array['']) then
		sqlcond = '';	
                sql_tab_raw = ' ';
	else
                sql_tab_raw = format(', %I AS raw ', table_raw);

	       	foreach filter in array filters loop
			i = position('!=' in filter);
		       	if (i = 0) then
		       		cond_cols = array_append(cond_cols, '"' || trim(split_part(filter, '=', 1) ) || '"'  || '=%L');
		   	else
		       		cond_cols = array_append(cond_cols, '"' || trim(split_part(filter, '!=', 1) ) || '"'  || '!=%L');
		       	end if;
		   	cond_vals = array_append(cond_vals, trim(split_part(filter, '=', 2)));
	       	end loop;
	       	sqlcond = format('AND ebs.source_orig_id = raw.source_orig_id AND ') || array_to_string(cond_cols, ' AND ');
	       	sqlcond = format(sqlcond, VARIADIC cond_vals);	
	 end if;
	 sqltext = format('UPDATE emis_before_scenario ebs 
	     			SET %I = ep_eval(concat(%I, operation, factor))
	     			FROM %I.ep_scenario_factors_all AS scen' || sql_tab_raw || 'WHERE scen.spec_in_id=ebs.spec_in_id AND scen.cat_id=ebs.cat_id AND scenario_id = $1 AND filter_id = $2' || sqlcond, value_col, value_col, schema);	
	 raise notice 'sqltext 4: %', sqltext;
	 BEGIN
	 	execute sqltext using scen_id, filter_id;
	 EXCEPTION
	 	WHEN SQLSTATE '42703' THEN	-- filter column(s) does not exist in raw data -> do nothing
	 	RAISE NOTICE 'Factor for scen_id % and filter % not applied as one (or more) columns are not specified in raw input data.', scen_id, filters;
	 END;
      end loop;
    end loop;
	    
    sqltext = format('INSERT INTO %I.%I (source_id, %I, cat_id, %I) SELECT source_id, %I, cat_id, %I FROM emis_before_scenario',
                      schema, data_table, spec_id, value_col, spec_id, value_col);
    raise notice 'sqltext 5: %', sqltext;
    execute sqltext;
    

    -- populate point source table if point source
    for i in 1 .. 4 loop
        par = point_par[i][1];
        if ((par is null) or (par = '')) then
            par_names_s = concat_ws(', ', par_names_s, 'NULL::real');
        else
            par_names_s = concat_ws(', ', par_names_s, format('(NULLIF(p.%I::text, '''')::float + %L) * %L', par,
                point_par[i][3], point_par[i][2]));
        end if;
    end loop;

    sqltext = format('INSERT INTO %I.ep_in_sources_point (source_id, height, diameter, temperature, velocity)
                      SELECT DISTINCT m.source_id, ' || par_names_s || '
                        FROM %I AS p
                        JOIN %I.sid_map AS m ON m.source_orig_id = p.source_orig_id
                        JOIN %I.ep_in_sources s ON s.source_id = m.source_id AND s.source_type = %L',
                    schema, table_raw, schema, schema, 'P');
    raise notice 'sqltext 4: %', sqltext;
    execute sqltext;
end;
$$ language plpgsql;


CREATE OR REPLACE FUNCTION ep_convert_to_row_table(
    schema text,
    table_raw text,
    specie_input_type text,
    specie_def text,
    specie_val text,
    category_input_type text,
    category_def text)
RETURNS VOID AS
-- gets the table_raw emission table in user provided fromat and converts it to row_raw_table temporary table which has species and categories in rows
$$
declare
    test boolean;
    spec_names text[];
    spec_names_s text;
    cat_names text[];
    cat_names_s text;
    spectext text;
    cattext text;
    emisstext text;
begin
    case specie_input_type
        when 'column' then
            execute 'SELECT ARRAY ( SELECT orig_name FROM spec_mapping
                         JOIN (SELECT column_name AS orig_name FROM information_schema.columns
                         WHERE table_name = $1) AS r USING (orig_name))' using table_raw into spec_names;
            if spec_names = '{}' then
                raise exception E'\nError: The emission set does not contain any columns with known species.\n';
            end if;
            execute 'SELECT string_agg(format(''%L'', s), '','') FROM unnest($1) AS s' USING spec_names INTO spec_names_s;
            spectext = 'unnest(array[' || spec_names_s || '])';
            execute 'SELECT string_agg(format(''%I::text'', s), '','') FROM unnest($1) AS s' USING spec_names INTO spec_names_s;
            emisstext = 'unnest(array[' || spec_names_s || '])';
        when 'row' then
            execute format('SELECT exists (SELECT 1 FROM information_schema.columns
                         WHERE table_name = $1 AND column_name = %L)', specie_def) using table_raw into test;
            if test then
                spectext = format('%I', specie_def);
            else
                raise exception E'\nError: Column % with specie names not found in emission set.\n', specie_def;
            end if;
        when 'predef' then
            execute format('SELECT exists (SELECT 1 FROM spec_mapping WHERE orig_name = %L LIMIT 1)', specie_def) into test;
            if test then
                spectext = format('''%s''::text', specie_def);
            else
                raise exception E'\nError: Specie % not found in specie mapping table\n', specie_def;
            end if;
    end case;
    
    case category_input_type
        when 'column' then
           execute 'SELECT ARRAY ( SELECT orig_cat_id FROM cat_mapping
                        JOIN (SELECT column_name AS orig_cat_id FROM information_schema.columns
                        WHERE table_name = $1) AS r USING (orig_cat_id))' USING table_raw INTO cat_names;
           if cat_names = '{}' then
                raise exception E'\nThe emission set does not contain any columns with known categories.\n';
            end if;
           execute 'SELECT string_agg(format(''%L'', s), '','') FROM unnest($1) AS s' USING cat_names INTO cat_names_s;
           cattext = 'unnest(array[' || cat_names_s || '])';
           execute 'SELECT string_agg(format(''%I::text'', s), '','') FROM unnest($1) AS s' USING cat_names INTO cat_names_s;
           emisstext = 'unnest(array[' || cat_names_s || '])';
        when 'row' then
            execute format('SELECT exists (SELECT 1 FROM information_schema.columns
                         WHERE table_name = $1 AND column_name = %L)', category_def) using table_raw into test;
            if test then
                cattext = format('%I', category_def);
            else
                raise exception E'\nError: Column % with category names not found in emission set.\n', category_def;
            end if;
        when 'predef' then
           execute format('SELECT exists (SELECT 1 FROM cat_mapping WHERE orig_cat_id = %L LIMIT 1)', category_def) into test;
           if test then
               cattext = format('''%s''::text', category_def);
           else
               raise exception E'\n Error: Category % not found in category mapping table\n', category_def;
           end if;
    end case;
    
    if (emisstext is null) then 
        execute format('SELECT exists (SELECT 1 FROM information_schema.columns
                        WHERE table_name = $1 AND column_name = %L)', specie_val) using table_raw into test;
        if test then
            emisstext = format('%I', specie_val);  
        else
            raise exception E'\nError: Column % with emission values not found in emission set.\n', specie_val;
        end if;
    end if;
    
    execute format('CREATE TEMP TABLE row_raw_emiss ON COMMIT DROP AS 
       SELECT source_orig_id, ' || 
              cattext || ' AS orig_cat_id, ' || 
              spectext || ' AS orig_name, ' ||
              emisstext || ' AS emiss_orig 
         FROM %I', table_raw);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ep_find_missing_calculate_pollutants(
    conf_schema VARCHAR DEFAULT 'conf',
    source_schema VARCHAR DEFAULT 'source')
RETURNS VOID AS
-- creates table source_schema.ep_calculate_pollutants_all, which contains calculate pollutants factors for all categories using category hierarchy structure        
$$
DECLARE
    spec_in_id BIGINT;
    cat_id BIGINT;
    par_id BIGINT;
BEGIN
    -- Fill in blanks in category-calculate polutants mapping by copying parameters to the descendant categories without parameters
    EXECUTE format('SELECT ep_create_table_like(%L, %L, %L, %L)', source_schema, 'ep_calculate_pollutants', source_schema, 'ep_calculate_pollutants_all');
    EXECUTE format('INSERT INTO %I.ep_calculate_pollutants_all SELECT * FROM %I.ep_calculate_pollutants', source_schema, source_schema); 

    FOR cat_id IN EXECUTE format('SELECT c.cat_id FROM %I.ep_emission_categories c LEFT JOIN %I.ep_calculate_pollutants p USING(cat_id) WHERE p.cat_id IS NULL', conf_schema, source_schema)
    LOOP
        EXECUTE format('WITH RECURSIVE find_parent(p_id, cat_id, spec_out_id) AS (
            SELECT parent as p_id, cat_id, spec_out_id FROM %I.ep_emission_categories LEFT JOIN %I.ep_calculate_pollutants USING (cat_id) WHERE cat_id=$1
            UNION ALL
            SELECT c.parent as p_id, c.cat_id, p.spec_out_id FROM %I.ep_emission_categories c, find_parent f LEFT JOIN %I.ep_calculate_pollutants p ON f.cat_id=p.cat_id WHERE c.cat_id = f.p_id AND p.spec_out_id IS NULL
        )
        SELECT cat_id FROM find_parent ORDER BY cat_id LIMIT 1', conf_schema, source_schema, conf_schema, source_schema) INTO par_id USING cat_id;

        EXECUTE format('INSERT INTO %I.ep_calculate_pollutants_all (cat_id, spec_out_id, spec_inp_id, coef, cat_order) SELECT $1, spec_out_id, spec_inp_id, coef, cat_order FROM %I.ep_calculate_pollutants WHERE cat_id=$2', source_schema, source_schema) USING cat_id, par_id;
    END LOOP;
    -- recompile statistics
    execute format('ANALYZE %I.ep_calculate_pollutants_all', source_schema);

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ep_find_missing_vdistribution_factors(
    conf_schema text DEFAULT 'conf',
    source_schema text DEFAULT 'source',
    case_schema text DEFAULT 'case')
RETURNS VOID AS
-- creates table case_schema.ep_vdistribution_factors_out_all, which contains vdistribution factors for all categories using category hierarchy structure    
$$
DECLARE
    vdistribution_id INTEGER;
    cat_id BIGINT;
    par_id BIGINT;
BEGIN
    EXECUTE format('SELECT ep_create_table_like(%L, %L, %L, %L)', case_schema, 'ep_vdistribution_factors_out', case_schema, 'ep_vdistribution_factors_out_all');
    EXECUTE format('INSERT INTO %I.ep_vdistribution_factors_out_all SELECT * FROM %I.ep_vdistribution_factors_out', case_schema, case_schema);

    FOR vdistribution_id IN EXECUTE format('SELECT vdistribution_id FROM %I.ep_vdistribution_names', source_schema) LOOP
        FOR cat_id IN EXECUTE format('SELECT c.cat_id FROM %I.ep_emission_categories c LEFT JOIN (SELECT cat_id FROM %I.ep_vdistribution_factors_out WHERE vdistribution_id=$1) f USING(cat_id) WHERE f.cat_id IS NULL', conf_schema, case_schema) USING vdistribution_id  LOOP

          EXECUTE format('WITH RECURSIVE find_parent(p_id, cat_id, factor) AS (
                SELECT parent as p_id, cat_id, factor FROM %I.ep_emission_categories LEFT JOIN (SELECT cat_id, factor FROM %I.ep_vdistribution_factors_out WHERE vdistribution_id=$2) s USING (cat_id) WHERE cat_id=$1
                 UNION ALL
                 SELECT c.parent as p_id, c.cat_id, s.factor FROM %I.ep_emission_categories c, find_parent f LEFT JOIN (SELECT cat_id, factor FROM %I.ep_vdistribution_factors_out WHERE vdistribution_id=$2 ) s ON f.cat_id=s.cat_id
                 WHERE c.cat_id = f.p_id AND s.factor IS NULL
              )
             SELECT cat_id FROM find_parent ORDER BY cat_id LIMIT 1', conf_schema, case_schema, conf_schema, case_schema) INTO par_id USING cat_id,  vdistribution_id;
            EXECUTE format('INSERT INTO %I.ep_vdistribution_factors_out_all (vdistribution_id, cat_id, level, factor) SELECT vdistribution_id, $1, level, factor FROM %I.ep_vdistribution_factors_out WHERE cat_id=$2 AND vdistribution_id=$3',
        case_schema, case_schema) USING cat_id, par_id, vdistribution_id;

        END LOOP;
    END LOOP;
    -- recompile statistics
    execute format('ANALYZE %I.ep_vdistribution_factors_out_all', case_schema);

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ep_find_missing_scenario_factors(
    conf_schema VARCHAR DEFAULT 'conf',
    source_schema VARCHAR DEFAULT 'source')
RETURNS VOID AS
-- creates table source_schema.ep_scenario_factors_all, which contains scenario factors for all categories using category hierarchy structure
$$
DECLARE
    spec_in_id INTEGER;
    scenario_id INTEGER;
    cat_id BIGINT;
    par_id BIGINT;
BEGIN
    EXECUTE format('SELECT ep_create_table_like(%L, %L, %L, %L)', source_schema, 'ep_scenario_factors', source_schema, 'ep_scenario_factors_all');
    EXECUTE format('INSERT INTO %I.ep_scenario_factors_all SELECT * FROM %I.ep_scenario_factors', source_schema, source_schema); 

    FOR scenario_id IN EXECUTE format('SELECT scenario_id FROM %I.ep_scenario_list', source_schema) LOOP
      FOR spec_in_id IN EXECUTE format('SELECT DISTINCT spec_in_id FROM %I.ep_scenario_factors WHERE scenario_id=$1', source_schema) USING scenario_id LOOP
    	FOR cat_id IN EXECUTE format('SELECT c.cat_id FROM %I.ep_emission_categories c LEFT JOIN (SELECT cat_id FROM %I.ep_scenario_factors WHERE scenario_id=$1 AND spec_in_id=$2) f USING(cat_id) WHERE f.cat_id IS NULL', conf_schema, source_schema) USING scenario_id, spec_in_id LOOP
    	
          EXECUTE format('WITH RECURSIVE find_parent(p_id, cat_id, factor) AS (
                SELECT parent as p_id, cat_id, factor FROM %I.ep_emission_categories LEFT JOIN (SELECT cat_id, factor FROM %I.ep_scenario_factors WHERE scenario_id=$3 AND spec_in_id=$2) s USING (cat_id) WHERE cat_id=$1
                 UNION ALL
                 SELECT c.parent as p_id, c.cat_id, s.factor FROM %I.ep_emission_categories c, find_parent f LEFT JOIN (SELECT cat_id, factor FROM %I.ep_scenario_factors WHERE scenario_id=$3 AND spec_in_id=$2) s ON f.cat_id=s.cat_id
                 WHERE c.cat_id = f.p_id AND s.factor IS NULL
              )
             SELECT cat_id FROM find_parent ORDER BY cat_id LIMIT 1', conf_schema, source_schema, conf_schema, source_schema) INTO par_id USING cat_id, spec_in_id, scenario_id;                        
            EXECUTE format('INSERT INTO %I.ep_scenario_factors_all (scenario_id, filter_id, cat_id, spec_in_id, factor, operation) SELECT scenario_id, filter_id, $1, spec_in_id, factor, operation FROM %I.ep_scenario_factors WHERE cat_id=$2 AND spec_in_id=$3 AND scenario_id=$4',
    	source_schema, source_schema) USING cat_id, par_id, spec_in_id, scenario_id;

    	END LOOP;
      END LOOP;
    END LOOP;
    -- recompile statistics
    execute format('ANALYZE %I.ep_scenario_factors_all', source_schema);

END;
$$ LANGUAGE plpgsql;


create or replace function ep_create_table_like(orig_schema text, orig_table text, new_schema text, new_table text)
-- creates empty copy of orig_schema.orig_table, including indexes and foreign keys, if it exists it is dropped first
-- copied from: https://stackoverflow.com/questions/23693873/how-to-copy-structure-of-one-table-to-another-with-foreign-key-constraints-in-ps
returns void language plpgsql
as $$
declare
    rec record;
    orig_schema_table text;
    new_schema_table text;
begin
    execute format('drop table if exists %I.%I', new_schema, new_table);
    execute format(
        'create table %I.%I (like %I.%I including all)',
        new_schema, new_table, orig_schema, orig_table);

    orig_schema_table = '"' || orig_schema || '"."' || orig_table || '"';
    new_schema_table = '"'|| new_schema || '"."' ||new_table || '"';
    for rec in
        select oid, conname
        from pg_constraint
        where contype = 'f' 
        and conrelid = orig_schema_table::regclass
    loop
        execute format(
            'alter table %I.%I add constraint %s %s',
            new_schema, new_table,
            replace(rec.conname, orig_schema_table, new_schema_table),
            pg_get_constraintdef(rec.oid));
    end loop;
end $$;


create or replace function ep_eval(_s text)
-- evaluates an expression given as string
-- https://stackoverflow.com/questions/43118512/eval-calculation-string-2-3-4-5-in-postgresql
returns numeric as $$
declare i numeric;
begin
    execute format('select %s', _s) into i;
    return i;
end;
$$ language plpgsql;
