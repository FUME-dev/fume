create or replace function ep_process_sources(
    schema text,
    table_raw text,
    eset_id integer,
    category text,
    category_def text,
    point_par text[][])
  returns void as 
$$
declare
	inv_id integer;
	gset_id integer;
	category_id bigint;
	data_type char;
    item text;
    i integer;
    spec_names text[];
    spec_names_s text;
    par_names_s text;
    par text;
    sqltext text;
    data_table text;
    specmap_table text;
    spec_id text;
    value_col text;
    t1 timestamp;
    t2 timestamp;
    t3 timestamp;
    t4 timestamp;
    t5 timestamp;
    t6 timestamp;
begin

    raise notice 'ep_process_sources start';
    execute 'select clock_timestamp()' into t1;
    -- find inventory and geometry set id
    execute format('SELECT f.inv_id, f.gset_id
                      FROM %I.ep_emission_sets s
                      JOIN %I.ep_source_files f ON f.file_id = s.file_id
                      WHERE eset_id = $1', schema, schema) into inv_id, gset_id using eset_id;
    execute 'select clock_timestamp()' into t2;
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
    execute 'select clock_timestamp()' into t3;

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

    -- find species defined in specie_mapping present in raw table
    execute format('SELECT ARRAY ( SELECT orig_name FROM %I.%I
                            JOIN (SELECT column_name AS orig_name FROM information_schema.columns
                            WHERE table_name = $1) AS r USING (orig_name)
                            WHERE inv_id = $2)', schema, specmap_table) USING table_raw, inv_id INTO spec_names;
    execute 'select clock_timestamp()' into t4;
    -- prepare spec_names string
    foreach item in array spec_names loop
        spec_names_s = concat_ws(', ', spec_names_s, format('i.%I', item));
    end loop;
    spec_names_s = 'unnest(array[' || spec_names_s || '])';

    -- populate emission table
    if category is null then          -- category is default (predefined) for this table
        execute format('SELECT cat_id FROM %I.ep_classification_mapping WHERE inv_id = $1 AND orig_cat_id = $2', schema) into category_id using inv_id, category_def;

        sqltext = format('INSERT INTO %I.%I (source_id, %I, cat_id, %I)
                          SELECT source_id, %I, $1, (replace(emission::text, '','', ''.'')::double precision)*conv_factor
                          FROM (SELECT source_orig_id, unnest($2) AS orig_name, '
                              || spec_names_s || ' AS emission
                              FROM %I AS i) AS e
                              JOIN (SELECT * FROM %I.%I WHERE inv_id = $3) AS m USING (orig_name)
                              JOIN %I.sid_map AS s ON s.source_orig_id = e.source_orig_id
                              WHERE (emission::text = '''') IS FALSE AND replace(emission::text, '','', ''.'')::double precision > 0',
                      schema, data_table, spec_id, value_col, spec_id, table_raw, schema, specmap_table, schema);
        raise notice 'sqltext 2: %, %, %, %', sqltext, category_id, spec_names, inv_id;
        execute sqltext using category_id, spec_names, inv_id;
    else                               -- category is read from table
        sqltext = format('INSERT INTO %I.%I (source_id, %I, cat_id, %I)
                          SELECT source_id, %I, cat_id, (replace(emission::text, '','', ''.'')::double precision)*conv_factor
                          FROM (SELECT source_orig_id, unnest($1) AS orig_name, '
                              || spec_names_s || ' AS emission, %I
                              FROM %I AS i) AS e
                              JOIN (SELECT * FROM %I.%I WHERE inv_id = $2) AS m USING (orig_name)
                              JOIN %I.sid_map AS s ON s.source_orig_id = e.source_orig_id
                              JOIN (SELECT * FROM %I.ep_classification_mapping WHERE inv_id = $2) AS cat ON cat.orig_cat_id=e.%I
                              WHERE (emission::text = '''') IS FALSE AND replace(emission::text, '','', ''.'')::double precision > 0',
                        schema, data_table, spec_id, value_col, spec_id, category, table_raw, schema, specmap_table, schema, schema, category);
        raise notice 'sqltext 3: %, %, %', sqltext, spec_names, inv_id;
        execute sqltext using spec_names, inv_id;
    end if;
    execute 'select clock_timestamp()' into t5;
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
    raise notice 'sqltext: %', sqltext;
    execute sqltext;
    execute 'select clock_timestamp()' into t6;
    raise notice 'ep_process_sources timing1: %, %, %, %, %, %',t1,t2,t3,t4,t5,t6;
    raise notice 'ep_process_sources timing2: %, %, %, %, %, %',t6-t1,t2-t1,t3-t2,t4-t3,t5-t4,t6-t5;
end;
$$ language plpgsql;
