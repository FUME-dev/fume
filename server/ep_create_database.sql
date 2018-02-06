
\echo 'Connected to database'
select * from information_schema."information_schema_catalog_name";

\echo 'Register needed EP functions';
\i ep_create_grid.sql;
\i ep_intersection.sql;
\i ep_mask.sql;
\i ep_case_proc.sql;
\i ep_timezones.sql;
\i ep_process_sources.sql;
\i ep_emission_output.sql
commit;


