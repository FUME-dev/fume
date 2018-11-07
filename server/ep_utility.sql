CREATE OR REPLACE FUNCTION ep_drop_all_functions() RETURNS VOID AS
$ep_drop_all_functions$
DECLARE
    sql text;
    fn_ns text;
    fn_name text;
    fn_args text;
BEGIN
    FOR fn_ns, fn_name, fn_args IN SELECT ns.nspname, proname, oidvectortypes(proargtypes) FROM pg_proc INNER JOIN pg_namespace ns ON (pg_proc.pronamespace = ns.oid) WHERE proname LIKE 'ep_%'
    LOOP
        sql := 'DROP FUNCTION ' || fn_ns || '.' || fn_name || '(' || fn_args || ');';
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;
END;
$ep_drop_all_functions$ LANGUAGE plpgsql;
