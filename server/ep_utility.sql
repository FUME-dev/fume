/*
Description: It creates auxiliary FUME sql functions.
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
