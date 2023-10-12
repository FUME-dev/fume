/*
Description: The main sql script for registration of the needed sql function in the sql database.
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

\echo 'Connected to database'
select * from information_schema."information_schema_catalog_name";

\echo 'Register needed EP functions';
\i ep_create_grid.sql;
\i ep_intersection.sql;
\i ep_mask.sql;
\i ep_case_proc.sql;
\i ep_timezones.sql;
\i ep_process_sources.sql;
\i ep_emission_output.sql;
\i ep_utility.sql;
commit;


