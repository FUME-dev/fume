/*
Description: It creates core source FUME tables.
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

-- registry of emission inventories
create table if not exists "{source_schema}"."ep_inventories" (
    inv_id serial,
    inv_name text not null,
    description text,
    primary key (inv_id),
    unique (inv_name)
);

-- insert mandatory inventory
insert into "{source_schema}"."ep_inventories"
    (inv_id, inv_name, description)
    select 0, 'Geometries', 'Sets of common geometries'
      where not exists
        (select 1 from "{source_schema}"."ep_inventories" where inv_id = 0);
-- reset sequence back to 1
select setval(pg_get_serial_sequence('"{source_schema}"."ep_inventories"', 'inv_id'), 1, false)
    where not exists (select 1 from "{source_schema}"."ep_inventories" where inv_id > 0);


-- registry of emission inventory files
create table if not exists "{source_schema}"."ep_source_files"
(
    file_id serial,
    file_name text not null,
    inv_id integer not null,
    file_path text not null,
    file_table text not null,
    primary key (file_id),
    unique (file_name),
    foreign key (inv_id) references "{source_schema}"."ep_inventories"
);

-- registry of geometry sets
create table if not exists "{source_schema}"."ep_geometry_sets" (
    gset_id serial,
    gset_name text not null,
    gset_table text not null,
    gset_filter text not null,
    gset_path text not null,
    gset_info text[] not null,
    file_id integer not null,
    description text,
    primary key (gset_id),
    foreign key (file_id) references "{source_schema}"."ep_source_files",
    unique (gset_name)
);

-- geometry shapes definitions
create table if not exists "{source_schema}"."ep_in_geometries" (
    geom_id bigserial,
    gset_id integer not null,
    geom_orig_id text not null,
    geom geometry(Geometry,{srid}),  -- original SRS, includes SRID
    source_type char(1) not null,  -- ('P', 'A', 'L') for point, area or line sources
    weight double precision not null default 1,
    primary key (geom_id),
    unique (gset_id, geom_orig_id),
    foreign key (gset_id) references "{source_schema}"."ep_geometry_sets"
);
create index if not exists "ep_in_geometries_source_type" on "{source_schema}"."ep_in_geometries" (source_type);
create index if not exists "ep_in_geometries_geom" on "{source_schema}"."ep_in_geometries" using gist (geom);

-- scenarios list
create table if not exists "{source_schema}"."ep_scenario_list" (
      scenario_id serial,
      scenario_name text not null,          -- scenario name, to be used in conf files 
      scenario_file text not null,          -- file where the scenario definition is given 
      primary key (scenario_id),
      unique (scenario_name),
      unique (scenario_file)
);

create table if not exists "{source_schema}"."ep_scenario_filters" (
    filter_id serial,
    filter_definition  text not null default '',
    primary key (filter_id),
    unique (filter_definition)
);

-- scenarios definition
-- for each scenario id and combination of general attribute (from raw source files), cat_id, inventory specie gives a multiplicative factor. The scenario is then original specie value * factor
create table if not exists "{source_schema}"."ep_scenario_factors" (
      scenario_id int,          
      filter_id int,  -- attribute filter id
      cat_id bigint,      -- category for which the factor will be used
      spec_in_id int,     -- id of the input specie which will be multiplied by factor
      factor real not null,  -- multiplicative factor
      operation varchar(100),                     -- optional operation instead of multiplication
      unique (scenario_id, filter_id, cat_id, spec_in_id),
      foreign key (scenario_id) references "{source_schema}"."ep_scenario_list" (scenario_id),
      foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
      foreign key (spec_in_id) references "{conf_schema}"."ep_in_species",
      foreign key (filter_id) references "{source_schema}"."ep_scenario_filters" (filter_id)
);

-- registry of emission source sets
create table if not exists "{source_schema}"."ep_emission_sets" (
    eset_id serial,
    eset_name text not null,
    eset_info text not null,
    file_id integer not null,
    gset_id integer not null,
    eset_filter text not null default '',
    scenario_id integer[],
    vdistribution_id integer[],
    data_type char(1) not null,  -- ('E', 'C', 'D') emission, count (e.g. number of cars), density (e.g density of roads)
    primary key (eset_id),
    unique (eset_name),
    foreign key (file_id) references "{source_schema}"."ep_source_files",
    foreign key (gset_id) references "{source_schema}"."ep_geometry_sets"
);

-- mapping of input classification categories to internal categories
create table if not exists "{source_schema}"."ep_classification_mapping" (
    inv_id int,
    orig_cat_id text,
    cat_id bigint,
    primary key (inv_id, orig_cat_id, cat_id),
    foreign key (inv_id) references "{source_schema}"."ep_inventories",
    foreign key (cat_id) references "{conf_schema}"."ep_emission_categories" 
);

-- mapping of input inventory species categories to internal specie ids
create table if not exists "{source_schema}"."ep_in_specie_mapping" (
    inv_id int,
    orig_name text,
    spec_in_id int,
    unit text,
    conv_factor double precision, -- conversion factor to (g/s)
    primary key (inv_id, orig_name, spec_in_id),
    foreign key (inv_id) references "{source_schema}"."ep_inventories",
    foreign key (spec_in_id) references "{conf_schema}"."ep_in_species"
);

-- mapping of input activity units to internal activiyty units ids
create table if not exists "{source_schema}"."ep_activity_units_mapping" (
    inv_id int,
    orig_name text,
    act_unit_id int,
    unit text,
    conv_factor double precision, -- conversion factor to SI units
    primary key (inv_id, orig_name, act_unit_id),
    foreign key (inv_id) references "{source_schema}"."ep_inventories",
    foreign key (act_unit_id) references "{conf_schema}"."ep_activity_units"
);

-- sources definitions
create table if not exists "{source_schema}"."ep_in_sources" (
  source_id bigserial,
  eset_id integer not null,
  source_orig_id text not null,
  geom_id bigint not null,
  source_type char(1) not null,  -- ('P', 'A', 'L') for point, area or line sources
  primary key (source_id),
  unique (eset_id, source_orig_id),
  foreign key (eset_id) references "{source_schema}"."ep_emission_sets",
  foreign key (geom_id) references "{source_schema}"."ep_in_geometries"
);

-- emissions
create table if not exists "{source_schema}"."ep_in_emissions" (
  source_id bigint,
  spec_in_id integer,  
  cat_id bigint,
  emission float,    -- g/s 
  primary key (source_id, spec_in_id, cat_id),
  foreign key (source_id) references "{source_schema}"."ep_in_sources",
  foreign key (spec_in_id) references "{conf_schema}"."ep_in_species",
  foreign key (cat_id) references "{conf_schema}"."ep_emission_categories" 
);

-- point sources parameters
-- all in SI units
create table if not exists "{source_schema}"."ep_in_sources_point" (
  source_id bigint,
  height real,
  diameter real,
  temperature real,
  velocity real,  
  primary key (source_id),
  foreign key (source_id) references "{source_schema}"."ep_in_sources"
);

-- activity data
create table if not exists "{source_schema}"."ep_in_activity_data" (
  source_id bigint,
  act_unit_id integer,  
  cat_id bigint,
  act_intensity float,   
  primary key (source_id, act_unit_id, cat_id),
  foreign key (source_id) references "{source_schema}"."ep_in_sources",
  foreign key (act_unit_id) references "{conf_schema}"."ep_activity_units",
  foreign key (cat_id) references "{conf_schema}"."ep_emission_categories" 
);

-- table stores the equations (output_sp=coef1*input1_sp+coef*input2_sp...) used to calculate new pollutants after import
create table if not exists "{source_schema}"."ep_calculate_pollutants" (
    cat_id bigint not null,			-- source category id
    spec_out_id bigint,				-- inventory specie id of output specie
    spec_inp_id bigint,				-- inventory specie id of input specie
    coef float not null,			-- multiplicative coeficient for input specie
    cat_order integer not null,		-- order of equation in scope of the category
    unique (cat_id, spec_out_id, spec_inp_id),
    foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
    foreign key (spec_out_id) references "{conf_schema}"."ep_in_species" (spec_in_id),
    foreign key (spec_inp_id) references "{conf_schema}"."ep_in_species" (spec_in_id)
);

-- table stores the emission vertical distribution names
create table if not exists "{source_schema}"."ep_vdistribution_names" (
    vdistribution_id serial primary key,                -- vdistribution id
    vdistribution_name text,                            -- source vdistribution id
    unique (vdistribution_name)
);

-- table stores the vertical distributions definition (multiplicative factors)
create table if not exists "{source_schema}"."ep_vdistribution_factors" (
    vdistribution_id int not null,                      -- vdistribution id
    cat_id bigint not null,                     -- source category id
    level int not null,                         -- level number
    height float not null,                      -- level height (AGL)
    factor float not null,                      -- multiplicative factor for emission calculation
    primary key (vdistribution_id, cat_id, level),
    foreign key (vdistribution_id) references "{source_schema}"."ep_vdistribution_names" (vdistribution_id) ON DELETE CASCADE,
    foreign key (cat_id) references "{conf_schema}"."ep_emission_categories"
);
