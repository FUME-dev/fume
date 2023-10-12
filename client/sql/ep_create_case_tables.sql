/*
Description: It creates case FUME tables.
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

-- grid_tz table represents case grid intersected with timezones
create table if not exists "{case_schema}"."ep_grid_tz" (
    grid_id serial,
    i integer,
    j integer,
    tz_id integer,
    geom geometry(MULTIPOLYGON,{srid}),
    primary key (grid_id)
);
-- create indices
create index if not exists "ep_grid_tz_geom" on "{case_schema}"."ep_grid_tz" using gist(geom);
create index if not exists "ep_grid_tz_i_j_tz_id" on "{case_schema}"."ep_grid_tz" (i,j,tz_id);

-- ep_grid_env table represents case grid envelope
create table if not exists "{case_schema}"."ep_grid_env" (
    geom geometry(POLYGON,{srid})
);
-- create indices
create index if not exists "ep_grid_env_geom" on "{case_schema}"."ep_grid_env" using gist(geom);

-- list of species that will/can be in ouput file
create table if not exists "{case_schema}"."ep_out_species" (
  spec_id int,
  name text not null unique,
  primary key (spec_id),
  foreign key (spec_id) references "{conf_schema}"."ep_mod_species" (spec_mod_id)
);

CREATE TABLE if not exists "{case_schema}"."ep_intersect_factors" (
    factor_id BIGSERIAL NOT NULL,
    geom_id BIGINT NOT NULL,
    grid_id BIGINT NOT NULL,
    sg_factor FLOAT NOT NULL DEFAULT 0.,
    geom GEOMETRY,
    primary key (factor_id),
    foreign key (geom_id) references "{source_schema}"."ep_in_geometries" (geom_id),
    foreign key (grid_id) references "{case_schema}"."ep_grid_tz" (grid_id)
);

-- cache for precalculated time zone shifts
create table if not exists "{case_schema}"."ep_time_zone_shifts" (
    ts_id integer,
    time_out timestamptz,
    time_loc timestamp,
    primary key (ts_id, time_out)
);

-- cache for precalculated time factors
create table if not exists "{case_schema}"."ep_time_factors" (
    cat_id bigint,
    time_loc timestamp,
    tv_factor numeric not null,
    primary key (cat_id, time_loc),
    foreign key (cat_id) references "{conf_schema}"."ep_emission_categories"
);

-- speciation split factors - converts inventory specie to speciation profile specie
create table if not exists "{case_schema}"."ep_sp_factors" (
      cat_id bigint,  		         -- source category id
      spec_in_id bigint,   			 -- inventory specie id
      spec_sp_id integer,    -- speciation profile specie identifier
      split_factor real not null,     -- multiplicative split factor 
      mol_weight real not null,            -- molecular weight of the specie, in case it is needed for recalculation back to mass unit in output (e.g. for PALM), please note that multiplying the value after speciation process is tricky for VOC as it is not clear what the molar weight of model lumped specie is. This is intended to be used for "straightforward" species as NO2, SO2 etc.
      primary key (cat_id, spec_in_id, spec_sp_id),
      foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
      foreign key (spec_in_id) references "{conf_schema}"."ep_in_species",
      foreign key (spec_sp_id) references "{conf_schema}"."ep_sp_species"
);

-- speciation split factors - converts inventory specie to model/mechanism (output) specie 
create table if not exists "{case_schema}"."ep_mod_spec_factors" (
      cat_id bigint,       			  -- source category id
      spec_in_id bigint,   	  		  -- inventory specie id
      spec_mod_id integer,  		  -- air quality model lumped species identifier
      split_factor real not null,     -- multiplicative split factor 
      mol_weight real not null,            -- molecular weight of the specie, in case it is needed for recalculation back to mass unit in output (e.g. for PALM), please note that multiplying the value after speciation process is tricky for VOC as it is not clear what the molar weight of model lumped specie is. This is intended to be used for "straightforward" species as NO2, SO2 etc.
      primary key (cat_id, spec_in_id, spec_mod_id),
      foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
      foreign key (spec_in_id) references "{conf_schema}"."ep_in_species",
      foreign key (spec_mod_id) references "{conf_schema}"."ep_mod_species"
);

-- registry of external models
create table if not exists "{case_schema}"."ep_ext_models" (
    ext_mod_id serial,
    ext_mod_name text,
    primary key (ext_mod_id),
    unique (ext_mod_name)
);

create table if not exists "{case_schema}"."ep_transformation_chains" (
    chain_id serial,
    name varchar(255) not null,
    primary key (chain_id),
    unique (name)
);

create table if not exists "{case_schema}"."ep_transformation_chains_scenarios" (
    chain_id int not null,
    scenario_id int not null,
    primary key (chain_id, scenario_id),
    foreign key (chain_id) references "{case_schema}"."ep_transformation_chains" (chain_id),
    foreign key (scenario_id) references "{source_schema}"."ep_scenario_list" (scenario_id)
);

create table if not exists "{case_schema}"."ep_transformation_chains_levels" (
    chain_id int not null,
    vertical_level int not null,
    primary key (chain_id, vertical_level),
    foreign key (chain_id) references "{case_schema}"."ep_transformation_chains" (chain_id)
);

-- emissions in grid before speciation and time disaggregation
create table if not exists "{case_schema}"."ep_sources_grid" (
  sg_id bigserial,
  source_type char(1) not null,
  source_id bigint,
  ext_mod_id int,
  grid_id integer not null,
  k integer,
  sg_factor real not null,
  transformation_chain integer null,
  primary key (sg_id),
  foreign key (source_id) references "{source_schema}"."ep_in_sources" (source_id),
  foreign key (ext_mod_id) references "{case_schema}"."ep_ext_models" (ext_mod_id),
  check (source_id is not null or ext_mod_id is not null),
  foreign key (grid_id) references "{case_schema}"."ep_grid_tz" (grid_id),
  foreign key (transformation_chain) references "{case_schema}"."ep_transformation_chains" (chain_id)
  -- unique (source_id, grid_id, k)
);

-- emissions in grid before speciation and time disaggregation
create table if not exists "{case_schema}"."ep_sg_emissions" (
  sg_id bigint,
  spec_in_id integer,
  cat_id bigint,
  emiss float,      -- g/s
  primary key (sg_id, spec_in_id, cat_id),
  foreign key (spec_in_id) references "{conf_schema}"."ep_in_species",
  foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
  foreign key (sg_id) references "{case_schema}"."ep_sources_grid"
);

-- activity data in grid before speciation and time disaggregation (i.e. before an external module treats them)
create table if not exists "{case_schema}"."ep_sg_activity_data" (
  sg_id bigint,
  act_unit_id integer,
  cat_id bigint,
  act_intensity float,     
  primary key (sg_id, act_unit_id, cat_id),
  foreign key (act_unit_id) references "{conf_schema}"."ep_activity_units",
  foreign key (cat_id) references "{conf_schema}"."ep_emission_categories", 
  foreign key (sg_id) references "{case_schema}"."ep_sources_grid"
);

-- emissions in grid after speciation before time disaggregation
create unlogged table if not exists "{case_schema}"."ep_sg_emissions_spec" (
  sg_id bigint,
  spec_id integer,
  cat_id bigint,
  emiss float,   -- mol/s for gases, g/s for aerosols
  primary key (sg_id, spec_id, cat_id),
  foreign key (spec_id) references "{case_schema}"."ep_out_species",
  foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
  foreign key (sg_id) references "{case_schema}"."ep_sources_grid"
);

-- emissions in grid after speciation and time disaggregation
create unlogged table if not exists "{case_schema}"."ep_sg_out_emissions" (
  sg_id bigint,
  spec_id integer,
  cat_id bigint,
  time_out timestamp ,
  emiss float,   -- mol/s for gases, g/s for aerosols
  primary key (sg_id, spec_id, cat_id, time_out),
  foreign key (spec_id) references "{case_schema}"."ep_out_species",
  foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
  foreign key (sg_id) references "{case_schema}"."ep_sources_grid"
);


create unlogged table if not exists "{case_schema}"."ep_out_emissions_array" (
    time_out timestamptz,
    emissions float[][],
    primary key (time_out)
);


-- point sources parameters
-- all in SI units
-- point sources parameters, filled in missing values
create table if not exists "{case_schema}"."ep_sources_point" (
  sg_id bigint,
  xstk real,
  ystk real,
  lon real,
  lat real,
  height real,
  diameter real,
  temperature real,
  velocity real,  
  primary key (sg_id),
  foreign key (sg_id) references "{case_schema}"."ep_sources_grid"
);


-- default point sources parameters (height, diameter, temperature, velocity) 
-- used in fill_missing_point_parameters
create table if not exists "{case_schema}"."ep_default_point_params" (
  cat_id bigint,
  spec_in_id bigint,    	-- this specie is used to determine the "size" of the source
  lim real, 			-- if the emission of spec_in is higher then limit (but lower than next limit) use these parameters
  height real,
  diameter real,
  temperature real,
  velocity real,  
  primary key (cat_id, lim),
  foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
  foreign key (spec_in_id) references "{conf_schema}"."ep_in_species"
);


-- point sources on output
/*create table if not exists "{case_schema}"."ep_point_out_sources" (
  source_id bigint,
  lon float not null,
  lat float not null,
  xstk float not null,
  ystk float not null,
  hstk float not null,
  dstk float not null,
  tstk float not null,
  vstk float not null,
  pig  boolean not null default 'false',
  flow float not null, -- CMAQ - mandatory; CAMx - zero: ignored, positive: real stack flow rate (m3/hr) for plume rise calculations, negative: real plume bottom (m)
-- CAMx specific flags
  src_apportionment boolean, -- OSAT/PSAT source apportionment
  plmht float, -- Zero or positive: ignored (plume rise calculation is performed); Negative: real plume top (m) for vertical plume distribution override 
  primary key (source_id),
  foreign key (source_id) references "{source_schema}"."ep_in_sources" 
);

-- point sources on output - emission values FIXME do we need this table?
create table if not exists "{case_schema}"."ep_point_out_emissions" (
  source_id bigint,
  spec_id   integer,
  time timestamp,
  emission  float,
  primary key (source_id, spec_id, time),
  foreign key (source_id) references "{case_schema}"."ep_point_out_sources", 
  foreign key (spec_id)   references "{case_schema}"."ep_out_species" 
);*/
-- vertical disaggregation information
create table if not exists "{case_schema}"."ep_vertical_factors" (
  cat_id bigint,
  level integer not null check (level >= 0),
  factor real not null,
  primary key (cat_id, level),
  foreign key (cat_id) references "{conf_schema}"."ep_emission_categories"
);

-- meteorological fields
create table if not exists "{case_schema}"."ep_met_data" (
  met_id int,
  i integer, -- indexed from 0
  j integer, --  -"-
  k integer,--   -"-
  time timestamp,
  value float,
  primary key (met_id),
  foreign key (met_id) references "{conf_schema}"."ep_met_names" (met_id)
);

-- table stores the vertical distributions definition (multiplicative factors) for the case vertical grid
create table if not exists "{case_schema}"."ep_vdistribution_factors_out" (
    vdistribution_id int not null,                      -- vdistribution id
    cat_id bigint not null,                     -- source category id
    level int not null,                         -- level number
    factor float not null,                      -- multiplicative factor for emission calculation
    primary key (vdistribution_id, cat_id, level),
    foreign key (vdistribution_id) references "{source_schema}"."ep_vdistribution_names" (vdistribution_id) ON DELETE CASCADE,
    foreign key (cat_id) references "{conf_schema}"."ep_emission_categories"

);

-- views
-- FIXME get_species is used in the context of both total emissions and area emissions
CREATE OR REPLACE VIEW "{case_schema}".get_species AS
    SELECT DISTINCT em.spec_id, spec.name FROM "{case_schema}".ep_sg_emissions_spec AS em
           INNER JOIN "{case_schema}".ep_out_species AS spec ON em.spec_id=spec.spec_id
           ORDER BY em.spec_id;

CREATE OR REPLACE VIEW "{case_schema}".get_species_point AS
    SELECT DISTINCT em.spec_id, spec.name FROM "{case_schema}".ep_sg_emissions_spec AS em
    JOIN "{case_schema}".ep_out_species AS spec ON em.spec_id=spec.spec_id
    JOIN "{case_schema}".ep_sources_grid  AS src ON src.sg_id=em.sg_id
    WHERE src.source_type = 'P'
    ORDER BY em.spec_id;

CREATE OR REPLACE VIEW "{case_schema}".get_categories AS
    SELECT cat_id, name FROM "{conf_schema}".ep_emission_categories
    WHERE cat_id IN (SELECT DISTINCT cat_id FROM "{case_schema}".ep_sg_emissions_spec)
    ORDER BY cat_id;
