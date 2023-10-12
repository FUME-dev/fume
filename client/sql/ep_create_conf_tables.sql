/*
Description: It creates core FUME tables.
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

-- list of models and their versions
create table if not exists "{conf_schema}"."ep_aq_models" (
  model_id serial,
  name text not null,
  version text not null,
  primary key (model_id),
  unique (name, version)
);

-- list of chemical mechanisms
create table if not exists "{conf_schema}"."ep_mechanisms" (
  mech_id serial,
  name text not null unique,
  description text,
  type text not null,
  primary key (mech_id)
);

-- list of model species that will be on output for defined model/mechanism
create table if not exists "{conf_schema}"."ep_mod_species" (
  spec_mod_id serial,
  model_id int not null,
  mech_id int not null,
  name text not null,
  description text,
  primary key (spec_mod_id),
  unique (model_id, mech_id, name),
  foreign key (model_id) references "{conf_schema}"."ep_aq_models",
  foreign key (mech_id) references "{conf_schema}"."ep_mechanisms"  
);

-- list of inventory species 
create table if not exists "{conf_schema}"."ep_in_species" (
  spec_in_id serial,
  name text not null unique,
  description text,
  primary key (spec_in_id)
);

-- list of known activity units
create table if not exists "{conf_schema}"."ep_activity_units" (
  act_unit_id serial,
  name text not null unique,
  description text,
  primary key (act_unit_id)
);

-- speciation profile species 
create table if not exists "{conf_schema}"."ep_sp_species" (
    spec_sp_id serial,
    mech_id int not null,      -- mechanism id
    name text not null,        -- name of the speciation profile specie
    primary key (spec_sp_id),
    unique (mech_id, name),
    foreign key (mech_id) references "{conf_schema}"."ep_mechanisms"
);

-- speciation profile specie to model specie mapping
create table if not exists "{conf_schema}"."ep_sp_mod_specie_mapping" (
  model_id int,
  mech_id int,
  spec_sp_name text,
  spec_mod_name text,
  map_fact real not null,
  primary key (model_id, mech_id, spec_sp_name, spec_mod_name),
  foreign key (model_id) references "{conf_schema}"."ep_aq_models",
  foreign key (mech_id) references "{conf_schema}"."ep_mechanisms",
  foreign key (mech_id, spec_sp_name) references "{conf_schema}"."ep_sp_species" (mech_id, name),
  foreign key (model_id, mech_id, spec_mod_name) references "{conf_schema}"."ep_mod_species" (model_id, mech_id, name)  
);

-- internal classification of emission categories (based on SNAP)
create table if not exists "{conf_schema}"."ep_emission_categories" (
  cat_id bigint not null,
  name text default '',
  parent bigint,
  description text,
  primary key (cat_id)
);

-- list of defined time profiles, all profiles (month/day/hour) are stored together)
create table if not exists "{conf_schema}"."ep_time_var" (
    tv_id int,          		    -- unique time variation profile id
    name text default '',           -- name of time variation profile
    resolution integer not null,    -- resolution of temporal profile, 3 - month, 2 - day, 1 - hour
    check (resolution = 1 or resolution = 2 or resolution = 3),
    primary key (tv_id)
);

create index if not exists "ep_time_var_resolution" on "{conf_schema}"."ep_time_var" (resolution);

-- time profile values
create table if not exists "{conf_schema}"."ep_time_var_values" (
    tv_id int,          	   	    -- time variation profile id
    period int,        			    -- time period number (1..12 for month time profile, 1 (Mon)..7 (Sun) day, 0..23 hour)
    tv_factor numeric not null,        -- time variation (multiplicative) factor that determines variance from average month, day or hour
									-- sum of tv_factors for one tv_id should be 12 (month), 7 (day) or 24 (hour)
    primary key (tv_id, period), 
    foreign key (tv_id) references "{conf_schema}"."ep_time_var" 
);

create index if not exists "ep_time_var_values_period" on "{conf_schema}"."ep_time_var_values" (period);

-- mapping of internal categories to time variation profiles
create table if not exists "{conf_schema}"."ep_time_var_mapping" (
    cat_id bigint,      		    -- source category id
    tv_id int,                      -- time variation profile id
    primary key (cat_id, tv_id), 
    foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
    foreign key (tv_id) references "{conf_schema}"."ep_time_var"
);

-- user defined time series (e.g. continual measurements from big sources)
create table "{conf_schema}"."ep_time_var_series" (
    cat_id bigint,
    time_loc timestamp,
    tv_factor numeric not null,
    primary key (cat_id, time_loc),
    foreign key (cat_id) references "{conf_schema}"."ep_emission_categories"
);

-- compounds for chemical mechanism speciations
create table if not exists "{conf_schema}"."ep_chem_compounds" (
      chem_comp_id int,   		  -- unique specie (compound) identifier
      name text not null, --unique,  -- specie name
      mol_weight real,            -- molecular weight of the specie
      non_vol int not null,       -- yes or no to non-volatile gas
      primary key (chem_comp_id)
);

-- assignment for chemical compunds to mechanism species
create table if not exists "{conf_schema}"."ep_comp_mechanisms_assignment" (
      mech_id int,      		   -- mechanism id
      chem_comp_id int,   		   -- chemical compound identifier
      spec_sp_id int,  		       -- air quality model lumped species identifier
      react_fact real not null,    -- the moles per mole
      primary key (mech_id, chem_comp_id, spec_sp_id),
      foreign key (mech_id) references "{conf_schema}"."ep_mechanisms",
      foreign key (chem_comp_id) references "{conf_schema}"."ep_chem_compounds",
      foreign key (spec_sp_id) references "{conf_schema}"."ep_sp_species"
);

-- compound speciation profiles for source categories
create table if not exists "{conf_schema}"."ep_comp_cat_profiles" (
      cat_id bigint,      -- source category id
      spec_in_id int,     -- id of the input specie which should be speciated by this profile
      chem_comp_id int,   -- chemical compound identifier
      fraction real not null,   -- weight fraction of specie in profile
      --primary key (cat_id, spec_in_id, chem_comp_id),
      foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
      foreign key (spec_in_id) references "{conf_schema}"."ep_in_species",
      foreign key (chem_comp_id) references "{conf_schema}"."ep_chem_compounds" 
);

-- table for storing input gspro files - speciation profiles. 
create table if not exists "{conf_schema}"."ep_gspro_sp_factors" (
      mech_id int,                      -- chemical mechanism id
      cat_id bigint,			-- source category id
      spec_in_id bigint,		-- inventory specie id
      spec_sp_id int,   		-- speciation profile specie identifier
      mole_split_factor real not null,	-- moles of model specie per mole of profile
      mol_weight real not null,		-- average molecular weight of the compound in profile (g/mol)
      mass_split_factor real not null,	-- mass fraction of the emitted specie represented by model specie
      primary key (mech_id, cat_id, spec_in_id, spec_sp_id),
      foreign key (mech_id) references "{conf_schema}"."ep_mechanisms",
      foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
      foreign key (spec_in_id) references "{conf_schema}"."ep_in_species",
      foreign key (spec_sp_id) references "{conf_schema}"."ep_sp_species"
);

-- meteorological variables
create table if not exists "{conf_schema}"."ep_met_names" (
      met_id serial,
      met_name text not null,
      met_desc text default '',
      met_dim int not null,
      primary key (met_id)
);

