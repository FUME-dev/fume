
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
    file_info text not null,
    gset_id integer not null,  -- cannot create foreign key as MEGAN has no gset attached and inserts gset_id = 0
    primary key (file_id),
    unique (file_name),
    foreign key (inv_id) references "{source_schema}"."ep_inventories"
);

-- registry of geometry sets
create table if not exists "{source_schema}"."ep_geometry_sets" (
    gset_id serial,
    gset_name text not null,
    gset_table text not null,
    gset_path text not null,
    gset_info text[] not null,
    description text,
    primary key (gset_id),
    unique (gset_name)
);

-- geometry shapes definitions
create table if not exists "{source_schema}"."ep_in_geometries" (
    geom_id bigserial,
    gset_id integer not null,
    geom_orig_id text not null,
    geom geometry(Geometry,{srid}),  -- original SRS, includes SRID
    source_type char(1) not null,  -- ('P', 'A', 'L') for point, area or line sources
    primary key (geom_id),
    unique (gset_id, geom_orig_id),
    foreign key (gset_id) references "{source_schema}"."ep_geometry_sets"
);
create index if not exists "ep_in_geometries_source_type" on "{source_schema}"."ep_in_geometries" (source_type);
create index if not exists "ep_in_geometries_geom" on "{source_schema}"."ep_in_geometries" using gist (geom);


-- registry of emission source sets
create table if not exists "{source_schema}"."ep_emission_sets" (
    eset_id serial,
    eset_name text not null,
    file_id integer not null,
    eset_filter text not null default '',
    data_type char(1) not null,  -- ('E', 'C', 'D') emission, count (e.g. number of cars), density (e.g density of roads)
    primary key (eset_id),
    unique (eset_name),
    foreign key (file_id) references "{source_schema}"."ep_source_files"
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

-- mapping of emission sources to time variation profiles
create table if not exists "{source_schema}"."ep_time_var_src_mapping" (
    source_id bigint,	            -- source id
    tv_id int,                      -- time variation profile id
    primary key (source_id, tv_id), 
    foreign key (source_id) references "{source_schema}"."ep_in_sources",
    foreign key (tv_id) references "{conf_schema}"."ep_time_var"
);

-- table stores the equations (output_sp=coef1*input1_sp+coef*input2_sp...) used to calculate new pollutants after import
create table if not exists "{source_schema}"."ep_calculate_pollutants" (
    cp_id serial primary key,		-- serial id
    cat_id bigint not null,			-- source category id
    spec_out_id bigint,				-- inventory specie id of output specie
    spec_inp_id bigint,				-- inventory specie id of input specie
	coef float not null,			-- multiplicative coeficient for input specie
	cat_order integer not null,		-- order of equation in scope of the category
	unique (cat_id, spec_out_id, spec_inp_id, cat_order),
    foreign key (cat_id) references "{conf_schema}"."ep_emission_categories",
    foreign key (spec_out_id) references "{conf_schema}"."ep_in_species" (spec_in_id),
    foreign key (spec_inp_id) references "{conf_schema}"."ep_in_species" (spec_in_id)
);

