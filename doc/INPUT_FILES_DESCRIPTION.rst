============================================
 Emission processor input files description
============================================

Static x configuration data
===========================
All static x configuration files are comma separated text files, with quotation marks to mark character strings. The files have one header row, with mandatory given column names. The order of the column is arbitrary, additional columns may be present. With the only excpetion which are gspro files, the files cannot contain comments nor empty lines. Empty lines are not allowed at the end of file neither (tricky!). Processor is case sensitive. 

Path to static (configuration) data is set in config parameter *input_params*.*static*.
Required files:

**Mandatory**

	- *inventory_species.csv* - list of inventory species, known internally by FUME
	- *emission_categories.csv* - list of defined emission sources categories
	- *model_list.csv* - list of chemical models (model, version)
	- *mechanism_list.csv* - list of chemical mechanisms
	- *model_specie_names.csv* - list of model species (known for model, version, mechanism)
	- *sp_species.csv* - list of speciation species for each chemical mechanism
	- *tv_def.csv* - time variation prolie definitions
	- *tv_values.csv* - time profile values
	- *tv_mapping.csv* - category-time variation profile assignment

For **speciations** either

	- *gspro files* - files with specie split factors

or

	- *speciation_profiles.csv* - compound category profiles, only required if speciation profiles (gspro files) are to be created by EP from basic compounds 
	- *compounds.csv* - list of known profile compounds, this is read when *speciation_profiles.csv* is given
	- *comp_mechanisms_assignment.csv* - assignment of compounds to chemical mechanisms, this is read when *speciation_profiles.csv* is given

or both.


**Optional**

	- *sp_mod_specie_mapping.csv* - maps speciation species to model species
	- *tv_series.csv* - explicit time factors 


General definition inputs
-------------------------

All files with general input definitions are expected to be in directory defined in *static* parameter. 

*inventory_species.csv*
	is a list of input emission species, known internally by FUME, unified between different emission inventories. The mapping between each emission inventory specie names is given for each inventory (see Emissions section). This allows to unify naming conventions across inventories like PM25, PM2_5 to be treated as one specie under one name. The file has two columns: name, description. Name defines the specie name, description can be empty. All species used in speciations as inventory specie or in calculate\_pollutants file must be listed here.

*emission_categories.csv* 
	defines the emission category (tree) hierarchy, known internally by EP. Similarly to species those categories are unified across different inventories through mapping file (see Emissions section). The file has three columns: cat\_id, name, parent. cat\_id is category id, is integer and each category has defined parent category. This is used for speciation and time dissagregation. In case the speciation or time profile is not found for given source category, FUME searches for the profile of parent category. This is recursive, thus the finally used profile can be several levels above the given category. If no speciation or time profile is found for any parent the sources of this category will not be on output (no warning given). parent\_id MUST be lower number than cat\_id for correct recursive parent category search.

*model_list.csv* 
	is a list of used chemical models. The file has two columns: model, version. This is user defined model name/version. Any model name appearing in othet input files/config must be listed here.


Speciation inputs
-----------------

All speciation input files are expected to be in *static*/speciations directory.

*model_specie_names.csv*
	list of model species (known for model, version, mechanism). The file has five columns: model, version, mechanism, name, description. This species will on output for defined model/mechanism. 

*sp_species.csv*
	list of speciation species for each chemical mechanism. The file has three columns: mechanism\_name, name, carbons (not used). Each speciation specie used in speciation process must be listed here. 

There are two ways of defining speciation profiles that can be combined together.

	#. profiles of basic compounds 
		Those files are consistent with output files of Chemical Speciation Database by Carter, 2015 (J Air Waste Manag Assoc, 65 (10)) for CMAS Speciation Tool.
		*speciation_profiles.csv*
			compound category profiles, only required if speciation profiles (gspro files) are to be created by FUME from basic compounds 

		*compounds.csv*
			list of known profile compounds, this is read when *speciation_profiles.csv* is given
	
		*comp_mechanisms_assignment.csv*
			assignment of compounds to chemical mechanisms, this is read when *speciation_profiles.csv* is given

	#. gspro files
		*gspro files*
			files with specie split factors


Temporal variation inputs
-------------------------


Shp time zones
-------------------------

 
Emissions
=========

Directory where input data for emission sources are stored is defined by 
configspec parameter *sources* (default is set same as *path* parameter).

List of all emission files to be imported are in configspec parameter *emission_inventories*.

Description of emission\_inventories file.
This file is a txt file, has exactly one header line and is tab separated.
It has 5 columns - inventory name, inventory version, short name for imported file, file name to be imported
(it is expected to be in sources directory) and infofile expected in the same place.
Lines beginning by # are ignored.

category + species files

emissions input files can be imported in one of two formats: text file, shapefile


inventory inputs list
---------------------

This file is a txt file, has exactly one header line and is tab separated.
It has 5 columns - inventory name, inventory version, short name for imported file, file name to be imported
(it is expected to be in input.sources directory) and infofile expected in the same place.

   **met**
   TODO

MEGAN
TODO
