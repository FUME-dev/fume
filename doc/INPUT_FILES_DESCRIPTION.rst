============================================
 Emission processor input files description
============================================

Input data configuration parameters 
===================================

Input data parameters are set in config file in *input_params* section. This section has five parameters that allow to set 
paths to different data types and three subsections for more detailed setup of speciation, meteorology and biogenic parameters.
The list of input data parameters is given in the Table 1.

.. csv-table:: Input config parameters
   :header: "Parameter", "Description", "Default"
   :widths: 25, 60, 15

   *path*, "allow to define path that can be used throughout the config file to ease setting other paths", input
   *static*, "path to different static and configuration data (if relative path is set, it is relative to run directory)", *path* value
   *sources*, "path to emission inventories  (if relative path is set, it is relative to run directory)", *path* value
   *emission_inventories*, "name of file (with path) with list of list of all emission files to be imported", \- 
   *tempfiles*, "path to directory with temporary, runtime files, not used FTM", "input/tmp"
   **speciation_params**
   *gspro_files*, "list of all gspro files to be imported", 
   **met**
   TODO
   \...
   **biogen_params**
   *megan_base_dir*, , *path* value
   *megan_input_dir*, , *path* value
   *megan_lai*, , 
   *megan_pft*, , 
   *megan_ef*, ,
   *megan_met*, ,
   *megan_out*, ,
   *megan_temp_dir*, ,*path* value
   *srid*, , -1
   *proj4*, ,
   *megan_category*, category that will be given to output megan emission,



Static x configuration data
===========================
All static x configuration files are comma separated text files, with quotation marks to mark character strings. The files have one header row, with mandatory given column names. The order of the column is arbitrary, additional columns may be present.
Path to static (configuration) data is set in config parameter *input_params*.*static* 
(default is set same as *path* parameter).
Required files:

**Mandatory**

	- *inventory_species.csv* - list of inventory species, known internally by EP
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
	is a list of input emission species, known internally by EP. The file has two columns: name, description. Name defines the specie name, description can be empty.

*emission_categories.csv* 
	defines a emission category hierarchy, known internally by EP. The file has three columns: cat\_id, name, parent.  

*model_list.csv* 
	is a list of used chemical models. The file has two columns: model, version.


Speciation inputs
-----------------

All speciation input files are expected to be in *static*/speciations directory.

*model_specie_names.csv*
	list of model species (known for model, version, mechanism)

*sp_species.csv*
	list of speciation species for each chemical mechanism

There are two ways of defining speciation profiles that can be combined together.

	#. profiles of basic compounds
		*speciation_profiles.csv*
			compound category profiles, only required if speciation profiles (gspro files) are to be created by EP from basic compounds 

		*compounds.csv*
			list of known profile compounds, this is read when *speciation_profiles.csv* is given
	
		*comp_mechanisms_assignment.csv*
			assignment of compounds to chemical mechanisms, this is read when *speciation_profiles.csv* is given

	#. gspro files
		*gspro files*
			files with specie split factors


Temporal variation inputs
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
