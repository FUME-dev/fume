
# FUME emission processor user guide

Introduction
-----------------

The present *New Generation  Emission  Processor* (FUME) has been developed by a consorcium of Czech Technical university (CIIRC and Transportation Faculty), Czech Hydrometeorolical institute and Dept. of Atmospheric Physics, Faculty of Mathematics and Physics, Charles University, Prague, under support of Technological Agency of the Czech Rep.


It uses newly available data sources, among others, a database of the spatial distribution of ammonia emissions from  farming  machines,  based  on  our knowledge of the location of the various sources in the  Czech  Republic.      A new methodology for quantifying  emissions  from  transport has been developed, using current traffic data and data from vehicle units. This methodology reflects the  actual  behaviour  of  vehicles  on  various types of roads, including variations over time.

Other new functionalities include:

 * link to GIS data
 * masking and filtering capabilities
 * nontrivial ways of combining different data sources
 * visualisation module
 * support for air quality models CMAQ and CAMx with support to coupling to other models, e.g. PALM
 * coupling with numeric weather models ALADIN and WRF


Emisson processor uses server-client architecture to perform most of the tasks. Server holds the database with input and auxiliary data, client is used to configure the tasks which should lead to transforming the input data to desired emission outputs (typically for the use by CTM).

The FUME processor is written in Python and PostgreSQL/PostGIS. Python scripts operate on a database and GIS data and the system is higly flexible and configurable.

Typical workflow contains following steps:

* only done once: - downloading code - setting up the database server
* done regularly: - configuring processor - running the processor


met@romeo.fd.cvut.cz:/storage/shared/repo.git/emisproc

At present two widely used CTM models are supported:
* CAMx with the chemical mechanism CB05 and CF (coarse/fine) module for particulate matter 

* CMAQ with CB05 and AES for particulate matter. 

For these configurations the output of the emission processor can be readily used in CTM simulations. 

For the context of the user documentation following conventions are being used::

FUME_ROOT specifies the FUME codebase directory (typically the local git repository clone)
FUME_CASE specifies the run directory for the user case outside of the FUME codebase


Installation
-------------
Prerequisities:

* python3 (3.5 or higher)
* database server: packages postgresql, postgresql-contrib 9.5 or newer
* postgis (version 1.5.3 tested)

Other libraries for python

* pip, pyproj, psycopg2, pygrib, netcdf4, gdal, pint, configobj, scipy, pytz, numpy, python-cdo

Optional libraries for plotting (only necessary when EmissPlotter postprocessors are used)

* **matplotlib**, basemap, shapely, fiona

Some of the python packages mentioned above may not be present depending on distribution. They may be installed via pip or the native package manager, e.g. on SUSE:


~~~

    pip install configobj
    zypper install python3-psycopg2

~~~

If using the anaconda distribution, it may be necessary to use conda-forge:

~~~

    conda install -c conda-forge pygrib pint gdal
~~~


Extension for pip3:
~~~

    pip3 install pytz

~~~

On some linux distributions a repository must be added, e.g. on SUSE leap 42.2: 

~~~

    zypper addrepo http://download.opensuse.org/repositories/Application:Geo:Staging/openSUSE_Leap_42.2/Application:Geo:Staging.repo zypper refresh zypper install postgresql94-postgis
    zypper refresh
    zypper install postgresql94-postgis

~~~


Tools for administration and visualisation

* qgis (+ python-qgis) for data manipulation
* pgadmin3 (access to databases)


Installation of FUME from github (nebo z jineho mista?)

* downloading FUME git pull from git repository of emisproc
* budeme umoznovat tarball ?


Input data
-------------
Input data are typically located in the directory $FUME_CASE, which is the user case run directory. The input directory contains inventories, meteo inputs and static data like speciacions and time schedules.
[//]: Asi bude nutne poskytnout nejaky test case

For the test case provided with the distribution, input data are downloadable from ...
After the download you should see the following structure:

~~~

$FUME_CASE
                /megan
                /meteo
                /static_data
                fume_run.conf

~~~

Quick start
--------------
If not already started, start (possibly as root) the postgres sql server

~~~

systemctl start postgresql

~~~
or possibly with version number:
~~~
systemctl start postgresql-9.6
~~~

Create database:

* You have to either copy intialization scripts in directory *server* somewhere where user postgres can read it or give user postgress read access to server directory, e.g.
  
~~~

    cp -ra emisproc/server /var/lib/pgsql/ 

~~~
as root on SUSE-based linuxes

* Switch to user "postgres": 

~~~

   sudo -u postgres -i

~~~

or if you don't use sudo:

~~~

   su; su - postgres

~~~

* run the script

~~~

$FUME_ROOT/server/ep_create_database.sh

~~~

Alternatively instead of running the script: 

~~~

sudo su - postgres
createuser -P username   # asks for password
createdb -E UTF8 -O *username* [database-name]
createlang postgis [database-name]
createlang postgis_topology [database-name]
createlang intarray [database-name]
psql -d [user] -c "grant all on database [database-name] to [user] with grant option;" (PostgreSQL syntax for adding full privileges to [user] for database [database-name])
psql -d [database-name] -c "grant all on spatial_ref_sys to [user];"
~~~

logout from the database, the rest is done under account *username*

~~~
\q

cd $FUME_ROOT/server

psql -h <hostname> -p <port> -U <username> [-W] -d <dbname> -f ep_create_database.sql 

~~~

Run the emission processor:

Create a working directory FUME_CASE (can be changed). For the test case, copy example configuration files from $FUME_ROOT/doc/example-config

cd $FUME_CASE

and run, generally

~~~

python3 $FUME_ROOT/client/fume [-c main_config] [-w workflow_config]

~~~

In particular for the test case provided with the distribution (nejaky maly test case - idealne TNO emise, ktere jsou volne dostupne)

~~~

python3 $FUME_ROOT/client/fume -c fume_run.conf -w fume_workflow.conf

~~~

Architecture/Philosophy/Structure of the emission processing system
--------------------------------------------------------------------

The emission processor is a complex, heterogeneous system consisting of many submodules of different character. These submodules treat different types of sources and process data in many particular formats.

The main and primary task of the FUME emission processor is to produce inputs for air quality models. The processor has been designed so as to keep the vast majority of the code independent of any particular air quality model. The coupling to the target AQ model is left to the very end of processing chain. Thus the outputs of FUME may be used for  Eulerian CTMs (CAMx and CMAQ supported so far) but the adaptation to Lagrangian and Gaussian models may be achieved with no overhead efforts.
This approach has been applied for processing sources as well as for spatial and temporal processing of emissions, speciations and final generation of emission flows. FUME supports GIS technology and is not restricted to regular 3D grids and its outputs thus may be used for survey and reporting tasks, e.g. for administrative units or other partitioning defined by the user.

The processor implements the widely accepted disaggregation model for emission flows: 

T(p,l,t,s) = Sum_{i,j} [ Z(i,j).q_p (i,j).q_l (i,j).q_t (i,j).q_s (i,j) ],

where 
T(p,l,t,s) is output emission flow for a given polygon p, vertical level l, time t and output species s,

Z(i,j) is a primary emission of emitted species j from source i (typically in tons per year)
q p (i,j) disaggregation coefficient for emitted species j from source i into polygonu p
q l (i,j) disaggregation coefficient for emitted species j from source i into vertical level l
q t (i,j) disaggregation coefficient for emitted species j from source i into time t
q s (i,j) disaggregation coefficient for emitted species j from source i into output species s.

For certain types of emission sources this model is not appropriate, e.g. biogenic emissions, emissions from lightning etc. For those cases special models exist, which are based on domain knowledge. These are naturally out of scope of FUME and an interface has been built into FUME for external models. Currently the MEGAN biogenic emission model has a built-in support in FUME. 

*PLUME RISE?*

*NH3, lightning?*


The database structure of an emission database contains several schemas:

* case schema
* configuration schema
* sources schema
* static schema
* topology schema

Normally the user doesn't need to change the schemas. Currently the schemas conf_test, static_test, sources_test and case_test are provided and all what is needed is to name them in the main configuration file. 

Advanced users may create their own schemas and specify single schemas for each run in the main configuration file (fume_run.conf), e.g.

~~~

conf_schema = conf_test 
static_schema = static_test
source_schema = sources_test
case_schema = case_test

~~~

This means (among others) that the user can modify some parts of the simulation without the necessity of running all processing from scratch.

To accomplish a simulation, the user needs to

* define the output grid
* manage the sources specification (see "Treatment of emission sources below").
  All of the specified sources will be included into processing

* define the transformation chains. During this step any filtering and masking of the sources will be done
* run the simulation. All emission outputs will be summed up and added to the user grid. Two files will be output (unless supressed) - Area emissions and Point emissions. 

It is the responsibility of the user to make sure that no erratic overlapping of emission outputs will occur. For example, if one uses finer inventories for the domain of interest while for the background a coarser inventory is available, the user has to mask out the finer domain from the coarser inventory. So far, the system doesn't detect these situations nor does produce any warnings of this kind. 



User configuration
------------------------------------

As mentioned above, user configuration typically resides in the $FUME_CASE directory, however, the user can specify any path to the main configuration file with the ``-c`` option (defaults to ``fume_run.conf``) and the path to the workflow configuration file with the ``-w`` option (defaults to ``fume_workflow.conf``). In the distribution, example configuration files are provided in the doc/example-config directory and full configuration specification files (templates) are located in the directory client/conf.

There is one main configuraion file, typically named fume_run.conf. The name may be supplied via the ``-c`` option to the main executable ``fume``.

A template for this file including all valid specifications is the file ``client/conf/configspec.conf``.

There are three other configuration files, namely: 

*VYJASNIT STRUKTURU konfiguraku*
*mohou se uz includovat?*

* fume_workflow.conf
* fume_transformations.conf 
* configspec-sources.conf
* configspec-cmaq.conf

The definitions (syntax) of any valid transformation is in the file config/transformations/configspec-transformations.conf. Using these definitions, the users may create their own file e.g. fume_transformations.conf where user-defined transformations are defined. These may then appear in transformation chains (see below).  


configspec.conf contains the following specifications:

* paths to input data, in particular inventories, static data, meteorology files, biogenic emisssion files
* parameters for connecting the PostgreSQL database
* parameters for connecting the PostgreSQL database and names of schemas
* specifications of grids in meteorological inputs as well as output user grid
* projection parameters (tady neni jasne ktereho gridu se projekce tyka)
* output specifications
* transformation chains to be performed

 




**Treatment of emission sources**

In order to minimize the efforts needed for incorporation of user-contributed or in-house inventories of the users, the system enables different formats of inventories, different projections of each source etc. 

The main configuration file responsible for processing of inventories (raw sources) is the file *inventory_input.txt* typically located in the FUME_CASE directory (full path including file name may be changed, it is specified in the main config in the input_params/emission_inventories option). In this file, location of the raw data and metadata is specified together with optional information on grouping/filtering the raw data. 


Any row of the  *inventory_input.txt* file has the following columns:
~~~
"inventory_name"  "file_name"  "file_path"  "file_info_path"  "set_name (opt.)" "filter (opt.)"
~~~

Thus a sample row from TNO inventory looks like this:
~~~


"TNO_III"	"TNO_MACC_III_v1_1_2011"	"TNO/TNO_MACC_III_emissions_v1_1_2011_ID.txt"	"TNO/TNO_MACC_III_emissions_v1_1_2011_ID.info"	"TNO_MACC_III_v1_1_2011_P"	"SourceType=P"

~~~

The file_info_path indicates the location of user-provided metadata *\*.info*, where the information on format, source type and geometry is stored. Below we see a sample info file:

~~~

# type of file
file_type = text
field_delimiter = ','
text_delimiter  = '"'
encoding = 'utf8'

# number of lines before header to be skipped
skip_lines = 0

# source type
src_type = A

category_def = 6
source_id = ZUJ,

# geometry
geom_name = 'CZ_ZUJ'

~~~ 

Valid values for source type and geometry are stored in the file *configspec-sources.conf*. Thus, for example, valid values for type are A(area sources), P(point sources) and L(line sources).

For technical reasons it is handy to include the description of geometry files at the beginning of the file *inventory_input.txt*. These rows have empty name of inventory. In this way sharing of geometry among different sources is easily treated. 

main config file : output files and other paths. The paths must be created in advance. 


Transformations
----------------
these do not change values, e.g. of emissions. 
Any transformation may be restricted to a inventory, set or category. 

The transformation configuration file , (e.g. fume_transformations.conf
(name specified in the main configuration file in the section transformation, parameter source mozna zmenit na definitions)) specifies or defines (running later in chains) transformation "objects". 

valid types of tranformations:

* intersect
* mask (here the masking condition is phrased as a SQL condition??)
* source_filter : filtering according to source parameters - category of source, ... anything which is in table of sources (doplnit) 

* geometrical transformations (change of domain, projection, regridding)


Transformations are general but they may be confined to inventory or set.

A built-in transformation to_grid performs the intersect with the grid ep_grid_tz (fixed name of the database table, this grid is the target grid of the user) 

The syntax of any section in the transformation configuration file is derived from the definition file configspec-transformations.conf (the first row involves the name of the transformation which serves as reference in the chain specification (see below) in the main config file):

~~~

[[ name_of_transformation ]]
   type = type_of_transformation
   intersect = name_of_the_shapefile (must be imported among geometries)
   filter = 
   mask_type =
   mask_file =
   ...

~~~

As in other files, the defaults are listed in the definition file configspec-transformations.conf, too.

**Running transformations:**

In the main configuration file the so called "chains" are defined.
Single chains are intrinsically independent, i.e. they may run in parallell and no interactions between different chains occurs. In particular, any chain doesn't overwrite outputs of another chain, but the output data are all stored in the database. Nevertheless, every chain should have the to_grid transformation at the end, otherwise the resulting emissions are not written into the output file. 
 
in the section [transformations], subsection [[chains]]

The simplest chain is a mere transformation to user grid. In our case, this is the [[ to_grid ]] built-in transformation. This is written as e.g. chain1 = to_grid




External models
---------------
In many cases, part of the emissions has to be calculated using different methods than offered by the emissions preprocessor. E.g. biogenic emissions are routinely calculated using standalone models; another example could be the calculation of lightning emissions, emissions from domestic heating or emission from agriculture - all having in common some dependence on meteorological conditions. External model can be written in any programming language and are called with a python wrapper (interface).

External model themsleves or their interfaces are placed in

client/models
e.g.
client/models/model1
client/models/model2


They are defined and configured in the [[models]] section of the main config file
[[models]]
    models = 'model1', 'model2' # the comma-separated model list
    model_configs = 'ep_model1.conf', 'ep_model2.conf' # the comma-separated list the each model configuration (this is the configuration of the interface in general)

There are some general requirements to include models in the emission preprocessor:
1) the model interface has to hold the name client/models/model1/ep_model1.py 
2) within ep_model1.py, a function named 'model1' has to be placed which calls the model itself.
3) optionally, if the particular model requires some preprocessing, the 'preproc_model1' function has to be specified in client/models/model1/ep_model1.py as well
4) configuration specification may be provided as a configspec file, eg. client/models/model1/configspec-model1.conf

Running the models in the workflow including the optional model preprocessing is done by placing the following steps:
case.preproc_external_models # for calling each model's preproc function
case.run_external_models     # to call the models 

Models are often dependent on meteorological input. In order they recieve the right meteoroligical input fields, the
_required_met list has to be specified in its wrapping module client/models/model1/ep_model1.py

e.g. for MEGAN this is 
_required_met = [ 'soim1', 'soit1', 'tas', 'ps', 'qas', 'wndspd10m', 'pr24', 'par'],
where the list points to the possible internal meteorological variables.

We use IPCC-abbrevations for internal meteorological variable naming
tas - temperature at surface [K]
ta  - 3D temperature [K]
qas - specific humidity at the surface [kg/kg]
qa  - 3D specific humidty
rsds - surface incident SW radiation [W/m2]
par - photosyntetically active radiation [W/m2]
pa - 3D pressure [Pa]
zf - layer interface heights [m]
uas - U-wind anemometer height (usually 10m) [m/s]
vas - V-wind anemometer height (usually 10m) [m/s]
ua - U-wind [m/s]
va - V-wind [m/s]
wndspd - 3D wind speed [m/s]
wndspd10m- wind speed at anemometer height (usually 10m) [m/s]
pr - precipiation flux [kg m-2 s-1]
pr24 - accumulated precipitation [kg m-2]
soim1 - Soil moisture [kg/m3] 1m
soilt - Soil temperature [K] 1m


Time disaggregation and speciation
-----------------------------------
These follow the processing of sources and all spatial transformations.


The workflow
------------
In the configuration file fume_workflow.conf the user may specify the actual workflow of the simulation. It is possible to comment out some steps of the simulation which are thus skipped by the processor. This enables the user to tune single steps without having to go through all the processes every time.
In this case the switch scratch in the main config file is to be set on False. 

The only step which cannot be skipped is case.prepare_conf. 


Postprocessing
---------------
Including the row 

~~~

postproc.run

~~~

causes the postprocessing to be run which includes production of a graphical output. At present all species and times are plotted as maps in png format. 
 
* provider
* receiver 
 

The final writing of emission flows in the required format is switched on by including the row 

output.ep_write_output.write_emis      

into the workflow.conf file. 

Logging
---------------



Technicalities 
-------------------

In the sources schema, each "atomic" source in the raw file has a unique identifier of source (source_orig_id) and geometry (geom_orig_id). Different esets are then created by grouping the identifiers. These tables are created during processing:
[schema].ep_in_sources, [schema].ep_in_emissions, [schema].ep_in_geometries. 

Under ordinary circumstances it is not necessary to optimize any settings or configuration of postgres database. Had the import of data or saving data to tables taken too long, 
it is possible to switch off the autovacuum daemon in 
/var/lib/postgresql.conf. 





