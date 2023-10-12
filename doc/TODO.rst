BUGS
==================================
Grid
----------------------------------
- irregular grid (polygons) and `create_grid` = `no` not working, (moreover `nx`, `dx` etc. still have to be in config file although they have no meaning in case of `create_grid` = `no`)


DEVELOPMENT PLANS
==================================
Calculation efficiency
----------------------------------

Data  import
----------------------------------
- add possibility to have comments, empty lines in input text files

Emission import
----------------------------------
- consider csv with gridded data with different resolution ``dx`` and ``dy`` supplied in the same file. ``grid_dx`` and ``grid_dx`` which are now fixed in the  info file could be supplied as column names
- import excel files


Calculation of pollutants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
consider calculation of pollutants not for categories but for inventories

Case processing
----------------------------
- point sources - consider only some point sources as point for export based on some criteria, others should be added to area sources


Chemical speciation
----------------------------------
- enable time and space dependent speciation and time factors
- chemical speciations should not depend on model, but just on chemistry mechanism

Time disaggregation
----------------------------------
- possibility to have geograficaly/... dependent profiles


Postprocessing
----------------------------------
- shapefile export










