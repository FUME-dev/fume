!!!! BUGS !!!!
==============
Import
======
- normalisation of geometry does not work correctly in case of mixed geometry types in one file (e.g. case of TNO emissions, if grid and point source has same Lat, Lon, normalisation fails)

Grid
====
- unregular grid (polygons) and create_grid = no not working, (moreover nx, dx etc still have to be in config file although they no meaning in case of create_grid = no)

Emission import
===============

- consider import of EMEP-like format (emissions in rows)
- consider converting all inventory headings to upper/lower case


Speciation and time disaggregation
==================================

- consider moving model species mapping from ep_speciation_splits to postprocessing
- enable time and space dependent speciation and time factors

Postprocessing
==============

- shapefile export

Coding conventions
==================

- unify column and variable naming (eg. ``source_id`` and ``src_type``)
    - check for inconsistencies like the ``source_type`` column in ep_in_geometries table

dopocty
~~~~~~~
Napr. NO=0.95*NOx, NMBVOC = VOC + TOC/0.8 – BZN, nebo PM_CRS = PM2_5 - PM10

Dopocty se aktualne provadi v calculate_pollutants.csv, jsou vazane na kategorii bez moznosti propadavani. Mozne problemy: pro SNAP_x chci dopocitat NOz NOx. Musim si tedy nacist NOx z inventare. Pokud je rodicem SNAP_x kat. 0, jiz v ni nesmi byt speciace NOx, aby nedoslo jk dvojimu zapocitavani emisi. Zaroven si do do databaze nactu NOx, ktere vysledne nepotrebuji. Pokud by se stalo, ze na jinem uzemi (v jinem inventari) bude pro SNAP_x platit jiny pomer pro dopocet NO z NOx, musim kvuli tomu zavest zvlastni kategorii...

Slo by resit doplnenim dopoctu do species_<inventory>.csv, napr. takto:

inv_specie_name,                        ep_specie_name, inv_unit,       filter,        neco

Suma_VOC_t + TOC_t/0.8 – v_BZN_t,       NMBVOC,         t/year,         SNAP=1,        "..."

*neco* by napr. urcilo, ze nejde o primy import, ale dopocet, aby se nemusel vzdy analyzovat vyraz nalevo"       
