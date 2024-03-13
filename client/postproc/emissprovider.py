"""
Description: emission provider - at the moment the sole implementation of a database reader that
distributes database data to postprocessors

"""

"""
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
"""

import numpy as np
from collections import defaultdict
from lib.ep_libutil import exec_timer, ep_rtcfg
from postproc.provider import DataProvider, pack
from lib.ep_libutil import combine_2_spec, combine_2_emis, combine_model_emis, combine_model_spec
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)


class EmissProvider(DataProvider):
    """
    Emission Provider: read emissions from database and distribute to receivers

    @pack('area_emiss') - for each time step read area emissions (optionally save to database)
                        - to use, implement a receive_area_emiss method in DataReceiver class to get

    @pack('species') - get list of species used in this case
                     - to use, implement a receive_species method in DataReceiver class
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @pack('grid')
    def get_grid(self):
        """
        Fetch grid coordinates from the database and distribute to the receiver objects.
        """

        cur = self.db.cursor()
        log.debug('Fetching grid coordinates...')
        cur.execute('SELECT i, j, xmi, xma, ymi, yma FROM "{}"."{}"'.format(self.cfg.db_connection.conf_schema, self.cfg.domain.grid_name))

        grid_x = np.zeros((self.cfg.domain.ny+1, self.cfg.domain.nx+1), dtype='f')
        grid_y = np.zeros_like(grid_x)
        for rec in cur:
            if rec[0] == 1:
                grid_x[rec[1], 0] = rec[2]
            if rec[1] == 1:
                grid_y[0, rec[1]] = rec[4]

            grid_x[rec[1], rec[0]] = rec[3]
            grid_y[rec[1], rec[0]] = rec[5]

        cur.close()
        self.distribute('grid', grid_x, grid_y)

    @pack('time_shifts')
    def get_time_shifts(self):
        """
        Fetch a list of case time shifts from ep_time_zone_shifts table and save as self.time_shifts
        dictionary of (ts_id, time_out)->time_loc.
        """

        # Make sure to call the select only once and save the results for later use
        try:
            self.time_shifts
        except AttributeError:  # Read the list from the database is it does not exist
            # get timezone shifts
            q = 'SELECT ts_id, time_out, time_loc FROM "{}".ep_time_zone_shifts ORDER BY ts_id, time_out'\
                .format(self.cfg.db_connection.case_schema)
            log.debug('Getting a list of case timezone shifts...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                self.time_shifts = {}
                for row in cur.fetchall():
                    self.time_shifts[(row[0],row[1])] = row[2]

            log.debug('Time shifts used in domain were provided.')

            self.distribute('time_shifts', time_shifts=self.time_shifts)


    @pack('total_emiss')
    def get_total_emissions(self):
        """
        Fetch total area emission data from the database and distribute to the receiver objects.

        The emissions received are speciated but not time dissagreggated.
        """

        self.get_species() # Make sure we have the list of species ready first
        cur = self.db.cursor()

        q = 'SELECT ep_total_emissions(%s, %s, %s, %s, %s)'
        log.debug('Fetching total area emissions...')
        cur.execute(q, (self.cfg.domain.nx, self.cfg.domain.ny, self.cfg.domain.nz,
                        [int(i[0]) for i in self.species],
                        self.cfg.db_connection.case_schema))

        emis = np.array(cur.fetchone()[0])
        self.distribute('total_emiss', data=emis)
        cur.close()

    @pack('emission_levels')
    def get_emission_levels(self):
        """
        Fetch total area emission data grouped by species and categories
        from the database and distribute to the receiver objects.

        The emissions received are speciated but not time dissagreggated.
        """
        try:
            self.emission_levels
        except AttributeError:
            cur = self.db.cursor()
            cur.execute('SELECT vertical_level FROM "{case_schema}".ep_transformation_chains_levels '
                        'GROUP BY vertical_level '
                        'ORDER BY vertical_level ASC '.format(case_schema=self.cfg.db_connection.case_schema))
            self.emission_levels = [i[0] for i in cur.fetchall()]
            cur.close()
            if (not self.emission_levels or self.emission_levels[0] != -1):
                self.emission_levels.insert(0, -1)
            log.debug('emission_levels: ', self.emission_levels)
            self.distribute('emission_levels', levels=self.emission_levels)

    @pack('number_volume_sources')
    def get_number_volume_sources(self):
        """
        Fetch total area emission data grouped by species and categories
        from the database and distribute to the receiver objects.

        The emissions received are speciated but not time dissagreggated.
        """
        sqltext = 'SELECT count(*) FROM ( ' + \
                  'SELECT i, j, l.vertical_level ' + \
                  'FROM "{case_schema}".ep_sources_grid sg ' + \
                  'JOIN "{case_schema}"."ep_grid_tz" gr on gr.grid_id = sg.grid_id ' + \
                  'JOIN "{case_schema}".ep_transformation_chains_levels l ' + \
                  '   ON l.chain_id = sg.transformation_chain ' + \
                  'GROUP BY i, j, l.vertical_level ) vsrc'
        sqltext = sqltext.format(case_schema=self.cfg.db_connection.case_schema)
        log.debug(sqltext)
        cur = self.db.cursor()
        cur.execute(sqltext)
        try:
            nvsrc = cur.fetchone()[0]
            log.debug('number_volume_sources: ', nvsrc)
        except AttributeError:
            pass
        cur.close()

        self.distribute('number_volume_sources', nvsrc=nvsrc)


    @pack('area_emiss_by_species_and_category')
    def get_area_emissions_by_species_and_category(self):
        """
        Fetch total area emission data grouped by species and categories
        from the database and distribute to the receiver objects.

        The emissions received are speciated but not time dissagreggated.
        """
        self.get_species()  # Make sure we have the list of species ready first
        self.get_categories()  # Make sure we have the list of categories ready first
        self.get_time_shifts()  # Make sure we have the list of time shifts ready first
        cur = self.db.cursor()

        if self.cfg.run_params.vdistribution_params.apply_vdistribution == False or "vdist" not in ep_rtcfg.keys() or ep_rtcfg["vdist"] != 1:
            cur.execute('DECLARE c_area_emiss_by_species_and_category CURSOR FOR '
                        'SELECT g.i, g.j, sg.k, em.spec_id s, em.cat_id c, z.ts_id z, sum(em.emiss) e '
                        'FROM "{case_schema}".ep_sg_emissions_spec em '
                        'JOIN "{case_schema}".ep_sources_grid sg USING(sg_id) '
                        'JOIN "{case_schema}".ep_grid_tz g USING(grid_id) '
                        'JOIN "{case_schema}".ep_timezones z USING(tz_id) '
                        "WHERE sg.source_type IN ('A', 'L') "
                        'GROUP BY g.i, g.j, sg.k, em.spec_id, em.cat_id, z.ts_id'.format(
                case_schema=self.cfg.db_connection.case_schema))
        elif self.cfg.run_params.vdistribution_params.apply_vdistribution == True and ep_rtcfg["vdist"] == 1:
            cur.execute('DECLARE c_area_emiss_by_species_and_category CURSOR FOR '
                        'SELECT i, j, COALESCE(vdf.level+1, 1) lev, em.spec_id s, em.cat_id c, z.ts_id z, sum(em.emiss * COALESCE(vdf.factor,1)) e '
                        'FROM "{case_schema}".ep_sg_emissions_spec em '
                        'JOIN "{case_schema}".ep_sources_grid sg USING(sg_id) '
                        'JOIN "{case_schema}".ep_grid_tz g USING(grid_id) '
                        'JOIN "{case_schema}".ep_timezones z USING(tz_id) '
                        'JOIN "{source_schema}".ep_in_sources sources USING(source_id) '
                        'JOIN "{source_schema}".ep_emission_sets es USING(eset_id) '
                        'LEFT JOIN "{case_schema}".ep_vdistribution_factors_out_all vdf ON vdf.vdistribution_id = ANY(es.vdistribution_id) '
                        'AND vdf.cat_id = em.cat_id '
                        "WHERE sg.source_type IN ('A', 'L') "
                        'GROUP BY i,j,lev, em.spec_id, em.cat_id, z.ts_id'.format(case_schema=self.cfg.db_connection.case_schema, source_schema=self.cfg.db_connection.source_schema))
        cur2 = self.db.cursor('c_area_emiss_by_species_and_category')
        try:
            cur2.itersize = int(self.cfg.db_connection.itersize)
        except AttributeError:
            pass

        while True:
            chunk = cur2.fetchmany(cur2.itersize)
            if not chunk:
                break

            self.distribute('area_emiss_by_species_and_category', data=chunk)

        cur.close()


    @pack('area_emiss_by_species_category_and_level')
    def get_area_emissions_by_species_category_and_level(self):
        """
        Fetch total area emission data grouped by species and categories
        from the database and distribute to the receiver objects.

        The emissions received are speciated but not time dissagreggated.
        """

        self.get_species()  # Make sure we have the list of species ready first
        self.get_categories()  # Make sure we have the list of categories ready first
        self.get_time_shifts()  # Make sure we have the list of time shifts ready first
        cur = self.db.cursor()

        q = 'DECLARE c_area_emiss_by_species_category_and_level CURSOR FOR ' + \
                    'SELECT g.i, g.j, coalesce(chl.vertical_level, -1) AS level, em.spec_id s, em.cat_id c, z.ts_id z, sum(em.emiss) e ' + \
                    'FROM "{case_schema}".ep_sg_emissions_spec em ' + \
                    'JOIN "{case_schema}".ep_sources_grid sg USING(sg_id) ' + \
                    'JOIN "{case_schema}".ep_grid_tz g USING(grid_id) ' + \
                    'JOIN "{case_schema}".ep_timezones z USING(tz_id) ' + \
                    'LEFT OUTER JOIN "{case_schema}".ep_transformation_chains_levels chl ON sg.transformation_chain=chl.chain_id ' + \
                    "WHERE sg.source_type IN ('A', 'L') " + \
                    'GROUP BY g.i, g.j, level, em.spec_id, em.cat_id, z.ts_id ' + \
                    'ORDER BY g.i, g.j, level, em.spec_id, em.cat_id, z.ts_id '
        q = q.format(case_schema=self.cfg.db_connection.case_schema)
        log.debug('get_area_emissions_by_species_category_and_level:', q)
        cur.execute(q)
        try:
            cur2 = self.db.cursor('c_area_emiss_by_species_category_and_level')
        except Exception as e:
            log.debug('cur2 create:', e)
        try:
            cur2.itersize = int(self.cfg.db_connection.itersize)
        except AttributeError:
            pass

        while True:
            chunk = cur2.fetchmany(cur2.itersize)
            if not chunk:
                break
            self.distribute('area_emiss_by_species_category_and_level', data=chunk)

        cur.close()


    @pack('point_vsrc_by_species_category_and_level')
    def get_point_vsrc_by_species_category_and_level(self):
        """
        Fetch total point vsrc emission data grouped by species, categories, and levels
        from the database and distribute to the receiver objects.

        The emissions received are speciated but not time dissagreggated.
        """

        self.get_species()  # Make sure we have the list of species ready first
        self.get_categories()  # Make sure we have the list of categories ready first
        self.get_time_shifts()  # Make sure we have the list of time shifts ready first
        self.get_emission_levels() # Make sure we have the list of emission levels ready first

        cur = self.db.cursor()
        q = 'DECLARE c_point_vsrc_by_species_category_and_level CURSOR FOR ' + \
                    'SELECT g.i, g.j, chl.vertical_level AS level, em.spec_id s, em.cat_id c, z.ts_id z, ' + \
                    'coalesce(ps.height, 0) as h, sum(em.emiss) e ' + \
                    'FROM "{case_schema}".ep_sg_emissions_spec em ' + \
                    'JOIN "{case_schema}".ep_sources_grid sg USING(sg_id) ' + \
                    'JOIN "{case_schema}".ep_grid_tz g USING(grid_id) ' + \
                    'JOIN "{case_schema}".ep_timezones z USING(tz_id) ' + \
                    'JOIN "{case_schema}".ep_transformation_chains_levels chl ' + \
                    '  ON sg.transformation_chain=chl.chain_id ' + \
                    'LEFT OUTER JOIN "{source_schema}".ep_in_sources_point ps USING (source_id) ' + \
                    "WHERE sg.source_type = 'P' " + \
                    'GROUP BY g.i, g.j, level, em.spec_id, em.cat_id, z.ts_id, h ' + \
                    'ORDER BY g.i, g.j, level, em.spec_id, em.cat_id, z.ts_id, h '
        q = q.format(case_schema=self.cfg.db_connection.case_schema, source_schema=self.cfg.db_connection.source_schema)
        log.debug('get_point_vsrc_emissions_by_species_category_and_level:', q)
        cur.execute(q)
        try:
            cur2 = self.db.cursor('c_point_vsrc_by_species_category_and_level')
        except Exception as e:
            log.debug('cur2 create:', e)
        try:
            cur2.itersize = int(self.cfg.db_connection.itersize)
        except AttributeError:
            pass

        while True:
            chunk = cur2.fetchmany(cur2.itersize)
            if not chunk:
                break
            self.distribute('point_vsrc_by_species_category_and_level', data=chunk)

        cur.close()


    @pack('point_emiss_by_species_and_category')
    def get_point_emissions_by_species_and_category(self):
        """
        Fetch total point emission data grouped by species and categories
        from the database and distribute to the receiver objects.

        The emissions received are speciated but not time dissagreggated.
        """

        self.get_point_species()  # Make sure we have the list of species ready first
        self.get_point_categories()  # Make sure we have the list of categories ready first
        self.get_time_shifts()  # Make sure we have the list of time shifts ready first
        cur = self.db.cursor()

        cur.execute('DECLARE c_point_emiss_by_species_and_category CURSOR FOR '
                    'SELECT em.sg_id, em.spec_id s, em.cat_id c, z.ts_id z, sum(em.emiss) e '
                    'FROM "{case_schema}".ep_sg_emissions_spec em '
                    'JOIN "{case_schema}".ep_sources_grid sg USING(sg_id) '
                    'JOIN "{case_schema}".ep_grid_tz g USING(grid_id) '
                    'JOIN "{case_schema}".ep_timezones z USING(tz_id) '
                    "WHERE sg.source_type IN ('P') "
                    'GROUP BY em.sg_id, em.spec_id, em.cat_id, z.ts_id'.format(case_schema=self.cfg.db_connection.case_schema))

        cur2 = self.db.cursor('c_point_emiss_by_species_and_category')
        try:
            cur2.itersize = int(self.cfg.db_connection.itersize)
        except AttributeError:
            pass

        while True:
            chunk = cur2.fetchmany(cur2.itersize)
            if not chunk:
                break

            self.distribute('point_emiss_by_species_and_category', data=chunk)

        cur.close()

    @pack('point_emiss_by_species_and_category_ij')
    def get_point_emissions_by_species_and_category_ij(self):
        """
        Fetch total point emission data grouped by grid, species, and categories
        from the database and distribute to the receiver objects.

        The emissions received are speciated but not time dissagreggated.
        """

        self.get_point_species()  # Make sure we have the list of species ready first
        self.get_point_categories()  # Make sure we have the list of categories ready first
        self.get_time_shifts()  # Make sure we have the list of time shifts ready first
        cur = self.db.cursor()

        cur.execute('DECLARE c_point_emiss_by_species_and_category CURSOR FOR '
                    'SELECT em.sg_id, em.spec_id s, em.cat_id c, z.ts_id z, sum(em.emiss) e '
                    'FROM "{case_schema}".ep_sg_emissions_spec em '
                    'JOIN "{case_schema}".ep_sources_grid sg USING(sg_id) '
                    'JOIN "{case_schema}".ep_grid_tz g USING(grid_id) '
                    'JOIN "{case_schema}".ep_timezones z USING(tz_id) '
                    "WHERE sg.source_type IN ('P') "
                    'GROUP BY em.sg_id, em.spec_id, em.cat_id, z.ts_id'.format(case_schema=self.cfg.db_connection.case_schema))

        cur2 = self.db.cursor('c_point_emiss_by_species_and_category')
        try:
            cur2.itersize = int(self.cfg.db_connection.itersize)
        except AttributeError:
            pass

        while True:
            chunk = cur2.fetchmany(cur2.itersize)
            if not chunk:
                break

            self.distribute('point_emiss_by_species_and_category', data=chunk)

        cur.close()



    @pack('area_emiss')
    def get_area_emission_time_series(self):
        """
        Fetch area emission data from the database and distribute to the receiver objects.

        For all time steps (as set up in the run configuration) call the ep_emiss_time_series
        function. A 4D matrix is received with dimensions [nx, ny, nz, nspec] where nspec is
        the number of output species and nx, ny, nz are domain dimensions.

        Uses the self.species list read by the get_species method.
        """

        self.get_species() # Make sure we have the list of species ready first
        cur = self.db.cursor()
        for i in range(self.cfg.run_params.time_params.num_time_int):
            q = 'SELECT ep_emiss_time_series(%s,%s,%s,%s,%s,%s::text,%s)'
            log.debug('Fetching area emissions for timestep', i)
            cur.execute(q, (self.cfg.domain.nx, self.cfg.domain.ny, self.cfg.domain.nz,
                            [int(i[0]) for i in self.ep_species], self.rt_cfg['run']['datestimes'][i],
                            self.cfg.db_connection.case_schema,
                            self.cfg.run_params.output_params.save_time_series_to_db))

            ep_emis = np.array(cur.fetchone()[0])
            # combine area species with other model species
            ep_species_names = [s[1] for s in self.ep_species]
            if len(ep_species_names) == 0:
                log.fmt_debug('WARNING: no emissions computed internally with FUME for timestep {}. It will continue anyway trying to collect emissions from external models.', i)
                emis, sp = combine_model_emis(ep_emis, ep_species_names ,i, noanthrop = True)
            else:
                emis, sp = combine_model_emis(ep_emis, ep_species_names ,i )
            self.distribute('area_emiss', timestep=i, data=emis)

        cur.close()

    @pack('species')
    def get_species(self):
        """
        Fetch a list of all output species used in the case by
        calling the {case_schema}.get_species view and save as self.species
        list of (spec_id, spec_name) tuples.
        """

        # Make sure to call the view only once and save the results for later use
        try:
            self.species
        except AttributeError:  # Read the list from the database is it does not exist
            q = 'SELECT * FROM "{}".get_species'.format(self.cfg.db_connection.case_schema)
            log.debug('Getting list of species...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                ep_species = cur.fetchall()
            self.species = combine_model_spec(ep_species)
            self.ep_species = ep_species
            log.fmt_debug('Species from FUME and other models: {}.', ','.join([s[1] for s in self.species]))

            self.distribute('species', species=self.species)

    @pack('categories')
    def get_categories(self):
        """
        Fetch a list of output emission categories used in the case by
        calling the {case_schema}.get_categories view and save as self.categories
        list of (cat_id, name) tuples.
        """

        # Make sure to call the view only once and save the results for later use
        try:
            self.categories
        except AttributeError:  # Read the list from the database is it does not exist
            q = 'SELECT cat_id, name FROM "{}".get_categories ORDER BY cat_id'.format(self.cfg.db_connection.case_schema)
            log.debug('Getting a list of categories...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                self.categories = cur.fetchall()

            log.fmt_debug('Categories from FUME: {}.', ','.join([c[1] for c in self.categories]))

            self.distribute('categories', categories=self.categories)

    @pack('time_factors')
    def get_time_factors(self):
        """
        Fetch a list of time disaggregation factors for calculation of emission time series.
        """

        try:
            self.time_factors
        except AttributeError:
            q = 'SELECT cat_id, time_loc, tv_factor FROM "{}".ep_time_factors ORDER BY time_loc, cat_id'.format(self.cfg.db_connection.case_schema)
            log.debug('Getting a list of time disaggregation factors...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                self.time_factors = defaultdict(dict)
                for row in cur:
                    self.time_factors[row[1]][row[0]] = row[2]

            self.distribute('time_factors', factors=self.time_factors)

    @pack('molar_weight')
    def get_molar_weight(self):
        """
        Fetch a dict of molar weight of output emission for particular
        categories and output species used in the case by
        list of (cat_id, spec_mod_id) tuples.
        """

        # Make sure to call the view only once and save the results for later use
        try:
            self.molar_weight
        except AttributeError:  # Read the list from the database is it does not exist
            self.molar_weight = {}
            q = 'SELECT cat_id, spec_mod_id, avg(mol_weight) AS molar_weight' \
                ' FROM "{}".ep_mod_spec_factors_all' \
                ' GROUP BY cat_id, spec_mod_id' \
                ' ORDER BY cat_id, spec_mod_id'.format(self.cfg.db_connection.case_schema)
            log.debug('Getting a list of molar weights...', q)
            cur = self.db.cursor()
            cur.execute(q)
            for m in cur.fetchall():
                self.molar_weight[(m[0], m[1])] = m[2]
            #log.fmt_debug('Molar weights from FUME: {}.', ','.join([c[1] for c in self.get_molar_weight]))
            self.distribute('molar_weight', molar_weight=self.molar_weight)
            cur.close()

    @pack('point_emiss')
    def get_point_emission_time_series(self):
        self.get_point_species()
        self.get_point_sources_params()
        cur = self.db.cursor()
        for i in range(self.cfg.run_params.time_params.num_time_int):
            q = 'SELECT ep_pemiss_time_series(%s,%s,%s::timestamp,%s::text)'
            log.debug('Fetching point emissions for timestep', i)
            cur.execute(q, ([int(i[0]) for i in self.stacks],
                            [int(i[0]) for i in self.pspecies],
                            self.rt_cfg['run']['datestimes'][i],
                            self.cfg.db_connection.case_schema))

            emis = np.array(cur.fetchone()[0])
            self.distribute('point_emiss', timestep=i, data=emis)
        cur.close()

    @pack('point_emiss_ij')
    def get_point_emission_time_series_ij(self):
        self.get_point_categories()
        self.get_point_species()
        cur = self.db.cursor()
        for i in range(self.cfg.run_params.time_params.num_time_int):
            q = 'SELECT ep_pemiss_time_series_ij(%s::integer[], %s::integer[], %s::timestamp, %s::text)'
            log.debug('Fetching point emissions ij for timestep', i)
            log.debug(q)
            pcat = [int(i[0]) for i in self.pcategories]
            pspec = [int(i[0]) for i in self.pspecies]
            if len(pcat) > 0 and len(pspec) > 0 :
                log.debug('pcat:', pcat)
                log.debug('pspec:', pspec)
                cur.execute(q, (pcat, pspec, self.rt_cfg['run']['datestimes'][i], self.cfg.db_connection.case_schema))
                emis = np.array(cur.fetchone()[0])
                log.debug('emis:', emis.shape)
                self.distribute('point_emiss_ij', timestep=i, data=emis)
        log.sql_debug(self.db)
        cur.close()

    @pack('stack_params')
    def get_point_sources_params(self):
        """
        Fetch the list of pointsources and the corresponding stack parameters
        """

        try:
            self.stacks
        except AttributeError: 
            q = 'SELECT DISTINCT  array[sg_id::float, lon, xstk, lat, ystk, height, diameter, temperature, velocity] FROM "{case_schema}".ep_sg_emissions_spec em JOIN "{case_schema}".ep_sources_point psrc USING(sg_id)'.format(case_schema = self.cfg.db_connection.case_schema)
            log.debug('Getting list of stacks...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                if cur.rowcount > 0:
                    self.stacks = np.array(cur.fetchall(), dtype=np.float).squeeze(axis=1)
                else:
                    self.stacks = np.array([], dtype=np.float)

            self.distribute('stack_params', stacks=self.stacks)

    @pack('point_species')
    def get_point_species(self):
        """
        Fetch a list of output point emission species from the database by
        calling the {case_schema}.get_species_point view and save as self.species
        list of (spec_id, spec_name) tuples.
        """

        # Make sure to call the view only once and save the results for later use
        try:
            self.pspecies
        except AttributeError:  # Read the list from the database is it does not exist
            q = 'SELECT * from "{}".get_species_point'.format(self.cfg.db_connection.case_schema)
            log.debug('Getting list of point species...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                self.pspecies = cur.fetchall()

            self.distribute('point_species', pspecies=self.pspecies)

    @pack('all_species')
    def get_all_species(self):
        """
        Fetch a list of output area and point emission species from the database by
        calling the {case_schema}.get_species and get_species_point view and save as self.aspecies
        list of (spec_id, spec_name) tuples.
        """
        # Make sure to call the view only once and save the results for later use
        try:
            self.aspecies
        except AttributeError:  # Read the list from the database is it does not exist
            q = 'SELECT * from "{}".get_species'.format(self.cfg.db_connection.case_schema)
            log.debug('Getting list of area species...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                species = cur.fetchall()

            q = 'SELECT * from "{}".get_species_point'.format(self.cfg.db_connection.case_schema)
            log.debug('Getting list of point species...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                pspecies = cur.fetchall()


            self.aspecies = list(set(species).union(set(pspecies)))


            self.distribute('all_species', aspecies=self.aspecies)

    @pack('point_categories')
    def get_point_categories(self):
        """
        Fetch a list of output area emission categories used in the case by
        calling the {case_schema}.get_categories view and save as self.categories
        list of (cat_id, name) tuples.
        """

        # Make sure to call the view only once and save the results for later use
        try:
            self.pcategories
        except AttributeError:  # Read the list from the database is it does not exist
            q = 'SELECT distinct c.cat_id, c.name FROM "{conf}".ep_emission_categories c '\
                ' JOIN "{case}".ep_sg_emissions_spec sp USING(cat_id) '\
                ' JOIN "{case}".ep_sources_grid sg USING(sg_id) WHERE sg.source_type = \'P\' ORDER BY c.cat_id'\
                .format(conf=self.cfg.db_connection.conf_schema, case=self.cfg.db_connection.case_schema)
            log.debug('Getting a list of point species categories...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                self.pcategories = cur.fetchall()

            log.fmt_debug('Point species categories from FUME: {}.', ','.join([c[1] for c in self.pcategories]))

            self.distribute('point_categories', pcategories=self.pcategories)

    @pack('all_categories')
    def get_all_categories(self):
        """
        Fetch a list of output area and point emission categories used in the case by
        calling the {case_schema}.get_categories and get_point_categories views and save as self.acategories
        list of (cat_id, name) tuples.
        """

        # Make sure to call the view only once and save the results for later use
        try:
            self.acategories
        except AttributeError:  # Read the list from the database is it does not exist
            # point categories
            q = 'SELECT distinct c.cat_id, c.name FROM "{conf}".ep_emission_categories c '\
                ' JOIN "{case}".ep_sg_emissions_spec sp USING(cat_id) '\
                ' JOIN "{case}".ep_sources_grid sg USING(sg_id) WHERE sg.source_type = \'P\' ORDER BY c.cat_id'\
                .format(conf=self.cfg.db_connection.conf_schema, case=self.cfg.db_connection.case_schema)
            log.debug('Getting a list of point species categories...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                pcategories = cur.fetchall()
            # area categories
            q = 'SELECT cat_id, name FROM "{}".get_categories ORDER BY cat_id'.format(self.cfg.db_connection.case_schema)
            log.debug('Getting a list of categories...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                categories = cur.fetchall()

            self.acategories = list(set(pcategories).union(set(categories)))

            self.distribute('all_categories', acategories=self.acategories)
