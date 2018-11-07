import numpy as np
from lib.ep_libutil import ep_debug
from postproc.provider import DataProvider, pack
from lib.ep_libutil import combine_2_spec,combine_2_emis,combine_model_emis,combine_model_spec


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
        ep_debug('Fetching grid coordinates...')
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

    @pack('total_emiss')
    def get_total_emissions(self):
        """
        Fetch total emission data from the database and distribute to the receiver objects.

        The emissions received are speciated but not time dissagreggated.
        Only works with aggregate_speciated_emissions = no
        """

        self.get_area_species() # Make sure we have the list of species ready first
        cur = self.db.cursor()

        q = 'SELECT ep_total_emissions(%s, %s, %s, %s, %s)'
        ep_debug('Fetching total emissions...')
        cur.execute(q, (self.cfg.domain.nx, self.cfg.domain.ny, self.cfg.domain.nz,
                        [int(i[0]) for i in self.species],
                        self.cfg.db_connection.case_schema))

        emis = np.array(cur.fetchone()[0])
        self.distribute('total_emiss', data=emis)
        cur.close()

    @pack('area_emiss')
    def get_area_emission_time_series(self):
        """
        Fetch area emission data from the database and distribute to the receiver objects.

        For all time steps (as set up in the run configuration) call the ep_emiss_time_series
        function. A 4D matrix is received with dimensions [nx, ny, nz, nspec] where nspec is
        the number of output species and nx, ny, nz are domain dimensions.

        Uses the self.species list read by the get_area_species method.
        """

        self.get_area_species() # Make sure we have the list of species ready first
        cur = self.db.cursor()

        for i in range(self.cfg.run_params.time_params.num_time_int):
            q = 'SELECT ep_emiss_time_series(%s,%s,%s,%s,%s,%s::text,%s,%s)'
            ep_debug('Fetching area emissions for timestep', i)
            cur.execute(q, (self.cfg.domain.nx, self.cfg.domain.ny, self.cfg.domain.nz,
                            [int(i[0]) for i in self.ep_species], self.rt_cfg['run']['datestimes'][i],
                            self.cfg.db_connection.case_schema,
                            self.cfg.run_params.output_params.save_time_series_to_db,
                            self.cfg.run_params.aggregate_speciated_emissions))

            ep_emis = np.array(cur.fetchone()[0])
            # combine area species with other model species
            ep_species_names = [s[1] for s in self.ep_species]
            if len(ep_species_names) == 0:
                ep_debug('WARNING: no emissions computed internally with FUME for timestep {}. It will continue anyway trying to collect emissions from external models.'.format(i))
                emis, sp = combine_model_emis(ep_emis, ep_species_names ,i, noanthrop = True)
            else:
                emis, sp = combine_model_emis(ep_emis, ep_species_names ,i )
            self.distribute('area_emiss', timestep=i, data=emis)

        cur.close()

    @pack('area_species')
    def get_area_species(self):
        """
        Fetch a list of output area emission species from the database by
        calling the {case_schema}.get_species view and save as self.species
        list of (spec_id, spec_name) tuples.
        """

        # Make sure to call the view only once and save the results for later use
        try:
            self.species
        except AttributeError:  # Read the list from the database is it does not exist
            if self.cfg.run_params.aggregate_speciated_emissions:
                viewname = 'get_species_agg'
            else:
                viewname = 'get_species'

            q = 'SELECT * from "{}"."{}"'.format(self.cfg.db_connection.case_schema, viewname)
            ep_debug('Getting list of species...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                ep_species = cur.fetchall()
                
            
#            ep_species_names = [s[1] for s in ep_species]

            self.species = combine_model_spec(ep_species)
            self.ep_species = ep_species
#            ep_debug('Species from FUME: {}.'.format(','.join(ep_species_names)))
            ep_debug('Species from FUME and other models: {}.'.format(','.join([s[1] for s in self.species])))

            self.distribute('area_species', species=self.species)

    @pack('point_emiss')
    def get_point_emission_time_series(self):
        self.get_point_species()
        self.get_point_sources_params()
        cur = self.db.cursor()

        for i in range(self.cfg.run_params.time_params.num_time_int):
            q = 'SELECT ep_pemiss_time_series(%s,%s,%s::timestamp,%s::text,%s::text)'
            ep_debug('Fetching point emissions for timestep', i)
            cur.execute(q, ([int(i[0]) for i in self.stacks],
                            [int(i[0]) for i in self.pspecies],
                            self.rt_cfg['run']['datestimes'][i],
                            self.cfg.db_connection.source_schema,
                            self.cfg.db_connection.case_schema))

            emis = np.array(cur.fetchone()[0])

            self.distribute('point_emiss', timestep=i, data=emis)

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
            ep_debug('Getting list of stacks...', q)
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
        calling the {case_schema}.get_point_species view and save as self.species
        list of (spec_id, spec_name) tuples.
        """

        # Make sure to call the view only once and save the results for later use
        try:
            self.pspecies
        except AttributeError:  # Read the list from the database is it does not exist
            q = 'SELECT * from "{}".get_species_point'.format(self.cfg.db_connection.case_schema)
            ep_debug('Getting list of point species...', q)
            with self.db.cursor() as cur:
                cur.execute(q)
                self.pspecies = cur.fetchall()

            self.distribute('point_species', species=self.pspecies)
