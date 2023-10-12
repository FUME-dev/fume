"""
Description: Simple plotter postprocessor

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

import os
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from postproc.receiver import DataReceiver, requires
from lib.misc import get_polygons_from_file, draw_multipolygon_on_map
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)


_basemap_resolution = 'i'
_file_resolution = 300
_cmap = 'hot_r'


class EmissPlot():
    def __init__(self, cfg, rt_cfg, **kwargs):
        self.cfg = cfg
        self.rt_cfg = rt_cfg

        try:
            self.grid_x = kwargs['grid_x']
        except KeyError:
            self.grid_x = None

        try:
            self.grid_y = kwargs['grid_y']
        except KeyError:
            self.grid_y = None

        self._setup()

    def __del__(self):
        plt.close(self.fig)

    def _setup(self):
        if hasattr(self.cfg.postproc.emissplotter, 'basemap_resolution'):
            self.resolution = self.cfg.postproc.emissplotter.basemap_resolution
        else:
            self.resolution = _basemap_resolution

        if hasattr(self.cfg.postproc.emissplotter, 'file_resolution'):
            self.file_resolution = self.cfg.postproc.emissplotter.file_resolution
        else:
            self.file_resolution = _file_resolution

        if hasattr(self.cfg.postproc.emissplotter, 'cmap'):
            self.cmap = self.cfg.postproc.emissplotter.cmap
        else:
            self.cmap = _cmap

        self.fig = plt.figure(dpi=self.file_resolution)
        plt.set_cmap(self.cmap)
        if hasattr(self.cfg.postproc.emissplotter, 'basemap') and self.cfg.postproc.emissplotter.basemap:
            pp = self.rt_cfg['projection_params']
            from mpl_toolkits.basemap import Basemap
            if pp['proj'] == 'LAMBERT':
                m = Basemap(projection='lcc',
                            resolution=self.cfg.postproc.emissplotter.basemap_resolution,
                            lat_0=pp['lat_central'], lon_0=pp['lon_central'],
                            lat_1=pp['p_alp'], lat_2=pp['p_bet'],
                            width=self.cfg.domain.nx*self.cfg.domain.delx,
                            height=self.cfg.domain.ny*self.cfg.domain.dely)

            self.plotter = m
            m.drawcoastlines()
            if self.cfg.postproc.emissplotter.draw_countries:
                m.drawcountries()

            self.grid_x = self.grid_x+m.projparams['x_0']
            self.grid_y = self.grid_y+m.projparams['y_0']
        else:
            self.grid_x = np.arange(self.cfg.domain.nx+1)
            self.grid_y = np.arange(self.cfg.domain.ny+1)
            self.plotter = plt

        try:
            self.overlay_polygons = get_polygons_from_file(self.cfg.postproc.emissplotter.overlay_polygons)
        except AttributeError:
            self.overlay_polygons = []
        except IOError:
            log.debug('No such file or directory: ', self.cfg.postproc.emissplotter.overlay_polygons)
            self.overlay_polygons = []

    def plot(self, data, filename, **kwargs):
        pic = self.plotter.pcolormesh(self.grid_x, self.grid_y, data)
        if 'title' in kwargs:
            plt.title(kwargs['title'])

        if hasattr(self.cfg.postproc.emissplotter, 'xmin') and hasattr(self.cfg.postproc.emissplotter, 'xmax') and self.cfg.postproc.emissplotter.basemap:
            plt.xlim(float(self.cfg.postproc.emissplotter.xmin)+self.plotter.projparams['x_0'],
                     float(self.cfg.postproc.emissplotter.xmax)+self.plotter.projparams['x_0'])

        if hasattr(self.cfg.postproc.emissplotter, 'ymin') and hasattr(self.cfg.postproc.emissplotter, 'ymax') and self.cfg.postproc.emissplotter.basemap:
            plt.ylim(float(self.cfg.postproc.emissplotter.ymin)+self.plotter.projparams['y_0'],
                     float(self.cfg.postproc.emissplotter.ymax)+self.plotter.projparams['y_0'])

        for poly in self.overlay_polygons:
            draw_multipolygon_on_map(poly, self.plotter)

        if hasattr(self.cfg.postproc.emissplotter, 'colorbar') and self.cfg.postproc.emissplotter.colorbar.lower() in ('horizontal', 'vertical'):
            cbar = plt.colorbar(orientation=self.cfg.postproc.emissplotter.colorbar.lower())

        filepath=os.path.dirname(os.path.abspath(filename))
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        plt.savefig(filename, dpi=self.file_resolution)
        try:
            cbar.remove()
        except NameError:
            raise

        pic.remove()


class EmissPlotterBase(DataReceiver):
    """
    Base class for emission plotting classes
    Options (set in config file):
        - postproc.emissplotter.filetype (default png)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        try:
            self.filetype = self.cfg.postproc.emissplotter.filetype
        except AttributeError:
            self.filetype = 'png'

    def receive_species(self, species):
        self.species = species

    def receive_grid(self, grid_x, grid_y):
        self.grid_x, self.grid_y = grid_x, grid_y
        self.plotter = EmissPlot(self.cfg, self.rt_cfg, grid_x=self.grid_x, grid_y=self.grid_y)


class EmissPlotter(EmissPlotterBase):
    """
    Simple plotter: for each timestep and each species received plot a figure
    Options (set in config file):
        - postproc.emissplotter.filetype (default png)
        - postproc.emissplotter.filename_pattern (default emission_{species}_{datetime})
    """

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        try:
            self.filename_pattern = self.cfg.postproc.emissplotter.filename_pattern
        except AttributeError:
            self.filename_pattern = 'emissions_{species}_{datetime}'

    @requires('grid', 'species')
    def receive_area_emiss(self, timestep, data):
        time = self.rt_cfg['run']['datestimes'][timestep]
        for s, spectuple in enumerate(self.species):
            specid, specname = spectuple
            title = 'Emissions of {} at {}'.format(specname, time)
            filename = self.filename_pattern.format(species=specname, datetime=time) + '.' + self.filetype
            self.plotter.plot(data[:, :, 0, s].T, filename, title=title)

    def receive_species(self, species):
        self.species = species


class TotalEmissPlotter(EmissPlotterBase):
    """
    Simple total emissions plotter: for each species received plot a figure
    Options (set in config file):
        - postproc.emissplotter.filetype (default png)
        - postproc.emissplotter.filename_pattern (default emission_{species})
    """

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        try:
            self.filename_pattern = self.cfg.postproc.emissplotter.filename_pattern
        except AttributeError:
            self.filename_pattern = 'emissions_{species}'

    @requires('grid', 'species')
    def receive_total_emiss(self, data):
        for s, spectuple in enumerate(self.species):
            specid, specname = spectuple
            title = 'Total emissions of {}'.format(specname)
            filename = self.filename_pattern.format(species=specname) + '.' + self.filetype
            self.plotter.plot(data[:, :, 0, s].T, filename, title=title)
