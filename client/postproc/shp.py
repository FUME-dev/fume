"""
Description: shape-file export - only total annual emission output, either
only area or only point.

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

Copyright 2014-2026 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
Copyright 2014-2026 Charles University, Faculty of Mathematics and Physics, Prague, Czech Republic
Copyright 2014-2026 Czech Hydrometeorological Institute, Prague, Czech Republic
Copyright 2014-2017 Czech Technical University in Prague, Czech Republic
"""

from osgeo import ogr, osr
import numpy as np
from postproc.receiver import DataReceiver, requires
import os


class SHPWriterBase(DataReceiver):
    """
    Postprocessor class for writing SHP total emission files.
    This is the base class for the common SHP format settings and should not be
    instantiated by itself. Descendant classes SHPAreaWriter or SHPPointWriter
    should be used instead.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class SHPAreaWriter(SHPWriterBase):
    """
    Postprocessor class for writing SHP area total emission file.
    """

    def setup(self):
        self.output_file = self.cfg.postproc.shpwriter.outfile_area

        if not os.path.exists(self.output_file):
            os.makedirs(self.output_file)

        if os.path.exists(self.output_file):
            ogr.GetDriverByName('ESRI Shapefile').DeleteDataSource(
                self.output_file)

    def receive_species(self, species):
        self.species_lookup = {member[0]: idx
                               for idx, member in enumerate(species)}
        self.species_names = {idx: member[1]
                              for idx, member in enumerate(species)}

    def receive_domain_geometry(self, geom):
        self.geometry_lookup = {member[0]: idx
                                for idx, member in enumerate(geom)}
        self.geometry = {idx: member[1] for idx, member in enumerate(geom)}

    @requires('domain_geometry', 'species')
    def receive_area_emiss_polygon(self, data):
        self.emis_out = np.zeros((len(self.geometry), len(self.species_names)))
        for row in data:
            geom_idx = self.geometry_lookup[row[0]]
            spec_idx = self.species_lookup[row[1]]
            emis = row[2]
            self.emis_out[geom_idx, spec_idx] = emis

    def finalize(self):
        driver = ogr.GetDriverByName('ESRI Shapefile')
        if driver is None:
            raise RuntimeError('ESRI Shapefile driver not available')

        outfile = driver.CreateDataSource(self.output_file)

        srs = osr.SpatialReference()
        srs.ImportFromProj4(self.rt_cfg['projection_params']['proj4string'])
        layer = outfile.CreateLayer('polygons', srs, ogr.wkbPolygon)

        # Define attribute fields
        for _, spec in self.species_names.items():
            field_defn = ogr.FieldDefn(spec, ogr.OFTReal)
            layer.CreateField(field_defn)

        feature_defn = layer.GetLayerDefn()
        for grid_idx, emissions in enumerate(self.emis_out):
            geometry = ogr.CreateGeometryFromWkt(self.geometry[grid_idx])

            # Create a new feature
            feature = ogr.Feature(feature_defn)
            feature.SetGeometry(geometry)

            # Set attribute values
            for spec_idx, spec in enumerate(emissions):
                feature.SetField(self.species_names[spec_idx], spec)

            # Add feature to the layer
            layer.CreateFeature(feature)


class SHPPointWriter(SHPWriterBase):
    """
    Postprocessor class for writing SHP point total emission file.
    """

    def setup(self):
        self.output_point_file = self.cfg.postproc.shpwriter.outfile_point

    @requires('point_species', 'stack_params')
    def receive_point_total_emiss(self, data):
        self.emis_out = np.zeros((self.numstk, len(self.species_names)))

        for row in data:
            stack_idx = self.stacks_lookup[row[0]]
            spec_idx = self.species_lookup[row[1]]
            emis = row[2]
            self.emis_out[stack_idx, spec_idx] = emis

    def receive_stack_params(self, stacks):
        self.stack_params = {idx: [stack[2], stack[4], stack[5], stack[6],
                                   stack[7], stack[8]]
                             for idx, stack in enumerate(stacks)}
        self.numstk = len(self.stack_params)
        stacks_id = list(map(int, stacks[:, 0]))
        self.stacks_lookup = {member: idx
                              for idx, member in enumerate(stacks_id)}

    def receive_point_species(self, pspecies):
        self.species_lookup = {member[0]: idx
                               for idx, member in enumerate(pspecies)}
        self.species_names = {idx: member[1]
                              for idx, member in enumerate(pspecies)}

    def finalize(self):
        driver = ogr.GetDriverByName('ESRI Shapefile')
        if driver is None:
            raise RuntimeError('ESRI Shapefile driver not available')

        outfile = driver.CreateDataSource(self.output_point_file)

        srs = osr.SpatialReference()
        srs.ImportFromProj4(self.rt_cfg['projection_params']['proj4string'])
        layer = outfile.CreateLayer('points', srs, ogr.wkbPoint)

        # Define attribute fields
        for par in ['height', 'diameter', 'temp', 'velocity']:
            field_defn = ogr.FieldDefn(par, ogr.OFTReal)
            layer.CreateField(field_defn)
        for _, spec in self.species_names.items():
            field_defn = ogr.FieldDefn(spec, ogr.OFTReal)
            layer.CreateField(field_defn)

        feature_defn = layer.GetLayerDefn()
        for stack_idx, emissions in enumerate(self.emis_out):
            wkt = "POINT ({x} {y})".format(x=self.stack_params[stack_idx][0],
                                           y=self.stack_params[stack_idx][1])
            geometry = ogr.CreateGeometryFromWkt(wkt)

            # Create a new feature
            feature = ogr.Feature(feature_defn)
            feature.SetGeometry(geometry)

            # Set attribute values
            for par_idx, par in enumerate(['height', 'diameter', 'temp',
                                           'velocity']):
                feature.SetField(par, self.stack_params[stack_idx][par_idx + 2])
            for spec_idx, spec in enumerate(emissions):
                feature.SetField(self.species_names[spec_idx], spec)

            # Add feature to the layer
            layer.CreateFeature(feature)
