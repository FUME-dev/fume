"""
Description: Base module for postprocessor classes

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

def requires(*names):
    """
    Decorator for the DataReceiver classes allowing to specify dependencies
    between packs.

    Example usage in DataReceiver class: define receiver method for getting
    the area emission data dependent on the prior knowledge of the area
    species.

    @requires('species')
    def receive_area_emiss(self, timestep, data):
        ...
    """
    def f_wrap(f):
        def f_wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        f_wrapped._requires = names
        return f_wrapped
    return f_wrap


class DataReceiver():
    """
    Base class for data receivers
    """

    def __init__(self, *args, **kwargs):
        if 'cfg' in kwargs:
            self.cfg = kwargs['cfg']

        if 'rt_cfg' in kwargs:
            self.rt_cfg = kwargs['rt_cfg']

        if 'db' in kwargs:
            self.db = kwargs['db']
