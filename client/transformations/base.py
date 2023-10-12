"""
Description: Base classes and helper functions for the transformations module.

Transformation: Base abstract class for all transformations, declares an
                abstract method apply all transformations must implement

OneToOneTransformation: Base class for transformations that perform any
    "conversion" from one relation to another one

TwoToOneTransformation: Base class for transformations that perform any
    "conversion" from two relations to one relation
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

from abc import ABC, abstractmethod


class Transformation(ABC):
    """
    Base abstract transformation class

    Abstract methods:
    apply -- runs the transformation

    If the virtual attribute is True, the transformation does not
    create an intermediate database table in the transformation chain.
    """

    virtual = False
    def __init__(self, *args, **kwargs):
        self.has_coef = False

    @abstractmethod
    def apply(self):
        pass


class OneToOneTransformation(Transformation):
    """
    Base class for transformation between one existing relation and a new one

    Attributes:
    inrelation -- existing relation to be transformed
    outrelation -- new relation created from the transformation
    outsrid -- Spatial reference identifier of the new relation
    """

    parameters = ['inrelation', 'outrelation', 'outsrid']

    def __init__(self, inrel=None, outrel=None, outsrid=None):
        super().__init__()
        self.inrelation = inrel
        self.outrelation = outrel
        self.outsrid = outsrid


class TwoToOneTransformation(Transformation):
    """
    Base class for transformation between two existing relations and a new one

    Attributes:
    inrelation -- existing relation to be transformed
    inrelation2 -- existing relation to be transformed
    outrelation -- new relation created from the transformation
    outsrid -- Spatial reference identifier of the new relation
    """
    parameters = ['inrelation', 'inrelation2', 'outrelation', 'outsrid']

    def __init__(self, inrel1=None, inrel2=None, outrel=None, outsrid=None):
        super().__init__()
        self.inrelation = inrel1
        self.inrelation2 = inrel2
        self.outrelation = outrel
        self.outsrid = outsrid


def virtual(cls):
    """
    Decorator function for denoting a virtual transformation class

    Example:
    @virtual
    class ScenarioTransformation(Transformation):
        ...
    """
    cls.virtual = True
    return cls
