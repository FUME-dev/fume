"""
Description:

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

class ep_met_data():
    """
    Helper object implementing basic arithmetic operators for
    meteorological data.
    """
    def getName(self):
        return self.name

    def getData(self):
        return self.data

    def getDim(self):
        return(self.data.size)

    def __init__(self, name, npdata):
        self.name = name
        self.data = npdata.copy()

    def __mul__(self,x):
        newdata = self.data * x    
        newname = self.name
        return (ep_met_data(newname, newdata))

    def __truediv__(self,x):
        newdata = self.data / x
        newname = self.name
        return (ep_met_data(newname, newdata))

    def __add__(self,x):
        if self.name != x.name:
            return (NotImplemented)
        else:
            newdata = self.data + x.data
            newname = self.name
            return (ep_met_data(newname, newdata))

    def __sub__(self,x):
        if self.name != x.name:
            return (NotImplemented)
        else:
            newdata = self.data - x.data
            newname = self.name
            return (ep_met_data(newname, newdata))


