"""
Description: helper debugging classes and functions

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

import time

class FakeDBConnection():
    source_schema = 'sources'
    case_schema = 'test'

    def mogrify(self, q, pars):
        return q.replace('%s', '{}').format(*pars)

    def execute(self, q):
        return True

    def cursor(self):
        return self


class ExecTimer():
    def __init__(self, name='', logger=None):
        self.name = name
        self.logger = logger

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.print_split()

    def print_split(self):
        if self.logger:
            self.logger.info('*** {} execution time: {}'.format(self.name,
                                                                self.duration))

    @property
    def duration(self):
        return time.time()-self.start
