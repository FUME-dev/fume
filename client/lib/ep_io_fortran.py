"""
Description: I/O functions for writing binary data (e.g. CAMx/UAM)

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

import struct

def get_format_string(endian, type_str):
    if endian=='big':
        fmt_mod='>'
    else:
        fmt_mod='<'

    return fmt_mod+type_str


def write_record(of, endian, type_str, *var):
    try:
        fmt_i = get_format_string(endian,'i')
        recordsize = struct.calcsize(type_str)
        fmt_str = get_format_string(endian, type_str)
        of.write( struct.pack(fmt_i, recordsize))
        of.write( struct.pack(fmt_str, *var))
        of.write( struct.pack(fmt_i, recordsize))
    except IOError:
        raise


def read_record(ifile, endian, type_str):
    try:
        fmt_i = get_format_string(endian,'i')
        recordsize = struct.calcsize(type_str)
        fmt_str = get_format_string(endian, type_str)
        size = struct.unpack(fmt_i, ifile.read(4))
        var = struct.unpack(fmt_str,ifile.read(recordsize))
        size = struct.unpack(fmt_i, ifile.read(4))      
        return(var)
    except IOError:
        raise
        

def ResultIter(cursor, arraysize=1000):
#   An iterator that uses fetchmany to keep memory usage down
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result
