import os
import struct
import numpy  as np

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
        print ('EE: Error while writing the record.')
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
        print ('EE: Error while writing the record.')
        raise
        

def ResultIter(cursor, arraysize=1000):
#   An iterator that uses fetchmany to keep memory usage down
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result



      
    
  
