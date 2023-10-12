"""
Description:
This module defines several functions for importing data from csv file into database. Main function ep_csv2table is general function that covers most of the needed cases. Then there are derived specialised functions: ep_read_model_specie_names, ep_read_sp_mod_specie_mapping, ep_read_ep_comp_mechanisms_assignment and ep_read_gspro are special function dedicated to read particular file. In all cases (except gspro file input) the tables in dtb are truncated first despite the scratch parameter from main config file (data are not added to existing tables).
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

import csv
import psycopg2
from psycopg2 import extras
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)

def ep_csv2table(con, filename, schema, tablename, fieldnames, foreign_key = [], naval = '#N/A'):
    """
    Reads csv file to an existing dtb table. 
  
    The parameter foreign_key is used when the input values in some column(s) will not be stored directly, but will be converted to some other value based on the foreign table.
    E.g. it does indexing based on a foreign table. 
    Example: we are reading the speciation profiles, and during the import process we need to index inventory specie name based on idx from different table. 
             fieldnames = ['cat_id', 'inv_specie', 'chem_comp_id', 'fraction']
             foreign_key = [('inv_specie', 'ep_in_species', 'name', 'spec_in_id')]
        This means that fieldname inv_specie will be searched in table ep_in_species based on column "name", than spec_in_id will be stored in table. 
    The function does not work if file has only one column.
  
    Parameters:
    con: connection to database
    filename (str): file (including path) to read
    schema (str): schema for the resulting table
    tablename (str): table name where data will be stored
    fieldnames (list of str): list of column names that will be read from filename. 
    foreign_key (list of tuples of str): defines foreign keys that should be used to convert values from import data
    naval (str): NA string in input files 
  
    Returns:
    None
  
    """

    with open(filename, mode='r', encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        header = ep_read_header(reader)

        # get order of columns
        colindex = []
        for field in fieldnames:
            try:
                colindex.append(header.index(field))
            except ValueError:
                log.fmt_error('Mandatory column {field} not found in file {filename}.', field=field, filename=filename)
                raise

        # replace column name based on foreign key
        foreign_key_i = []
        for old_name, ftable, fcol, fid in foreign_key:
            fieldnames[fieldnames.index(old_name)] = fid
            foreign_key_i.append((header.index(old_name), ftable, fcol, fid))

        sql = 'INSERT INTO "{}"."{}" ({}) VALUES %s'.format(schema, tablename, ', '.join(h for h in fieldnames))

        with con.cursor() as cur:
            lines = []
            try:
                cur.execute('TRUNCATE TABLE "{}"."{}" RESTART IDENTITY CASCADE'.format(schema, tablename))
                for line in reader:
                    if ep_skip_line(line):
                        continue
                
                    # fetch foreign_key
                    for i, ftable, fcol, fid in foreign_key_i:
                        cur.execute('SELECT {} FROM "{}"."{}" WHERE {} = %s'.format(fid, schema, ftable, fcol), (line[i], ))
                        f_id = cur.fetchone()[0]
                        line[i] = str(f_id)
                    line = [item.strip() for item in line]
                    # replace empty string and NA to be None
                    line = [None if item == '' else item for item in line]
                    line = [None if item == naval else item for item in line]
                    lines.append([line[i] for i in colindex])
                extras.execute_values(cur, sql, lines)
                con.commit()
            except:
                log.fmt_error("Unable to import data from file {filename} into table {schema}.{tablename}.", filename=filename, schema=schema, tablename=tablename)
                con.rollback()
                raise


def ep_read_model_specie_names(con, filename, schema, tablename, naval='#N/A'):

    """ Reads model specie names from CSV file filename into an existing table schema.tablename.
    """

    fieldnames = ['model', 'version', 'mechanism', 'name', 'description']
    tab_col_names = ['model_id', 'mech_id', 'name', 'description']
    
    sql = 'INSERT INTO "{}"."{}" ({}) VALUES %s'.format(schema, tablename, ', '.join(h for h in tab_col_names))

    with open(filename, mode='r', encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        header = ep_read_header(reader)

        # get order of columns
        colindex = []
        for field in fieldnames:
            try:
                colindex.append(header.index(field))
            except ValueError:
                log.fmt_error('Mandatory column {field} not found in file {filename}.', field=field, filename=filename)
                raise

        with con.cursor() as cur:
            lines = []        
            try:
                cur.execute('TRUNCATE TABLE "{}"."{}" RESTART IDENTITY CASCADE'.format(schema, tablename))
                for line in reader:
                    if ep_skip_line(line):
                        continue

                    outline = []
                    line = [line[i] for i in colindex]
                    # fetch foreign_keys
                    # model id
                    cur.execute('SELECT model_id FROM "{}"."ep_aq_models" '
                                'WHERE name = %s AND version = %s'.format(schema), (line[0], line[1]))
                    outline.append(str(cur.fetchone()[0]))
                    # mechanism id
                    cur.execute('SELECT mech_id FROM "{}"."ep_mechanisms" '
                                'WHERE name = %s'.format(schema), (line[2], ))
                    outline.append(str(cur.fetchone()[0]))
                    outline.append(line[3])
                    outline.append(line[4])

                    outline = [item.strip() for item in outline]
                    # replace empty string and NA to be None
                    outline = [None if item == '' else item for item in outline]
                    outline = [None if item == naval else item for item in outline]
                    lines.append(outline)    
                extras.execute_values(cur, sql, lines)
                con.commit()
            except:
                log.fmt_error("Unable to import data from file {filename} into table {schema}.{tablename}.", filename=filename, schema=schema, tablename=tablename)
                con.rollback()
                raise


def ep_read_sp_mod_specie_mapping(con, filename, schema, tablename, naval='#N/A'):
    """ Reads model specie mapping file. 
    """

    fieldnames = ['model', 'version', 'mechanism', 'spec_sp_name', 'spec_mod_name', 'map_fact']
    tab_col_names = ['model_id', 'mech_id', 'spec_sp_name', 'spec_mod_name', 'map_fact']
    
    sql = 'INSERT INTO "{}"."{}" ({}) VALUES %s'.format(schema, tablename, ', '.join(h for h in tab_col_names))

    try:
        with open(filename, mode='r', encoding='utf8') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            header = ep_read_header(reader)

            # get order of columns
            colindex = []
            for field in fieldnames:
                try:
                    colindex.append(header.index(field))
                except ValueError:
                    log.fmt_error('Mandatory column {field} not found in file {filename}.', field=field, filename=filename)
                    raise

            with con.cursor() as cur:
                lines = []
                try:
                    cur.execute('TRUNCATE TABLE "{}"."{}" RESTART IDENTITY CASCADE'.format(schema, tablename))
                    for line in reader:
                        if ep_skip_line(line):
                            continue

                        outline = []
                        line = [line[i] for i in colindex]
                        # fetch foreign_keys
                        # model id
                        cur.execute('SELECT model_id FROM "{}"."ep_aq_models" '
                                    'WHERE name = %s AND version = %s'.format(schema), (line[0], line[1]))
                        mod_id = str(cur.fetchone()[0])
                        outline.append(mod_id)
                        # mechanism id
                        cur.execute('SELECT mech_id FROM "{}"."ep_mechanisms" '
                                    'WHERE name = %s'.format(schema), (line[2], ))
                        mech_id = str(cur.fetchone()[0])
                        outline.append(mech_id)
                        # spec_sp_name
                        outline.append(line[3])
                        # spec_mod_name
                        outline.append(line[4])
                        # mapping factor
                        outline.append(line[5])

                        outline = [item.strip() for item in outline]
                        # replace empty string and NA to be None
                        outline = [None if item == '' else item for item in outline]
                        outline = [None if item == naval else item for item in outline]
                        lines.append(outline)                    
                    extras.execute_values(cur, sql, lines)
                    con.commit()
                    return True
                except:
                    log.fmt_error("Unable to import data from file {filename} into table {schema}.{tablename}.", filename=filename, schema=schema, tablename=tablename)
                    con.rollback()
                    raise
    except FileNotFoundError:
        return False


def ep_read_ep_comp_mechanisms_assignment(con, filename, schema, tablename, naval='#N/A'):

    """ Reads compounds to mechanism assignment file.
    """

    fieldnames = ['mechanism_name', 'chem_comp_id', 'name', 'react_fact']
    tab_col_names = ['mech_id', 'chem_comp_id', 'spec_sp_id', 'react_fact']

    sql = 'INSERT INTO "{}"."{}" ({}) VALUES %s'.format(schema, tablename, colnames = ', '.join(h for h in tab_col_names))

    with open(filename, mode='r', encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        header = ep_read_header(reader)

        # get order of columns
        colindex = []
        for field in fieldnames:
            try:
                colindex.append(header.index(field))
            except ValueError:
                log.fmt_error('Mandatory column {field} not found in file {filename}.', field=field, filename=filename)
                raise

        with con.cursor() as cur:
            lines = []        
            try:
                cur.execute('TRUNCATE TABLE "{}"."{}" RESTART IDENTITY CASCADE'.format(schema, tablename))
                for line in reader:
                    if ep_skip_line(line):
                        continue

                    outline = []
                    line = [line[i] for i in colindex]
                    # fetch foreign_keys
                    # mechanism id
                    cur.execute('SELECT mech_id FROM "{}"."ep_mechanisms" '
                                'WHERE name = %s'.format(schema), (line[0], ))
                    mech_id = str(cur.fetchone()[0])
                    outline.append(mech_id)
                    outline.append(line[1])
                    # spec_sp_id
                    cur.execute('SELECT spec_sp_id FROM "{}"."ep_sp_species" '
                                'WHERE name = %s AND mech_id = %s'.format(schema), (line[2], mech_id))
                    outline.append(str(cur.fetchone()[0]))
                    outline.append(line[3])

                    outline = [item.strip() for item in outline]
                    # replace empty string and NA to be None
                    outline = [None if item == '' else item for item in outline]
                    outline = [None if item == naval else item for item in outline]
                    lines.append(outline)
                extras.execute_values(cur, sql, lines)
                con.commit()
            except:
                log.fmt_error("Unable to import data from file {filename} into table {schema}.{tablename}.", filename=filename, schema=schema, tablename=tablename)
                con.rollback()
                raise


def ep_read_gspro(con, filename, schema, tablename):
    """
    Reads SMOKE gspro file into database table.
  
    This file has 6 column (columns 7 and higher can be present but they are omitted). 
    Any line beginning with # is treated as comment and omitted. Considers comma, semicolon, tab and
    space as column delimiter.
  
    Parameters:
    con: connection to database
    filename (str): file (including path) to read
    schema (str): schema for the resulting table
    tablename (str): table name where data will be stored
  
    Returns:
    None
  
    """
    
    import re

    with open(filename, mode='r', encoding='utf8') as f:
        with con.cursor() as cur:
            lines = []        
            try:
                # find mechanism id
                for line in f:
                    if ep_skip_line(line):
                        continue
                    mechanism = re.split(';|,|[ ]+|\t+', line.rstrip('\n'))[0]
                    cur.execute('SELECT mech_id FROM "{}".ep_mechanisms WHERE name = %s'.format(schema), (mechanism, ))
                    mech_id = str(cur.fetchone()[0])
                    break

                for line in f:
                    if ep_skip_line(line):
                        continue
                    line = [l.strip('"') for l in re.split(';|,|[ ]+|\t+', line.rstrip('\n'))]

                    # fetch foreign_keys
                    # spec_in_id
                    cur.execute('SELECT spec_in_id FROM "{}".ep_in_species WHERE name = %s'.format(schema), (line[1], ))
                    spec_in_id = str(cur.fetchone()[0])
                    # spec_sp_id
                    cur.execute('SELECT spec_sp_id FROM "{}".ep_sp_species '
                                'WHERE name = %s AND mech_id = %s'.format(schema), (line[2], mech_id))
                    spec_sp_id = str(cur.fetchone()[0])

                    outline = [mech_id, line[0], spec_in_id, spec_sp_id, line[3], line[4], line[5]]
                    lines.append(outline)
                extras.execute_values(cur, 'INSERT INTO "{}".ep_gspro_sp_factors (mech_id, cat_id, spec_in_id, spec_sp_id, '
                                'mole_split_factor, mol_weight, mass_split_factor) VALUES %s'.format(schema), lines)
                                
                # check whether profiles sum up to one +- 5%  !! omitted VOC profiles does not need to be 1 !
                #sql = 'CREATE TEMP TABLE gspro_sums ON COMMIT DROP AS SELECT cat_id, eis.name, sum(mole_split_factor) ' \
                #'FROM "{schema}".ep_gspro_sp_factors egsf JOIN "{schema}".ep_in_species eis USING (spec_in_id) WHERE mech_id = {} group by cat_id, eis.name'  
                #cur.execute(sql.format(mech_id, schema=schema))                 
                #cur.execute('SELECT cat_id, name FROM gspro_sums WHERE sum > 1.05 OR sum < 0.95')
                #for wrong in cur.fetchall():
                # 	log.fmt_warning('Chemical speciation profile (gspro) for mechanism {mech}, category {cat} and ' \
                #	'specie {spec} does not sum up to 1 (with 5% tolerance).', mech=mechanism, cat=wrong[0], spec=wrong[1])
                con.commit()
            except:
                log.fmt_error("Unable to import data from file {filename} into table {schema}.{tablename}.", filename=filename, schema=schema, tablename=tablename)
                con.rollback()
                raise


def ep_skip_line(line):
    """ line is a list, this function returns True in case the line is empty or starts with # - (will not be processed further), otherwise False"""
    try:
        test = line[0]
    except IndexError:
        return True

    if test.startswith('#'):
        return True

    return False


def ep_read_header(reader):
    """ returns first valid line (assume to be header), reader is csv.reader"""
    for line in reader:
        if ep_skip_line(line):
            continue
        else:
            return [colname.strip() for colname in line]
