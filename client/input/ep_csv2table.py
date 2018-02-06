import csv
import psycopg2


def ep_csv2table(con, filename, schema, tablename, fieldnames, foreign_key = [], naval = '#N/A'):

    """ From CSV file filename reads data to an existing table schema.tablename.
    """

    # TODO distinguish between required and optional columns
    # TODO names of columns in csv x in tables doesn't have to be same
    # TODO what if only one column is on input --- problem with (a,)

    with open(filename, mode='r', encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        header = [colname.strip() for colname in next(reader)]

        # get order of columns
        colindex = []
        for field in fieldnames:
            try:
                colindex.append(header.index(field))
            except ValueError:
                print('Mandatory column {} not found in file {}.'.format(field, filename))
                raise

        # replace column name based on foreign key
        foreign_key_i = []
        for old_name, ftable, fcol, fid in foreign_key:
            fieldnames[fieldnames.index(old_name)] = fid
            foreign_key_i.append((header.index(old_name), ftable, fcol, fid))

        with con.cursor() as cur:
            try:
                # first empty the table
                # TODO ?
                cur.execute('TRUNCATE TABLE "{}"."{}" RESTART IDENTITY CASCADE'.format(schema, tablename))
                for line in reader:
                    # fetch foreign_key
                    for i, ftable, fcol, fid in foreign_key_i:
                        cur.execute('SELECT {} FROM "{}"."{}" WHERE {} = %s'.format(fid, schema, ftable, fcol), (line[i], ))
                        f_id = cur.fetchone()[0]
                        line[i] = str(f_id)
                    colnames = ', '.join(h for h in fieldnames)
                    s_string = ', '.join('%s' for _ in fieldnames)
                    sql = 'INSERT INTO "{}"."{}" ({}) VALUES ({})'.format(schema, tablename, colnames, s_string)
                    line = [item.strip() for item in line]
                    # replace empty string to be None
                    # TODO should we do this?
                    line = [None if item == '' else item for item in line]
                    # replace NA string to be None
                    line = [None if item == naval else item for item in line]
                    cur.execute(sql, [line[i] for i in colindex])
                con.commit()
            except psycopg2.ProgrammingError:
                print("Unable to import data from file {} into table {}.{}.".format(filename, schema, tablename))
                con.rollback()
                raise


# TODO try to rewrite csv2table that following functions are not necessary

def ep_read_model_specie_names(con, filename, schema, tablename, naval='#N/A'):

    """ Reads model specie names from CSV file filename into an existing table schema.tablename.
    """

    fieldnames = ['model', 'version', 'mechanism', 'name', 'description']
    tab_col_names = ['model_id', 'mech_id', 'name', 'description']

    with open(filename, mode='r', encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        header = [colname.strip() for colname in next(reader)]

        # get order of columns
        colindex = []
        for field in fieldnames:
            try:
                colindex.append(header.index(field))
            except ValueError:
                print('Mandatory column {} not found in file {}.'.format(field, filename))
                raise

        with con.cursor() as cur:
            try:
                # first empty the table
                cur.execute('TRUNCATE TABLE "{}"."{}" RESTART IDENTITY CASCADE'.format(schema, tablename))
                for line in reader:
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

                    colnames = ', '.join(h for h in tab_col_names)
                    s_string = ', '.join('%s' for _ in tab_col_names)
                    sql = 'INSERT INTO "{}"."{}" ({}) VALUES ({})'.format(schema, tablename, colnames, s_string)
                    outline = [item.strip() for item in outline]
                    # replace empty string to be None
                    # TODO should we do this?
                    outline = [None if item == '' else item for item in outline]
                    # replace NA string to be None
                    outline = [None if item == naval else item for item in outline]
                    cur.execute(sql, outline)
                con.commit()
            except psycopg2.ProgrammingError:
                print("Unable to import data from file {} into table {}.{}.".format(filename, schema, tablename))
                con.rollback()
                raise


def ep_read_sp_mod_specie_mapping(con, filename, schema, tablename, naval='#N/A'):

    fieldnames = ['model', 'version', 'mechanism', 'spec_sp_name', 'spec_mod_name', 'map_fact']
    tab_col_names = ['model_id', 'mech_id', 'spec_sp_name', 'spec_mod_name', 'map_fact']

    try:
        with open(filename, mode='r', encoding='utf8') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            header = [colname.strip() for colname in next(reader)]

            # get order of columns
            colindex = []
            for field in fieldnames:
                try:
                    colindex.append(header.index(field))
                except ValueError:
                    print('Mandatory column {} not found in file {}.'.format(field, filename))
                    raise

            with con.cursor() as cur:
                try:
                    # first empty the table
                    cur.execute('TRUNCATE TABLE "{}"."{}" RESTART IDENTITY CASCADE'.format(schema, tablename))
                    for line in reader:
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

                        colnames = ', '.join(h for h in tab_col_names)
                        s_string = ', '.join('%s' for _ in tab_col_names)
                        sql = 'INSERT INTO "{}"."{}" ({}) VALUES ({})'.format(schema, tablename, colnames, s_string)
                        outline = [item.strip() for item in outline]
                        # replace empty string to be None
                        # TODO should we do this?
                        outline = [None if item == '' else item for item in outline]
                        # replace NA string to be None
                        outline = [None if item == naval else item for item in outline]
                        cur.execute(sql, outline)
                    con.commit()
                    return True
                except psycopg2.ProgrammingError:
                    print("Unable to import data from file {} into table {}.{}.".format(filename, schema, tablename))
                    con.rollback()
                    raise
    except FileNotFoundError:
        return False


def ep_read_ep_comp_mechanisms_assignment(con, filename, schema, tablename, naval='#N/A'):

    fieldnames = ['mechanism_name', 'chem_comp_id', 'name', 'react_fact']
    tab_col_names = ['mech_id', 'chem_comp_id', 'spec_sp_id', 'react_fact']

    with open(filename, mode='r', encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        header = [colname.strip() for colname in next(reader)]

        # get order of columns
        colindex = []
        for field in fieldnames:
            try:
                colindex.append(header.index(field))
            except ValueError:
                print('Mandatory column {} not found in file {}.'.format(field, filename))
                raise

        with con.cursor() as cur:
            try:
                # first empty the table
                cur.execute('TRUNCATE TABLE "{}"."{}" RESTART IDENTITY CASCADE'.format(schema, tablename))
                for line in reader:
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

                    colnames = ', '.join(h for h in tab_col_names)
                    s_string = ', '.join('%s' for _ in tab_col_names)
                    sql = 'INSERT INTO "{}"."{}" ({}) VALUES ({})'.format(schema, tablename, colnames, s_string)
                    outline = [item.strip() for item in outline]
                    # replace empty string to be None
                    # TODO should we do this?
                    outline = [None if item == '' else item for item in outline]
                    # replace NA string to be None
                    outline = [None if item == naval else item for item in outline]
                    cur.execute(sql, outline)
                con.commit()
            except psycopg2.ProgrammingError:
                print("Unable to import data from file {} into table {}.{}.".format(filename, schema, tablename))
                con.rollback()
                raise


def ep_read_gspro(con, filename, schema, tablename):
    """
    :param con: conection to database
    :param filename: file (including path) to read
    :param schema: schema for the resulting table
    :param tablename: table name where data will be stored
    :return:
    This reads SMOKE gspro file into database table. This file has 6 column (columns 7 and higher can be present but a
    nd are omitted). Any line beginning with # is treated as comment and omitted. Considers comma, semicolon, tab and
    space as column delimiter.
    """

    import re

    with open(filename, mode='r', encoding='utf8') as f:
        with con.cursor() as cur:
            try:
                # find mechanism id
                for line in f:
                    if line.startswith('#'):
                        continue
                    mechanism = re.split(';|,|[ ]+|\t+', line.rstrip('\n'))[0]
                    cur.execute('SELECT mech_id FROM "{}".ep_mechanisms WHERE name = %s'.format(schema), (mechanism, ))
                    mech_id = str(cur.fetchone()[0])
                    break

                for line in f:
                    if line.startswith('#'):
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
                    cur.execute('INSERT INTO "{}".ep_gspro_sp_factors (mech_id, cat_id, spec_in_id, spec_sp_id, '
                                'mole_split_factor, mol_weight, mass_split_factor) '
                                'VALUES (%s, %s, %s, %s, %s, %s, %s)'.format(schema), outline)
                con.commit()
            except psycopg2.ProgrammingError:
                print("Unable to import data from file {} into table {}.{}.".format(filename, schema, tablename))
                con.rollback()
                raise
