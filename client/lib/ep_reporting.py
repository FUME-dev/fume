"""
Description: EP reporting module.

Import this module as:
    import lib.ep_reporting
    report = lib.ep_reporting.Reporter(__name__)

Then perform reporting by calling report class (record, check, sum)
and appropriate method (currently message and sql implemented).

Examples:
Message method:
- prints simple message (same as fromatted logging)
  report.record.message('This reports {something} and also the {}.',
      'other thing', something='something')
- this prints the above message to the record report file.

Sql method:
- prints string, then performs select query end prints its output to the sum file:
report.sum.sql('This is the table ep_inventory.',
    'SELECT {}, {} FROM "{schema}"."ep_invenotry"', "col1", "col2", schema='sources_schema')

See configspec for description of reporting options.
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
import csv
from datetime import datetime
from lib.ep_config import ep_cfg
from lib.ep_libutil import ep_connection, ep_ResultIter, ep_rtcfg

import lib.ep_logging
lib.ep_logging.configure()
log = lib.ep_logging.Logger(__name__)

report_types = ['record', 'check', 'sum']

module_types = {}
registered_reporters = []
    

class ReportNoop:
    """Reporting class containing report methods doing nothing 
    (the specified reporting type is disabled)."""

    def __init__(self, rtype):
        self.outfile = outfile_names[rtype]
        
    def message(*args, **kwargs):
        pass
        
    def sql(*args, **kwargs):
        pass


class ReportWritter:
    """Reporting class containing different types of report outputs."""

    def __init__(self, rtype, modulename):
        self.outfile = outfile_names[rtype]     
        self.module = modulename              
        if rtype == 'sum':
           self.sql = self.sum_sql
        if rtype == 'check':
           self.sql = self.check_sql
        
    def write_mod_title(func):
        """ Decorator of report writter methods, to create title on each module section."""
        def wrapper(self, *args, **kwargs):
            if ep_rtcfg['last_report_modname'] != self.module: 
                with open(self.outfile, 'a') as f:  
                    if os.path.getsize(self.outfile) > 0:            
                        f.write('\n')
                    out_str = '*' * 32 + '\n' + self.module + '\n' + '*' * 32 + '\n'
                    f.write(out_str)
                ep_rtcfg['last_report_modname'] = self.module
            func(self, *args, **kwargs)
        return wrapper     
        
    @write_mod_title
    def message(self, s, *args, **kwargs):        
        with open(self.outfile, 'a') as f:
            f.write(s.format(*args, **kwargs)) 
            f.write('\n')            
        
    @write_mod_title
    def sql(self, sql_message = '', sql_query = '', *args, **kwargs):
        with ep_connection.cursor() as cur:
            if sql_query:
                cur.execute(sql_query.format(*args, **kwargs)) 
                sql_result = [list(row)[0] if len(row)==1 else str(row) for row in cur.fetchall()] 
            else:
                sql_result = []
            
        with open(self.outfile, 'a') as f:
            f.write(sql_message + ': ') 
            f.write(', '.join(map(str, sql_result)))   
            f.write('\n') 

    @write_mod_title
    def check_sql(self, message, sql_message = '', sql_query = '', *args, **kwargs):
        """ Always prints message. Prints sql_message only if sql_query output not empty. """
        with ep_connection.cursor() as cur:
            if sql_query:
                cur.execute(sql_query.format(*args, **kwargs)) 
                sql_result = [list(row)[0] if len(row)==1 else str(row) for row in cur.fetchall()] 
            else:
                sql_result = []
       
            with open(self.outfile, 'a') as f:
                f.write(message + '\n') 
                if sql_result:     
                    f.write(sql_message + ': ') 
                    f.write(', '.join(map(str, sql_result)))   
                    f.write('\n') 
    
    @write_mod_title
    def sum_sql(self, sql_message = '', sql_query = '', *args, **kwargs):
        with ep_connection.cursor() as cur:
            if sql_query:
                cur.execute(sql_query.format(*args, **kwargs)) 
                sql_result = [list(row) for row in cur.fetchall()]   
            else:
                sql_result = []
            
        with open(self.outfile, 'a') as f:
            f.write(sql_message + ':\n') 
            wr = csv.writer(f)
            wr.writerows(sql_result)
            f.write('\n')       
    

class Reporter:
    """Main EP reporter class.

    Provides reporting methods for the specified reporting types according to
    configuration. Upon configuration, the methods are directly assigned as
    reporters or dummy no-op methods, so there is no checking done at reporting
    time.
    """
    def __init__(self, modulename=None, custom_type=None):
        self.module = modulename
        self.reinit(custom_type)
        registered_reporters.append(self)

    def reinit(self, new_types=None):
        """(Re)initialize reporter with specified type or (module) default type"""

        if new_types is None:
            types = module_types.get(self.module, default_types)
        else:
            types = [x.lower() for x in new_types]     
        
        for rtype in report_types:   
            if rtype in types:
                setattr(self, rtype, ReportWritter(rtype, self.module))
            else:
                setattr(self, rtype, ReportNoop)           
              

def configure():
    """(Re)configures all reporting using ep_config."""
    global default_types, module_types, outfile_names

    default_types = [x.lower() for x in ep_cfg.reporting.type] 
    
    module_types = {}
    for modname in ep_cfg.reporting.module_types:
        module_types[modname] = [x.lower() for x in ep_cfg.reporting.module_types[modname]]
    
    outfile_path = ep_cfg.reporting.outfiles_path
    outfile_names = {}
    for rtype in report_types:
        outfile_names[rtype] = os.path.join(outfile_path, 'report_' + rtype.upper() + '_' + str(datetime.timestamp(datetime.now())*1000).split('.', 1)[0] + '.txt')
    
    if not os.path.exists(outfile_path):
        os.makedirs(outfile_path)    
      
    used_types = list(set(default_types) | set([item for sublist in module_types.values() for item in sublist]))  
    for rtype in list(set(used_types) & set(report_types)):
        open(outfile_names[rtype], 'w+').close()
        log.fmt_info('The file {} was created for {} report.', outfile_names[rtype], rtype.upper())                 
                
