"""
Description: EP Logging module.

Import this module as:
    import lib.ep_logging
    log = lib.ep_logging.Logger(__name__)

Then perform logging by calling methods of log:
    log.error(*args):
    log.warning(*args):
    log.info(*args):
    log.debug(*args):
    log.tracing(*args):
        Performs print-like logging (list of args), e.g.:

        log.info('Starting conversion...')
        log.error('Problem number ', number, ' occurred.')

    log.fmt_error(s, *args, **kwargs):
    log.fmt_warning(s, *args, **kwargs):
    log.fmt_info(s, *args, **kwargs):
    log.fmt_debug(s, *args, **kwargs):
    log.fmt_tracing(s, *args, **kwargs):
        Performs formatted logging by s.format(*args, **kwargs), e.g.:

        log.fmt_warning('Hey, the thing {thing} has problems with {} and {}.',
                        problem1, problem2, thing=something)

        This has the advantage that string formatting is not performed if the
        logging does not apply (i.e. if the specified logging level is above
        configured logging level).

    log.sql_error(con):
    log.sql_warning(con):
    log.sql_info(con):
    log.sql_debug(con):
    log.sql_tracing(con):
        Prints all SQL connection notices according to the specified logging
        level, but only if sql_notices is enabled.

See configspec for description of logging levels.
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

import sys
from datetime import datetime
from lib.ep_config import ep_cfg


levels = {
    'ERROR'  : 10,
    'WARNING': 20,
    'INFO'   : 30,
    'DEBUG'  : 40,
    'TRACING': 50,
    }

datefmt = '%Y-%m-%d %H:%M:%S'
default_level = levels['INFO']
sql_notices = False
stderr_from = levels['WARNING']

levels_r = {v: n for n, v in levels.items()}
module_levels = {}
registered_loggers = []
now = datetime.now

def noop(*args, **kwargs):
    """Logging method that does nothing (the specified logging level is disabled)."""
    pass

def sql_noop(con):
    """SQL logging method that prints nothing (the specified logging level is disabled)."""
    con.notices.clear()

def mklogger(log_type, level, module, fout):
    """Prepares a logger method with the given parameters.

    The prepared method works directly without any "if"s, mostly with one print
    command.
    """
    fmt = [datefmt]
    if module:
        fmt.extend([' ', module])
    if log_type == 's':
        fmt.append(' SQL')
    fmt.extend([' ', level, ':'])
    fmt = ''.join(fmt)

    if log_type == 'f':
        # formatted logging (see module doc)
        return lambda s, *args, **kwargs: print(
                now().strftime(fmt), s.format(*args, **kwargs), file=fout)
    elif log_type == 'p':
        # print-like logging (list of args; see module doc)
        return lambda *args: print(now().strftime(fmt), *args, file=fout)
    elif log_type == 's':
        # SQL logging (see module doc)
        def slog(sqlcon):
            h = now().strftime(fmt)
            for n in sqlcon.notices:
                print(h, n, file=fout)
            sqlcon.notices.clear()
        return slog

class Logger:
    """Main EP logger class.

    Provides logging methods for the specified levels according to
    configuration. Upon configuration, the methods are directly assigned as
    loggers or dummy no-op methods, so there is no checking done at logging
    time.
    """
    def __init__(self, modulename=None, custom_level=None):
        self.module = modulename
        self.reinit(custom_level)
        registered_loggers.append(self)

    def reinit(self, new_level=None):
        """(Re)initialize logger with specified level or (module) default level"""

        if new_level is None:
            level = module_levels.get(self.module, default_level)
        else:
            level = new_level

        for lname, lnum in levels.items():
            lnamel = lname.lower()
            if lnum > level:
                setattr(self, lnamel, noop)
                setattr(self, 'fmt_'+lnamel, noop)
                setattr(self, 'sql_'+lnamel, sql_noop)

            else:
                fout = sys.stderr if lnum <= stderr_from else sys.stdout

                setattr(self, lnamel, mklogger('p', lname, self.module, fout))
                setattr(self, 'fmt_'+lnamel, mklogger('f', lname, self.module, fout))
                if sql_notices:
                    setattr(self, 'sql_'+lnamel, mklogger('s', lname, self.module, fout))
                else:
                    setattr(self, 'sql_'+lnamel, sql_noop)


def reinit_all_loggers(custom_level=None):
    for logger in registered_loggers:
        logger.reinit(custom_level)

def configure():
    """(Re)configures all logging using ep_config."""
    global default_level, module_levels, sql_notices

    if ep_cfg.debug:
        print('Found deprecated configuration option "debug". '
                'Use "level = DEBUG" in section [logging].', file=sys.stderr)
        default_level = levels['DEBUG']
    else:
        default_level = levels[ep_cfg.logging.level]

    if ep_cfg.debug_sql:
        print('Found deprecated configuration option "debug_sql". '
                'Use "sql_notices = on" in section [logging].', file=sys.stderr)
        sql_notices = True
    else:
        sql_notices = ep_cfg.logging.sql_notices

    module_levels = {}
    for modname in ep_cfg.logging.module_levels:
        module_levels[modname] = levels[ep_cfg.logging.module_levels[modname]]

    reinit_all_loggers()
