"""
Description: Configuration module
If this module is executed by itself, it loads the default configspec file and
prints out an example config file with all the default values. Note that values
without defaults are omitted, therefore the produced config file might be
invalid (incomplete).
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

__all__ = ['cfgfile', 'ep_cfg', 'init_global', 'ConfigFile']

import sys
import os
import re
import datetime
import glob

import configobj
import validate

# Upon importing this module, locate its directory and use it as the default
# cfgspec directory
default_cfgspec_dir = os.path.dirname(os.path.abspath(__file__))

# Parsed configspecs
configspecs = {}
def get_configspec(*add_files):
    '''(Pre)loads and returns the requested configspec.
    
    Arguments:
        add_files:   Additional configspecs to parse.
    '''
    global configspecs

    specs = []
    for fn in ('configspec.conf',) + add_files:
        try:
            # Try to use preloaded configspec file
            specs.append(configspecs[fn])
        except KeyError:
            # Parse configspec file
            spec = configobj.ConfigObj(os.path.join(default_cfgspec_dir, fn),
                    encoding='UTF8', file_error=True, list_values=False,
                    _inspec=True)
            configspecs[fn] = spec
            specs.append(spec)

    # Merge configspecs
    s = specs[0]
    for spec in specs[1:]:
        s.merge(spec)
    return s

class ConfigValues(object):
    '''A syntactic-sugar helper for accessing values using dot notation instead
    of getitem-like brackets. Supports also direct iterating over keys.
    '''
    def __init__(self, d):
        subsections = [(n, ConfigValues(v)) for n, v in d.items() if isinstance(v, dict)]
        d.update(subsections)
        self.__dict__ = d

    def __iter__(self):
        return iter(self.__dict__)

    def __getitem__(self, key):
        return self.__dict__[key]

class ConfigValidationError(Exception):
    pass

class ConfigFile(object):
    '''A configuration loaded from one or more files that can be saved back
    (preserving comments and file structure)'''

    def __init__(self, cfg_fnames, cfgspec):
        '''Loads config from a single file or multiple files

        If ``cfg_fnames`` is a sequence, load config from all the specified
        filenames and merge them iteratively.

        After loading config, validate it using given ``cfgspec``.
        '''
        fns = [cfg_fnames] if isinstance(cfg_fnames, str) else cfg_fnames
        cobjs = [configobj.ConfigObj(fname, encoding='UTF8', configspec=cfgspec,
                    interpolation='Template')
                for fname in fns]

        # Take first config and merge it with all others
        self.cobj = cobj = cobjs[0]
        for c in cobjs[1:]:
            cobj.merge(c)

        valres = cobj.validate(validator, preserve_errors=True)
        if valres is not True:
            errs = ['The configuration contains following errors:\n\n']
            for seclist, key, err in configobj.flatten_errors(cobj, valres):
                if key is None:
                    errs.extend(['Missing section ', ':'.join(seclist), '\n'])
                elif err is False:
                    errs.extend(['Missing ', key, ' in section ',
                        ':'.join(seclist), '\n'])
                else:
                    errs.extend([key, ' (section ',
                        ':'.join(seclist), '): ', str(err), '\n'])
            raise ConfigValidationError(''.join(errs))

    @classmethod
    def from_string(cls, cfgstring):
        '''Loads config from a string instead of file(s).'''

        obj = object.__new__(cls)
        obj.cobj = configobj.ConfigObj(cfgstring.splitlines(), encoding='UTF8',
                    interpolation='Template')
        return obj

    def values(self):
        '''Return values accessible like config.section.subsection.value instead
        of brackets'''
        return ConfigValues(self.cobj.dict())

    def __getitem__(self, key):
        return self.cobj[key]

def init_global(cfg_fnames, cfgspec_dir=None):
    '''Load config from files and save it within this module

    In this usage pattern, all the modules that want to use configuration only
    need to write:
    >> from ep_config import ep_cfg
    >> do_whatever_with(ep_cfg.some_section.some_value)
    '''
    global cfgfile, ep_cfg, default_cfgspec_dir

    if cfgspec_dir is not None:
        default_cfgspec_dir = cfgspec_dir

    cfgfile = ConfigFile(cfg_fnames, get_configspec())

    # Check for values that require additional configspecs
    add_specs = []
    if cfgfile['run_params']['output_params']['model'] == 'CMAQ':
        add_specs.append('configspec-cmaq.conf')
    if 'emissplotter' in cfgfile['postproc']:
        add_specs.append('configspec-postproc-emissplotter.conf')
    # ... further checks here ...

    if add_specs:
        # Reload config with added configspecs
        cfgfile = ConfigFile(cfg_fnames, get_configspec(*add_specs))

    ep_cfg = cfgfile.values()
    init_specific_cfg(ep_cfg)

class EmptyConfig(object):
    '''An empty placeholder that raises error whenever used (until
    init_global() is called to load proper config)'''
    def __getattr__(self, attr):
        raise RuntimeError('Global config not loaded yet, call '
                'ep_config.init_global() first')


cfgfile = ep_cfg = EmptyConfig()

################################################################################
# Custom validator functions
################################################################################

def vtor_datetime(value, format=None, formats=['%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S.%f']):
    if format is not None:
        if not isinstance(format, str):
            raise validate.VdtParamError('format', format)
        fmts = [format]
    else:
        try:
            if not all(isinstance(f, str) for f in formats):
                raise validate.VdtParamError('formats', formats)
        except TypeError:
            raise validate.VdtParamError('formats', formats)
        fmts = formats

    for fmt in fmts:
        try:
            return datetime.datetime.strptime(value, fmt)
        except ValueError:
            pass
    raise validate.VdtValueError(value)


# Global validator instance which includes custom validator funcs
validator = validate.Validator({
    'datetime':     vtor_datetime,
    })

################################################################################
# Handling of specific parts of configfile
################################################################################

re_glob = re.compile(r'.*[?\*[]') #Matches shell globbing patterns

def init_specific_cfg(cfg):

    ##### ep_cfg.input_params.met.met_paths #####

    # Build paths using globbing for each filepath
    met_path = cfg.input_params.met.met_path
    cfg.input_params.met.met_paths = met_paths = []
    for fname in cfg.input_params.met.met_files:
        fpath = os.path.join(met_path, fname)
        if re_glob.match(fpath):
            # Shell patterns, use globbing
            met_paths.extend(sorted(glob.glob(fpath)))
        else:
            # Exact filename
            met_paths.append(fpath)
    if not met_paths:
        raise ConfigValidationError('No input files found matching input_params.met_files')

if __name__ == '__main__':
    # Print out the default config
    cobj = configobj.ConfigObj(None, encoding='UTF8',
            configspec=get_configspec())
    cobj.validate(validator, copy=True)
    cobj.initial_comment[0:1] = ['Example configuration composed of default values']
    cobj.write(sys.stdout.buffer)
