"""
Description: Base module for data providers
A database of hooks, functions that get called when a named "pack hook" is invoked.
    - pack: decorator function to register a method as a data sender
    - DataProvider: base class for data providers
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

from abc import ABCMeta
from collections import defaultdict
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)

def pack(name):
    """
    Decorator for the DataProvider classes registers given method as a pack
    hook.
    """
    def f_wrap(f):
        def f_wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        f_wrapped._pack = name
        return f_wrapped
    return f_wrap

pack_hooks = {}

class DataProviderMeta(ABCMeta):
    """
    Upon class instantiation, go through the methods and find out which ones have
    a _pack attribute and save that function as a pack hook in the 'pack_hooks' database.
    Each pack can have one hook.
    """
    def __new__(cls, name, bases, dct):
        inst = super().__new__(cls, name, bases, dct)
        rank = 0
        for n, o in dct.items():
            if callable(o) and hasattr(o, '_pack'):
                if o._pack in pack_hooks:
                    raise ValueError('Hook already defined for pack {}'.format(o._pack))

                pack_hooks[o._pack] = {'name': n, 'ran': False}
                rank += 1

        return inst


class DataProvider(metaclass=DataProviderMeta):
    """
    Base class for data providers
    """

    def __init__(self, *args, **kwargs):
        self.receivers = defaultdict(list)

        if 'cfg' in kwargs:
            self.cfg = kwargs['cfg']

        if 'rt_cfg' in kwargs:
            self.rt_cfg = kwargs['rt_cfg']

        if 'db' in kwargs:
            self.db = kwargs['db']

    def register_receiver(self, receiver):
        """
        Register a receiver object, typically through the dispatch.run function
        from the list of receiver objects specified in the main configuration.
        If a receive_<pack_name> method exists in the object, the object is
        registered as a receiver.
        """
        log.debug('*** Pack hooks', pack_hooks)
        for pack in pack_hooks:
            rcv_fun = 'receive_{pack}'.format(pack=pack)
            if hasattr(receiver, rcv_fun):
                rcv_fun_obj = getattr(receiver, rcv_fun)
                if callable(rcv_fun_obj):
                    self.receivers[pack].append(receiver)

    def distribute(self, pack, *args, **kwargs):
        """
        Send the data for a given pack to all registered receivers.
        Must be called explicitely by a pack hook method in a data provider
        class.
        """
        for r in self.receivers[pack]:
            getattr(r, 'receive_{pack}'.format(pack=pack))(*args, **kwargs)

    def _run_hook(self, pack):
        """
        Run a pack hook method for an invoked pack if any receivers are registered
        for a given pack, potentially resolving dependencies between packs if
        defined by a receiver method.
        """
        log.debug('*** Running pack hook', pack)
        met = pack_hooks[pack]
        met_obj = getattr(self, met['name'])
        if met['ran']:
            log.debug('*** Pack', pack,'already ran, skipping...')
            return

        if len(self.receivers[pack]) > 0:
            log.debug('*** Running pack hook', pack, len(self.receivers[pack]))
            for r in self.receivers[pack]:
                rcv_fun_obj = getattr(r, 'receive_{pack}'.format(pack=pack))
                if hasattr(rcv_fun_obj, '_requires'):
                    for p in rcv_fun_obj._requires:
                        log.debug('*** Resolving dependency ', p)
                        self._run_hook(p)

            met_obj()
            met['ran'] = True

    def run(self):
        receivers = set([r for p in self.receivers.values() for r in p])
        for r in receivers:
            if hasattr(r, 'setup') and callable(r.setup):
                r.setup()

        for pack in pack_hooks:
            self._run_hook(pack)

        for r in receivers:
            if hasattr(r, 'finalize') and callable(r.finalize):
                r.finalize()
