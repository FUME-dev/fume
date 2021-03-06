"""
Base module for data providers
    - pack: decorator function to register a method as a data sender
    - DataProvider: base class for data providers
"""


from abc import ABCMeta
from collections import defaultdict
from lib.ep_libutil import ep_debug


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
    def __new__(cls, name, bases, dct):
        inst = super().__new__(cls, name, bases, dct)
        rank = 0
        for n, o in dct.items():
            if callable(o) and hasattr(o, '_pack'):
                if o._pack in pack_hooks:
                    raise ValueError('Hook already defined for pack {}'.format(o._pack))

                pack_hooks[o._pack] = n
                rank += 1

        return inst


class DataProvider(metaclass=DataProviderMeta):
    """
    Base class for data providers
    """

    def __init__(self, *args, **kwargs):
        self.receivers = defaultdict(list)
        self._packs_run = []

        if 'cfg' in kwargs:
            self.cfg = kwargs['cfg']

        if 'rt_cfg' in kwargs:
            self.rt_cfg = kwargs['rt_cfg']

        if 'db' in kwargs:
            self.db = kwargs['db']

    def register_receiver(self, receiver):
        ep_debug('*** Pack hooks', pack_hooks)
        for pack in pack_hooks:
            rcv_fun = 'receive_{pack}'.format(pack=pack)
            if hasattr(receiver, rcv_fun):
                rcv_fun_obj = getattr(receiver, rcv_fun)
                if callable(rcv_fun_obj):
                    self.receivers[pack].append(receiver)

    def distribute(self, pack, *args, **kwargs):
        for r in self.receivers[pack]:
            getattr(r, 'receive_{pack}'.format(pack=pack))(*args, **kwargs)

    def _run_hook(self, pack):
        met_name = pack_hooks[pack]
        met_obj = getattr(self, met_name)
        if not pack in self._packs_run and len(self.receivers[pack]) > 0:
            ep_debug('*** Running pack hook', pack)
            for r in self.receivers[pack]:
                rcv_fun_obj = getattr(r, 'receive_{pack}'.format(pack=pack))
                if hasattr(rcv_fun_obj, '_requires'):
                    for p in rcv_fun_obj._requires:
                        ep_debug('*** Resolving dependency ', p)
                        self._run_hook(p)

            met_obj()
            self._packs_run.append(pack)

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
