"""
Postprocessing interface: to run, include ep_postproc.run_postprocessing in workflow.conf
    - create a data provider class
    - register data processors from ep_cfg.postproc.processors
    - run postprocessing
"""


import importlib
from lib.ep_libutil import ep_connection, ep_rtcfg
from lib.ep_config import ep_cfg


def data_provider(name='postproc.emissprovider.EmissProvider'):
    """
    Helper function for instantiating a data provider object
    Should not be run manually (within a typical workflow)
    """

    mod_name, class_name = name.rsplit('.', 1)
    mod_obj = importlib.import_module(mod_name)
    class_obj = getattr(mod_obj, class_name)
    dp_instance = class_obj(cfg=ep_cfg, rt_cfg=ep_rtcfg,
                            db=ep_connection)

    return dp_instance


def run():
    """
    Main postprocessing dispatcher: run from within workflow.conf
    """

    dp = data_provider()
    for rec in ep_cfg.postproc.processors:
        mod_name, class_name = rec.rsplit('.', 1)
        mod_obj = importlib.import_module(mod_name)
        class_obj = getattr(mod_obj, class_name)
        dp.register_receiver(class_obj(cfg=ep_cfg, rt_cfg=ep_rtcfg,
                                       db=ep_connection))

    dp.run()
