"""
Description: Postprocessing interface
To run, include ep_postproc.run_postprocessing in workflow.conf:
    - create a data provider class
    - register data processors from ep_cfg.postproc.processors
    - run postprocessing
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
