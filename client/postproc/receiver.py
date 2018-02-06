def requires(*names):
    """
    Decorator for the DataReceiver classes allowing to specify dependencies
    between packs.

    Example usage in DataReceiver class: define receiver method for getting
    the area emission data dependent on the prior knowledge of the area
    species.

    @requires('area_species')
    def receive_area_emiss(self, timestep, data):
        ...
    """
    def f_wrap(f):
        def f_wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        f_wrapped._requires = names
        return f_wrapped
    return f_wrap


class DataReceiver():
    """
    Base class for data receivers
    """

    def __init__(self, *args, **kwargs):
        if 'cfg' in kwargs:
            self.cfg = kwargs['cfg']

        if 'rt_cfg' in kwargs:
            self.rt_cfg = kwargs['rt_cfg']

        if 'db' in kwargs:
            self.db = kwargs['db']
