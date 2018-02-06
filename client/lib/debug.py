import time


class FakeDBConnection():
    source_schema = 'sources'
    case_schema = 'test'

    def mogrify(self, q, pars):
        return q.replace('%s', '{}').format(*pars)

    def execute(self, q):
        return True

    def cursor(self):
        return self


class ExecTimer():
    def __init__(self, name='', print_out=True):
        self.name = name
        self.print_out = print_out

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        if self.print_out:
            self.print_split()

    def print_split(self):
        print('*** {} execution time: {}'.format(self.name,
                                                 self.duration))

    @property
    def duration(self):
        return time.time()-self.start
