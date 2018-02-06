from lib.ep_libutil import ep_debug


def preproc(cfg):
    ep_debug('Dummy external model preprocessor...')
    ep_debug(cfg)

def run(cfg, myid):
    ep_debug('Dummy external model {id} run...'.format(id=myid))
