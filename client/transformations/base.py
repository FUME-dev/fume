from abc import ABC, abstractmethod


class Transformation(ABC):
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def apply(self):
        pass


class OneToOneTransformation(Transformation):
    parameters = ['inrelation', 'outrelation', 'outsrid']

    def __init__(self, inrel=None, outrel=None, outsrid=None):
        self.inrelation = inrel
        self.outrelation = outrel
        self.outsrid = outsrid


class TwoToOneTransformation(Transformation):
    parameters = ['inrelation', 'inrelation2', 'outrelation', 'outsrid']

    def __init__(self, inrel1=None, inrel2=None, outrel=None, outsrid=None):
        self.inrelation = inrel1
        self.inrelation2 = inrel2
        self.outrelation = outrel
        self.outsrid = outsrid
