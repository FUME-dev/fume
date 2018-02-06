#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Modul pro spravu chybovych a informacnich vystupu na konzoli.

Stara se o vystupy urcene typicky na stdout a stderr, majici charakter
informacnich zprav s ruznou verbosity ci chybovych hlasek. Krome toho
udrzuje informaci o zavaznosti nejzavaznejsi vyskytujici se chyby.

Bezne zpracovava 4 typy udalosti:
info:  Vypisuje se na stdout, ma urcenou verbosity (>=1, 1=nejzavaznejsi).
       Pokud je jeji verbosity vetsi nez aktualne nastavena mez, ignoruje se.
warn:  Informace s verbosity=0. Nelze ignorovat, vypisuje se na stderr.
error: Vypisuje se na stderr a krome toho meni chybovy vystup programu
       na urcenou severity (>=1, 1 = nejzavaznejsi)
die:   Vypise error, nasledne program okamzite ukonci s kodem odpovidajicim
       nejhorsi chybe, ktera zatim nastala.

Je-li mez verbosity nastavena na 0 (default), neloguji se zadne info a tedy
veskery vystup jde pouze na stderr.

Pri beznem pouziti se importuje jeden staticky existujici logovaci objekt:
>>> from consoling import log

ktery se pred prvnim pouzitim inicializuje pomoci jeho metody init().

Jinak je mozno ziskat libovolne mnozstvi logovacich objektu klasickym
konstruktorem consoling.Consoler().
'''

__author__ = 'Pavel Krc'
__email__ = 'src@pkrc.net'
__date__ ='2010-09'

import sys
import string
import datetime

_fmt = string.Formatter()
_maxerr = float('inf')

class Consoler():
    def __init__(self,
            verbosity=0,
            logging_output=sys.stdout,
            error_output=sys.stderr,
            progname=sys.argv[0],
            time_format='%y%m%d_%H%M%S.%f',
            flush_logging_output=True, # flush after every message?
            flush_error_output=True,
            ):
        assert verbosity >= 0
        
        if flush_logging_output:
            def low(x):
                logging_output.write(x)
                logging_output.flush()
            self.logging_output = low
        else:
            self.logging_output = logging_output.write

        if flush_error_output:
            def eow(x):
                error_output.write(x)
                error_output.flush()
            self.error_output = eow
        else:
            self.error_output = error_output.write

        if time_format and progname:
            fmt = '{0} {1}: '.format(progname, time_format)
            self.prefix = lambda: [datetime.datetime.now().strftime(fmt)]
        elif time_format:
            fmt = time_format + ': '
            self.prefix = lambda: [datetime.datetime.now().strftime(fmt)]
        elif progname:
            s = progname + ': '
            self.prefix = lambda: [s]
        else:
            self.prefix = lambda: []

        self.current_verbosity = verbosity
        self.current_severity = _maxerr
    
    def _print(self, out, message, args, kwargs):
        msg = self.prefix()

        if args or kwargs:
            msg.append(_fmt.vformat(message, args, kwargs))
        else:
            msg.append(message)
        if not msg[-1].endswith('\n'):
            msg.append('\n')
        
        out(''.join(msg))
    
    def set_verbosity(self, verbosity):
        assert verbosity >= 0
        self.current_verbosity = verbosity
    
    def is_verbose(self, verbosity):
        return verbosity <= self.current_verbosity
    
    def get_worst_severity(self):
        if self.current_severity is _maxerr:
            return 0
        else:
            return self.current_severity
    
    def event(self, severity, verbosity, message, *args, **kwargs):
        assert severity >= 0 
        assert verbosity >= 0
        assert not (severity and verbosity)
        
        if severity:
            self.current_severity = min(self.current_severity, severity)
        if verbosity <= self.current_verbosity:
            if verbosity == 0:
                out = self.error_output
            else:
                out = self.logging_output
            
            self._print(out, message, args, kwargs)
    
    def info(self, verbosity, message, *args, **kwargs):
        assert verbosity > 0
        if verbosity <= self.current_verbosity:
            self._print(self.logging_output, message, args, kwargs)
    
    def info1(self, message, *args, **kwargs):
        if 1 <= self.current_verbosity:
            self._print(self.logging_output, message, args, kwargs)
    
    def warn(self, message, *args, **kwargs):
        self._print(self.error_output, message, args, kwargs)

    def error(self, severity, message, *args, **kwargs):
        assert severity > 0
        self.current_severity = min(self.current_severity, severity)
        self._print(self.error_output, message, args, kwargs)
    
    def error1(self, message, *args, **kwargs):
        self.current_severity = min(self.current_severity, 1)
        self._print(self.error_output, message, args, kwargs)
    
    def die(self, severity, message, *args, **kwargs):
        assert severity > 0
        self.current_severity = min(self.current_severity, severity)
        self._print(self.error_output, message, args, kwargs)
        sys.exit(self.get_worst_severity())
    
    def die1(self, message, *args, **kwargs):
        self.current_severity = min(self.current_severity, 1)
        self._print(self.error_output, message, args, kwargs)    
        sys.exit(self.get_worst_severity())

# staticky objekt je puvodne inicializovany defaultne
log = Consoler()
log.init = log.__init__
