======================================
 Emission processor programming guide
======================================

Workflow
========

Program workflow is controlled by the workflow configuration file
(WCF, default: fume_workflow.conf). Each line of the WCF is a name of
a function. The functions get called in the order of the lines. To skip
a function from being called, comment out the line with the hash sign.

The function name is composed similarly to the way the import is done in
Python, ie. by module (submodule...) name and name of the function delimited
by the dot. Avoiding long names in case of a complex directory structure of
a module can be achieved by importing the symbol in the __init__.py script
of a module, eg. the ``postproc.run`` function in the workflow is actually
``postproc.dispatch.run``, but is imported in the ``postproc/__init__.py``
file::

    from .dispatch import run


Postprocessing
==============

Postprocessing (ie. everything after the disaggregation of emissions in the
database, including output to CTMs) is handled by the ``DataProvider`` and
``DataReceiver`` classes. The postprocessing dispatcher is run by calling the
``postproc.run`` function from within the workflow (internally
``postproc.dispatch.run``). No changes should typically be made in the
``postproc.dispatch`` module.

To add a processor, create a new receiver class inherited from the base
``receiver.DataReceiver`` class (you can use eg. the included
``postproc.emissplotter`` module as a template). To register a receiver class,
add it to the ``postproc`` section of the main configuration file in the
``processors`` option with full python path, eg.::

    [postproc]
        processors = postproc.emissplotter.EmissPlotter, postproc.anotherproc.ProcClass

All postprocessor objects (provider and receivers) have access to the
configuration options via the ``cfg`` attribute (accessed as ``self.cfg``
from within the processor code, runtime configuration options via the
``rt_cfg`` attribute (``self.rt_cfg``) and to the database connector
via the ``db`` attribute (``self.db``).

Data are distributed by the provider in *packs*. To request a data pack,
the receiver class must implement a ``receive_[name_of_pack]`` method, eg.
a method that wants to be provided with the list of output species needs
to implement a ``receive_species`` method::

    def receive_species(self, species):
        self.species = species


Receiver method may specify a dependency on other packs by using the
``requires`` decorator, eg. a method processing area emissions depending
on the prior knowledge of the list of area species can be implemented
using this decorator::

    @requires('area_species')
    def receive_area_emiss(self, timestep, data):
        ...


Dependencies are strictly a receiver responsibility.

Receiver cleanup actions (ie. the code that should be run after all data packs
have been processed) can be provided by implementing the ``finalize`` method.

At present, the data packs are provided by the included
``postproc.emissprovider.EmissProvider`` class, which is used by default
by the dispatcher (this is hardcoded and can be changed in the
``postproc.dispatch`` module). See the documentation of the ``EmissProvider``
class for the list of available packs.

Adding a data pack is achieved by implementing a method to the ``EmissProvider``
class decorated by the ``pack`` decorator with the name of the pack specified
as the decorator argument: ``@pack('name_of_pack')``. There can be only one
method implementing a specific pack. The pack providing method must call the
``distribute`` method explicitely.

To avoid repeating heavy database operations, share data within the provider
object by saving it as a ``self.[data]`` attribute and reuse in other methods.
An example of the data pack provider method fetching, saving and distributing
the list of output species as provided in postproc.emissprovider.EmissProvider
class::

    @pack('species')
    def get_species(self):
        try:
            self.species
        except AttributeError:
            q = 'SELECT * from "{}".get_species'.format(self.cfg.db_connection.case_schema)
            with self.db.cursor() as cur:
                cur.execute(q)
                self.species = cur.fetchall()

            self.distribute('species', species=self.species)

A pack providing method implemented in this manner can be called repeatedly
from other methods (in this example those that need the list of species for
their operation), however, the database fetch will be performed only the
first time.

Configuration
=============

Configuration options are read from the configuration file by the main program
(user can use the -c argument to change the default configuration file name).
To access the configuration in a module, import the ``ep_cfg`` object of the
``ep_config`` module, eg.::

    from lib.ep_config import ep_cfg

Configuration is a customized ConfigObj object allowing access via attributes, eg.::

    nx = ep_cfg.domain_params.nx


Database connection
===================

DB connection is initialized by the main program. To access the database in a module,
import the ``ep_connection`` object of the ``ep_libutil`` module, eg.::

    from lib.ep_libutil import ep_connection

The ``ep_connection`` object is a Psycopg2 connection object conforming to the Python
DB-API 2.0 specification. Always use query parameters for ``execute`` method instead
of Python string interpolation or format! Example usage (create cursor, execute
a query, commit)::

    cur = ep_connection.cursor()
    cur.execute('INSERT INTO table (name, desc) VALUES (%s, %s)', (vname, vdesc))
    ep_connection.commit()


Exceptions
==========

1. Functions and methods should catch only those exceptions that can be handled
   by them. Do not use the "catch-all" syntax::

    try:
        some_code
    except Exception:
        do_something

   Do this instead::

    try:
        some_code
    except SomeError:
        handle_the_exception
    except AnotherError:
        handle_the_exception

2. Simply logging the error does not usually constitute exception handling.
   Most likely this will not be what you want::

    try:
        some_code
    except SomeError:
        log('some error occured')

    more_code

   If you cannot decide how to handle the exception, but absolutely need to log
   its occurence immediately, reraise the exception and let the calling context
   decide how to proceed::

    try:
        some_code
    except SomeError:
        log('some error occured') # but we don't know what to do about it
        raise

3. Do not exit the program during exception handling. Ie. do not use::

    try:
        some_code
    except SomeError:
        log('some error occured')
        sys.exit(1)


Naming conventions
==================


