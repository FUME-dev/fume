# FUME - *F*lexible *U*niversal Processor for *M*odeling *E*missions
FUME is an open source software intended primarily for the preparation of emissions for chemical transport models. As such, FUME is responsible for preprocessing the input files and the spatial distribution, chemical speciation, and time disaggregation of the primary emission inputs. The main characteristics of FUME are:
* PostgreSQL, PostGIS, and Python based (for minimum versions see model documentation);
* flexible and configurable tool for emissions processing; 
* independent on specific input data;
* applicable in different scales (local / regional / continental);
* not limited to any geographical coordinate systems;
* configurable chains of emissions processing;
* easy implementation of new modules;
* easy involvement of specialised external models (e.g. for biogenic emissions) through common interface;
* outputs usable for different types of models / application;
* allowing for output also on irregular grids and general polygons;
* reporting and QA/QC.

FUME is being currently used for the air quality modelling in the Czech Hydrometeorological Institute and in several projects: [LIFE-IP Malopolska](https://powietrze.malopolska.pl/en/life-ip/) (action C.6, project reference LIFE14 IPE/PL/000021) and [URBI PRAGENSI](https://www.mff.cuni.cz/to.en/verejnost/konalo-se/2018-01-urbi/) (project reference CZ.07.1.02/0.0/0.0/16_040/0000383).

Despite of intensive work on its development, there are still some issues that need to be finished, mainly:
* improvement of documentation;
* increase of the computational efficiency;
* user-friendly usage / reporting / logging;
* additional possibilities to process emissions (e.g. provide emissions with proxy data for spatial disaggregation).

Therefore we hope that interested users not to be discouraged by some initial difficulties / inconvenience that are an inevitable part of the newly developed software. We will be happy if you  provide us with feedback and we will try to provide you maximum support within our limited time resources.

FUME is a common project of the [Czech Academy of Sciences](http://www.ustavinformatiky.cz/?id_jazyk=en&id_stranky=), [Czech Hydrometeorological Institute](http://portal.chmi.cz/), [Charles University](http://kfa.mff.cuni.cz/?lang=en), and Czech Technical University in Prague ([CIIRC](https://www.ciirc.cvut.cz/) and [FD](https://www.fd.cvut.cz/english/)).

## Availability
Software is distributed free of charge under the licence GNU GPL v3.0. View on [GitHub](https://github.com/FUME-dev/fume).

## Citing
Here we give you a recommended citing of the FUME: Benešová, N., Belda, M., Eben, K., Geletič, J., Huszár, P., Juruš, P., Krč, P., Resler, J. and Vlček, O. (2018): New open source emission processor for air quality models. In Sokhi, R., Tiwari, P. R., Gállego, M. J., Craviotto Arnau, J. M., Castells Guiu, C. & Singh, V. (eds) *Proceedings of Abstracts 11th International Conference on Air Quality Science and Application*. DOI: 10.18745/PB.19829. (pp. 27). Published by University of Hertfordshire. Paper presented at Air Quality 2018 conference, Barcelona, 12-16 March.

## Support
The developing team does not provide a regular support. But we are ready and will be happy to provide advice for new FUME users. Question should be sent to [Mr. Ondrej Vlcek](mailto:ondrej.vlcek@chmi.cz). 


## Acknowledgement
The first version of emission processor FUME (Flexible Universal processor for Modeling Emissions) was created within the project of the Technology Agency of the Czech Republic No. TA04020797 *Advanced emission processor utilizing new data sources*. 
