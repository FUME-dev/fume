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
Here we give you a recommended citing of the FUME:
* Belda, M., Benešová, N., Resler, J., Huszár, P., Vlček, O., Krč, P., Karlický, J., Juruš, P., and Eben, K.: FUME 2.0 – Flexible Universal processor for Modeling Emissions, Geosci. Model Dev., 17, 3867–3878, https://doi.org/10.5194/gmd-17-3867-2024, 2024.
* Benešová, N., Belda, M., Eben, K., Geletič, J., Huszár, P., Juruš, P., Krč, P., Resler, J. and Vlček, O. (2018): New open source emission processor for air quality models. In Sokhi, R., Tiwari, P. R., Gállego, M. J., Craviotto Arnau, J. M., Castells Guiu, C. & Singh, V. (eds) *Proceedings of Abstracts 11th International Conference on Air Quality Science and Application*. DOI: 10.18745/PB.19829. (pp. 27). Published by University of Hertfordshire. Paper presented at Air Quality 2018 conference, Barcelona, 12-16 March.

## Support
The developing team does not provide a regular support. But we are ready and will be happy to provide advice for new FUME users. Question should be sent to [Mr. Ondrej Vlcek](mailto:ondrej.vlcek@chmi.cz). 


## Acknowledgement
The first version of emission processor FUME (Flexible Universal processor for Modeling Emissions) was created within the project of the Technology Agency of the Czech Republic No. TA04020797 *Advanced emission processor utilizing new data sources*. 

## Publications
A list of publications that utilized FUME:
* Prieto Perez, A.P., Huszár, P. and Karlický, J.: Validation of multi-model decadal simulations of present-day central European air-quality, Atmos. Environ., 349, 121077, https://doi.org/10.1016/j.atmosenv.2025.121077, 2025.
* Bartík, L., Huszár, P., Karlický, J., Vlček, O., and Eben, K.: Modeling the drivers of fine PM pollution over Central Europe: impacts and contributions of emissions from different sources, Atmos. Chem. Phys., 24, 4347–4387, https://doi.org/10.5194/acp-24-4347-2024, 2024.
* Huszar, P., Prieto Perez​​​​​​​, A. P., Bartík, L., Karlický, J., and Villalba-Pradas, A.: Impact of urbanization on fine particulate matter concentrations over central Europe, Atmos. Chem. Phys., 24, 397–425, https://doi.org/10.5194/acp-24-397-2024, 2024.
* Liaskoni, M., Huszár, P., Bartík, L., Prieto Perez, A. P., Karlický, J., and Šindelářová, K.: The long-term impact of biogenic volatile organic compound emissions on urban ozone patterns over central Europe: contributions from urban and rural vegetation, Atmos. Chem. Phys., 24, 13541–13569, https://doi.org/10.5194/acp-24-13541-2024, 2024.
* Liaskoni, M., Huszar, P., Bartík, L., Prieto Perez, A. P., Karlický, J., and Vlček, O.: Modelling the European wind-blown dust emissions and their impact on particulate matter (PM) concentrations, Atmos. Chem. Phys., 23, 3629–3654, https://doi.org/10.5194/acp-23-3629-2023, 2023.
* Huszar, P., Karlický, J., Bartík, L., Liaskoni, M., Prieto Perez, A. P., and Šindelářová, K.: Impact of urbanization on gas-phase pollutant concentrations: a regional-scale, model-based analysis of the contributing factors, Atmos. Chem. Phys., 22, 12647–12674, https://doi.org/10.5194/acp-22-12647-2022, 2022.
* Huszar, P., Karlický, J., Marková, J., Nováková, T., Liaskoni, M., and Bartík, L.: The regional impact of urban emissions on air quality in Europe: the role of the urban canopy effects, Atmos. Chem. Phys., 21, 14309–14332, https://doi.org/10.5194/acp-21-14309-2021, 2021.
* Huszar, P., Karlický, J., Ďoubalová, J., Nováková, T., Šindelářová, K., Švábik, F., Belda, M., Halenka, T., and Žák, M.: The impact of urban land-surface on extreme air pollution over central Europe, Atmos. Chem. Phys., 20, 11655–11681, https://doi.org/10.5194/acp-20-11655-2020, 2020.
* Huszar, P., Karlický, J., Ďoubalová, J., Šindelářová, K., Nováková, T., Belda, M., Halenka, T., Žák, M., and Pišoft, P.: Urban canopy meteorological forcing and its impact on ozone and PM2.5: role of vertical turbulent transport, Atmos. Chem. Phys., 20, 1977–2016, https://doi.org/10.5194/acp-20-1977-2020, 2020.
