# COVID Data Analysis

This project implements a program that analyzes datasets to perform data analysis on the COVID-19 situation. Using a dataset of daily reported cases for each country, it computes statistics such as 

This project was created as part of a set of deliveries for Middleware Technologies for Distributed Systems course.

# How to run
## Run locally
Open a terminaland run:

```sh
java -jar covid-analysis.jar
```

## Run distributed
To run our application, passwordless ssh must be set up from the manager to the workers and vice versa and Spark environment must be set up. A nfs shared folder must be created and mounted on all machines.
Open a terminal from the manager, enter the directory with the executable and run:

```sh
spark-submit --class Main --deploy-mode cluster --master spark://<MASTER_ADDRESS>:7077
```
To load the required files (JAR, dataset), then run: 
```sh
sh /path/to/spark/sbin/ start-all.sh
```
To start the master and all the workers. An overview of the execution can be found on [port 8080].

## Profiling
Make sure plotly and pandas are installed. Move to cmake-build-debug folder and run:
```sh
python3 profiling.py
```
The run configuration can be edited on profiling.py.
## Documentation
The documentation is hosted in the [GitHub Docs] folder.


## Authors
This project was developed by [Chiara Marzano](mailto:chiara.marzano@mail.polimi.it), [Massimiliano Nigro](mailto:massimiliano.nigro@mail.polimi.it), [Daniele Paletti](mailto:daniele.paletti@mail.polimi.it).

[GitHub Docs]: https://github.com/Massimilianonigro/covid-analysis/documentation.pdf
[port 8080]: localhost:8080
