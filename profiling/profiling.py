import pexpect
import json
import random
import math
import csv
import os
import time

OUTPUT_PATH = "./output/output.json"
CSV_PATH = "../spark_profiling/results.csv"
JAR_PATH = "./covid-analysis.jar"
STARTING_CORES = 2
STARTING_COUNTRIES = 2
DEFAULT_COUNTRIES = 10
DEFAULT_CORES = 8
MASTER_ADDRESS = "172.20.10.4"
MASTER_PORT = 7077
EXECUTOR_MEMORY = "4g"


# Waits for the file with file_name to be modified
def wait_output(file_name):
    time = os.path.getmtime(file_name)
    while True:
        if time != os.path.getmtime(file_name):
            with open(file_name, "r") as f:
                info = f.read()
            time = os.path.getmtime(file_name)
        time.sleep(0.1)


# Initializates csv file with the simulation parameters (working)
def csv_initialization(parameters):
    columns = parameters.copy()
    columns.append("execution_time")
    columns.append("dataset_size")
    writer = csv.writer(open(CSV_PATH, "w+"))
    writer.writerow(columns)


# Inserts a new row in the csv file (working)
def csv_insertion(execution_time, dataset_size=-1, country_number=-1, core_number=-1):
    fields = []
    if country_number != -1:
        fields.append(str(country_number))
    if core_number != -1:
        fields.append(str(core_number))
    if dataset_size != -1:
        fields.append(str(dataset_size))
    fields.append(str(execution_time))
    writer = csv.writer(open(CSV_PATH, "a+"))
    writer.writerow(fields)


# Runs a simulation with core_number cores, parses the output stream and gives back the execution time
def simulate(cores=DEFAULT_CORES, countries=DEFAULT_COUNTRIES):
    commands = [
        "/usr/local/spark/sbin/start-all.sh",
        "spark-submit --class Main --master "
        + MASTER_ADDRESS
        + " --deploy-mode client"
        + " --executor-memory "
        + EXECUTOR_MEMORY
        + " --total-executor-cores "
        + str(cores)
        + " "
        + JAR_PATH
        + " "
        + str(countries),
    ]
    print(commands[0])
    exe = pexpect.spawnu(
        commands[0],
        encoding="utf-8",
        codec_errors="ignore",
    )
    exe.expect(pexpect.EOF)
    print(commands[1])
    exe = pexpect.spawnu(
        commands[1],
        encoding="utf-8",
        codec_errors="ignore",
    )
    wait_output(OUTPUT_PATH)


def run_profiling(cores=False, countries=False, step=1, simulation_number=10):
    csv_fields = []
    core_number = DEFAULT_CORES
    country_number = DEFAULT_COUNTRIES
    if countries:
        csv_fields.append("country_number")
        country_number = STARTING_COUNTRIES
    if cores:
        csv_fields.append("cores")
        core_number = STARTING_CORES
    csv_initialization(csv_fields)
    for sim in range(simulation_number):
        simulate(cores=core_number, countries=country_number)
        output = json.loads(open(OUTPUT_PATH, "r").read())
        if countries == True:
            csv_insertion(
                output["execution_time"],
                output["dataset_size"],
                country_number=country_number,
            )
            country_number = country_number + step
        if cores == True:
            csv_insertion(
                output["execution_time"],
                output["dataset_size"],
                core_number=core_number,
            )
            core_number = core_number + step
    print("Simulation Completed")


if __name__ == "__main__":
    run_profiling(cores=True, step=1, simulation_number=3)
