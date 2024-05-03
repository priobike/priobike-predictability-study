# Observation Sink

Dataset download: https://doi.org/10.25532/OPARA-466 

With this project it is possible to fetch and analyze traffic light data from the city of Hamburg (https://metaver.de/trefferanzeige?docuuid=AB32CF78-389A-4579-9C5E-867EF31CA225). The service for data aquisition uses HTTP and MQTT to retrieve real time traffic light data redundantly. The data is then stored in a database and can be analyzed using the study scripts. The study is split into two parts. To deal with the large amount of data, the first part is implemented in Go. Having the processed data, the second part is implemented in Python using Jupyter Notebooks.

## Citing

The corresponding paper is currently under review.

## Quickstart

### Data Aquisition

It is recommended to use the provided docker-compose file to start the data aquisition. The docker-compose file starts with the Go-script (including MQTT and HTTP client) and the database, where all the observations are stored.

### Data Analysis

First run the Go-script to process the data: `go run main.go`. The following environment variables have to be set: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` and `POSTGRES_HOST`. Afterwards, use the Jupyter Notebooks to analyze the output of the Go-script (`processed_things.json`).

## Development

Disclaimer: Development stretched over multiple months. During this time, we activaly worked on the project and changed naming for certain things. The following names are used synonymously throughout the code base:
- Cycle Discrepancy, Cycle Dependent Stability, Distance
- Wait Time Diversity, Cycle Independent Stability, Shifts Fuzzyness

## Anything unclear?

Help us improving this documentation. If you have any problems or unclarities, feel free to open an issue.
