# Basic-Airflow-Project
This is a project that uses Airflow to automate data retrieval process from Open weather API and storing the data in a Postgres Database.
## Project Overview
- Tools: Apache Airflow
- Source: Open Weather webiste to get the API to retrieve the weather data.
- Purpose: To automate weather data ingestion from the API and bring it to a database for further analysis.
- Language: Python.
## How it works:
We write a DAG that does two functions that is Fetching the data from the API and the other one is to store the data to a database. 
The DAG will be triggered and will fetch the weather data for a specified location. The data is collected and then the storage function runs which 
stores the data into the database. 
