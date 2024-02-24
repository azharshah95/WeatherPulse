# Weather ETL Pipeline with Airflow DAGs and Snowflake Queries
This repository contains code for an Airflow DAG (Directed Acyclic Graph) named `hello_weather`, which orchestrates an Extract-Transform-Load (ETL) pipeline to fetch weather data from an API, parse it, and load it into a PostgreSQL database. Additionally, it includes Snowflake queries for setting up the Snowflake Data Warehousing environment and loading data into Snowflake tables. The entire project is containerized using Docker, with Docker volumes used for storing data.


## Prerequisites

- Python 3.x
- Snowflake
- Airflow
- PostgreSQL
- Docker
- Docker Compose

### Libraries

- pandas
- psycopg2


## Installation
1. Clone the repository:

   ```bash
   git clone https://github.com/azharshah95/etl-weather.git
   ```
2. Setup .env file:

   Please check the [misc.md](misc.md) file.

3. Run Docker Containers:

   To run `stack.yml`
   ```bash
   docker-compose -f stack.yml up
   ```
   To run `docker-compose.yaml`
   ```bash
   docker-compose up
   ```

## Code Entry Point

- `./dags/hello_weather.py`
  - This file contains the ETL tasks.
- `./snowflakeQueries/practiceWeatherData.sql`
  - This file contains SnowSQL to Load data into Snowflake warehouse.
- `.code.ipynb`
  - This file contains testing try/run code snippets that shall be incorporated later on in the DAG for automation
