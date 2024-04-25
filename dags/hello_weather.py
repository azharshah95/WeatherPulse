import datetime
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import concurrent.futures
import coordinates
from os import getenv
import requests
from dotenv import load_dotenv
load_dotenv()

# initialize coordinates
coordinatesValues = coordinates.points()

try:
    # read API key
    API_KEY = getenv('API_KEY')
    LAT = getenv('LAT')
    LON = getenv('LON')
    UNITS = getenv('UNITS')

    # read database password
    DATABASE_HOST = getenv('DATABASE_HOST')
    DATABASE_USER = getenv('DATABASE_USER')
    DATABASE_NAME = getenv('DATABASE_NAME')
    DATABASE_PASSWORD = getenv('DATABASE_PASSWORD')

except:
    print("An exception occurred in Configurations")


def dbConnect():
    """Connects to the database and returns the connection object."""

    try:
        pgHook = PostgresHook(postgres_conn_id='etl_weather_db')
        return pgHook

    except:
        print("Error occurred while connecting to Database")


def parsingRawData(ti, cityname):
    """Parses raw data and pushes it to xcom."""

    rawData = ti.xcom_pull(key=f"raw_data_{cityname}")
    myDict = {
        "date": datetime.datetime.fromtimestamp(rawData["dt"]),
        "city_name": rawData["name"],
        "city_id": rawData["id"],
        "country": rawData["sys"]["country"],
        "lon": rawData["coord"]["lon"],
        "lat": rawData["coord"]["lat"],
        "base": rawData["base"],
        "temp": rawData["main"]["temp"],
        "feels_like": rawData["main"]["feels_like"],
        "timezone": rawData["timezone"]
    }
    ti.xcom_push(key=f"parsed_data_{cityname}", value=myDict)


def insertData(ti, cityname):
    """Inserts data into the database."""

    try:
        parsedData = ti.xcom_pull(key=f"parsed_data_{cityname}")
        conn = dbConnect()

        city_id = parsedData["city_id"],
        lat = parsedData['lat'],
        lon = parsedData["lon"],
        temp = parsedData["temp"],
        feels_like = parsedData["feels_like"],
        city_name = parsedData["city_name"],
        country = parsedData["country"],
        timezone = parsedData["timezone"],
        date = parsedData["date"],

        row = (city_id, lat, lon, temp, feels_like,
               city_name, country, timezone, date)
        SQL = "INSERT INTO tblweatherdata (city_id, lat, lon, temp, feels_like, city_name, country, timezone, date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"

        conn.run(sql=SQL, parameters=row)

    except (Exception) as error:
        ti.xcom_push(key=f"insert_error_{cityname}", value=json.dumps(error))
        print("Failed to insert record into tbldata", error)

    # finally:
    #     # closing database connection.
    #     if conn:
    #         conn.close()
    #         print("PostgreSQL connection is closed")


def getAPIData(LAT, LON):
    """Fetches data from the API and returns it as a dictionary."""
    rawData = requests.get(
        f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units={UNITS}").json()
    return rawData


def fetchData(ti, coords, cityname):
    """Fetches data from the API and pushes it to xcom."""

    raw_data = getAPIData(coords[0], coords[1])

    ti.xcom_push(key=f"raw_data_{cityname}", value=raw_data)


with DAG('hello_weather',
         description='Hello World DAG',
         schedule_interval='*/15 * * * *',
         start_date=datetime.datetime(2024, 4, 25),
         catchup=False):

    # run in loop
    for city in coordinatesValues:
        cityname = city['cityname'].lower()

        downloading_data = PythonOperator(
            task_id='weather_task' + cityname,
            provide_context=True,
            python_callable=fetchData,
            op_kwargs={
                "cityname": cityname,
                "coords": (
                    city["latitude"], city["longitude"]
                )
            }
        )

        # run after loop
        transform_data = PythonOperator(
            task_id='weather_parsing_task_' + cityname,
            provide_context=True,
            python_callable=parsingRawData,
            op_kwargs={
                "cityname": cityname
            }
        )

        # run in last
        load_data = PythonOperator(
            task_id='weather_load_task_' + cityname,
            provide_context=True,
            python_callable=insertData,
            op_kwargs={
                "cityname": cityname
            }
        )

        downloading_data >> transform_data >> load_data
