import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from os import getenv
import requests
from dotenv import load_dotenv
load_dotenv()

try:
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
    try:
        pgHook = PostgresHook(postgres_conn_id='etl_weather_db')
        return pgHook

    except:
        print("Error occurred while connecting to Database")


def fetchData(ti):
    rawData = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units={UNITS}").json()
    
    ti.xcom_push(key='raw_data', value=rawData)

def parsingRawData(ti):
  rawData = ti.xcom_pull(key='raw_data')
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
  ti.xcom_push(key='parsed_data', value=myDict)
  
def insertData(ti):
  try:
    parsedData = ti.xcom_pull(key='parsed_data')
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
    
    row = (city_id, lat, lon, temp, feels_like, city_name, country, timezone, date)      
    SQL = "INSERT INTO tblweatherdata (city_id, lat, lon, temp, feels_like, city_name, country, timezone, date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    
    conn.run(sql=SQL, parameters=row)

  except (Exception) as error:
    ti.xcom_push(key='insert_error', value=error)
    print("Failed to insert record into tbldata", error)

  # finally:
  #     # closing database connection.
  #     if conn:
  #         # conn.close()
  #         print("PostgreSQL connection is closed")


with DAG('hello_weather',
         description='Hello World DAG',
         schedule='0 12 * * *',
         start_date=datetime.datetime(2017, 3, 20),
         catchup=False):

    downloading_data = PythonOperator(
        task_id='weather_task',
        python_callable=fetchData
    )
    
    transform_data = PythonOperator(
        task_id='weather_parsing_task',
        python_callable=parsingRawData
    )
    
    load_data = PythonOperator(
        task_id='weather_load_task',
        python_callable=insertData
    )

    downloading_data >> transform_data >> load_data
