from datetime import datetime, time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from contextlib import closing
from os import getenv
import json
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


def dbConnect(ti):
    try:
        pgHook = PostgresHook(postgres_conn_id='etl_weather_db')
        return pgHook

    except (Exception) as error:
        ti.xcom_push(
            key='export_daily_weather_csv_dbConnect_error', value=error)
        print("Error occurred while connecting to Database", error)


def exportData(ti):
    conn = dbConnect(ti)
    try:
        start_of_day = datetime.combine(datetime.now(), time.min)
        end_of_day = datetime.combine(datetime.now(), time.max)

        filename = '/tmp/tblweatherdata_' + datetime.now().strftime('%Y-%m-%d') + '.csv'

        SQL = f"""
            COPY (select * from tblweatherdata where date between '{start_of_day}' and '{end_of_day}') TO '{filename}' WITH DELIMITER ',' CSV HEADER;
            """
        conn.copy_expert(sql=SQL, filename=filename)

    except (Exception) as error:
        print("Failed to export CSV", error)
        ti.xcom_push(
            key='export_daily_weather_csv_exportData_error', value=json.dumps(error))

    finally:
        ti.xcom_push(key='export_daily_weather_csv_exportData',
                     value=f'CSV exported successfully - {datetime.now()}')
        print("PostgreSQL connection is closed")


with DAG('export_daily_weather_csv',
         description='Export daily weather data in CSV format',
         schedule='*/10 * * * *',
         start_date=datetime(2017, 3, 20),
         catchup=False):

    export_csv = PythonOperator(
        task_id='export_daily_weather_csv_task',
        python_callable=exportData
    )

    export_csv
