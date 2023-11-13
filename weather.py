from os import getenv
import configparser
import requests
import pandas as pd
import datetime
import psycopg2
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

def dbConnect():
  try:
    conn = psycopg2.connect(f"host={DATABASE_HOST} dbname={DATABASE_NAME} user={DATABASE_USER} password={DATABASE_PASSWORD}")
    return conn
  
  except:
    print("Error occurred while connecting to Database")

def parsingRawData(rawData: dict) -> dict[dict]:
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
  return myDict



def insertData(parsedData: dict):
  conn = dbConnect()
  try:
      cursor = conn.cursor()

      SQL = "INSERT INTO tbldata (city_id, lat, lon, temp, feels_like, city_name, country, timezone, date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
      data = (
          parsedData["city_id"],
          parsedData['lat'],
          parsedData["lon"],
          parsedData["temp"],
          parsedData["feels_like"],
          parsedData["city_name"],
          parsedData["country"],
          parsedData["timezone"],
          parsedData["date"],
          )
      cursor.execute(SQL, data)

      conn.commit()
      count = cursor.rowcount
      print(count, "Record inserted successfully into tbldata")

  except (Exception, psycopg2.Error) as error:
      print("Failed to insert record into tbldata", error)

  finally:
      # closing database connection.
      if conn:
          cursor.close()
          conn.close()
          print("PostgreSQL connection is closed")

rawData = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units={UNITS}").json()
print(json.dumps(rawData))        

parsedData = parsingRawData(rawData)
print(parsedData)

insertData(parsedData)

def hello():
    print('Hello!')