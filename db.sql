CREATE TABLE IF NOT EXISTS "tblweatherdata"  (
  "id" serial NOT NULL,
  PRIMARY KEY ("id"),
  "city_id" integer NOT NULL,
  "lon" real NOT NULL,
  "lat" real NOT NULL,
  "temp" real NOT NULL,
  "feels_like" real NOT NULL,
  "city_name" character(255) NOT NULL,
  "country" character(8) NOT NULL,
  "timezone" integer NOT NULL,
  "date" timestamp NOT NULL
)