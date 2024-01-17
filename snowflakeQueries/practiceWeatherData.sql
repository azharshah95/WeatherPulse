
CREATE OR REPLACE DATABASE sf_openweather;

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

CREATE OR REPLACE WAREHOUSE sf_openweather_wh WITH
   WAREHOUSE_SIZE='X-SMALL'
   AUTO_SUSPEND = 180
   AUTO_RESUME = TRUE
   INITIALLY_SUSPENDED=TRUE;

-- in order to keep names in lowercase, wrap them in "".
CREATE OR REPLACE TABLE "weather_data"  (
  "id" INTEGER NOT NULL,
  PRIMARY KEY ("id"),
  "city_id" INTEGER NOT NULL,
  "lon" REAL NOT NULL,
  "lat" REAL NOT NULL,
  "temp" REAL NOT NULL,
  "feels_like" REAL NOT NULL,
  "city_name" STRING NOT NULL,
  "country" STRING NOT NULL,
  "timezone" INTEGER NOT NULL,
  "date" TIMESTAMP NOT NULL UNIQUE
);

SELECT CURRENT_WAREHOUSE();

-- create transient table
CREATE TABLE "weather_data_clone" LIKE "weather_data";

-- creating stage
create or replace stage my_postgres_stage
copy_options = (on_error='skip_file')
file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1 field_optionally_enclosed_by='"');

-- copying file to stage
-- PUT file:///home/azhar/repo/etl-weather/snowflakeQueries/tblweatherdata.csv @my_postgres_stage;
PUT file:///home/azhar/repo/etl-weather/snowflakeQueries/tblweatherdata_*.csv @my_postgres_stage;

LIST @my_postgres_stage;

-- loading data from stage to table
COPY INTO "SF_OPENWEATHER"."PUBLIC"."weather_data_clone"
-- FROM '@"SF_OPENWEATHER"."PUBLIC"."%weather_data_clone"/__snowflake_temp_import_files__/'
FROM '@"SF_OPENWEATHER"."PUBLIC"."MY_POSTGRES_STAGE"'
FILES = ('tblweatherdata.csv.gz')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT='YYYY-MM-DD',
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS'
)
ON_ERROR=CONTINUE
PURGE=TRUE;

-- loading pattern data from stage to table
COPY INTO "SF_OPENWEATHER"."PUBLIC"."weather_data_clone"
-- FROM '@"SF_OPENWEATHER"."PUBLIC"."%weather_data_clone"/__snowflake_temp_import_files__/'
FROM '@"SF_OPENWEATHER"."PUBLIC"."MY_POSTGRES_STAGE"'
-- FILES = ('tblweatherdata.csv.gz')
PATTERN = '.*tblweatherdata_.*[.]csv[.]gz'
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT='YYYY-MM-DD',
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS'
)
ON_ERROR=CONTINUE
PURGE=TRUE;

-- merge transient table to target table
MERGE INTO "weather_data" AS target
USING (SELECT * FROM "weather_data_clone") AS source 
ON target."id" = source."id"
WHEN NOT MATCHED THEN INSERT (target."id", target."city_id", target."lon", target."lat", target."temp", target."feels_like", target."city_name", target."country", target."timezone", target."date") VALUES (source."id", source."city_id", source."lon", source."lat", source."temp", source."feels_like", source."city_name", source."country", source."timezone", source."date");

-- truncating transient table
truncate table "weather_data_clone";

-- Debugging Data Loading Errors
CREATE OR REPLACE TABLE save_copy_errors AS SELECT * FROM TABLE(VALIDATE("weather_data", JOB_ID=>'01b17307-0000-885b-0000-00064e5ab84d'));

SELECT * FROM SAVE_COPY_ERRORS;
-- !exit

select * from "weather_data_clone" order by "id" ASC;
select * from "weather_data";